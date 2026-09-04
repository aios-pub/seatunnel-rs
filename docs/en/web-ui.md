# Web Management Console

`seatunnel-web` is a standalone management console for SeaTunnel clusters:
an axum REST server that embeds a Leptos (WebAssembly) single-page UI and
talks to the master nodes over gRPC.

```
+-----------+  HTTP/JSON  +--------------+   gRPC    +--------+
|  Browser  | <---------- | seatunnel-web| <--------> | master |
+-----------+  + embedded +--------------+  failover  +--------+
                SPA (Leptos CSR)               list    + workers
```

## Run

### One-command local demo

The quickest way to see everything working on a single machine:

```bash
./scripts/web-demo.sh
```

It builds the binaries, starts master + worker + the console, waits for
the worker to register, submits the streaming demo job automatically and
prints the URL. `Ctrl+C` stops all three processes and removes the demo
state. Ports/login can be overridden (`WEB_LISTEN`, `MASTER_ADDR`,
`SEATUNNEL_WEB_PASSWORD`, …).

> A job submission fails with `no worker registered` when the console's
> master has no registered worker — jobs are only scheduled to workers.
> The demo script (or a `--role hybrid` node, which embeds a worker)
> fixes it.

### Embedded in the engine server (`--web`)

The console (SPA + REST API + `/metrics`) is compiled into the
`seatunnel-engine-server` binary as well; pass `--web` to serve it from the
engine process itself — one binary, one command for the whole stack:

```bash
seatunnel-engine-server --role hybrid --addr 0.0.0.0:5800 --web
```

Without `--web` no HTTP port is opened. Extra flags:

> Nohup wrappers (`start|stop|status|restart`, password via `WEB_PASSWORD` /
> `SEATUNNEL_WEB_PASSWORD`): `scripts/start-hybrid-web.sh` (single node,
> info logs), `scripts/start-hybrid-web-debug.sh` (single node,
> `SEATUNNEL_LOG=debug` — the log level, not the build profile) and
> `scripts/start-cluster-web.sh` (N-node cluster, one console per node on
> ports `WEB_LISTEN`+i).
>
> The crate's `build.rs` copies all three next to the compiled binaries,
> so `cargo build --release` turns `target/release` into a self-contained
> package: `cd target/release && WEB_PASSWORD=secret ./start-cluster-web.sh start`.
> Package mode needs no cargo and no repo (state lands in `./.seatunnel-state`).

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--web` | — | off | Serve the embedded console |
| `--web-listen` | — | `0.0.0.0:8080` | HTTP listen address |
| `--web-master` | — | own gRPC endpoint | Engine endpoint(s) the console proxies to (comma separated) |
| `--web-auth-user` | `SEATUNNEL_WEB_USER` | `admin` | Login username |
| `--web-auth-password` | `SEATUNNEL_WEB_PASSWORD` | `admin` (+warning) | Login password |
| `--web-auth-disable` | — | off | Disable auth (local dev only) |

By default the console targets this node's own gRPC endpoint (master/hybrid)
or the `--master` list (worker role); sessions last 12 hours. For every
other knob (refresh interval, session TTL) run the standalone `seatunnel-web`
binary instead.

### Manual startup

Start a cluster node, then the console:

```bash
seatunnel-engine-server --role hybrid --addr 127.0.0.1:5800

seatunnel-web --master 127.0.0.1:5800 --listen 0.0.0.0:8080
```

Open `http://127.0.0.1:8080`. `--master` accepts a comma-separated list
(failover order), matching the CLI `-a` flag.

## Authentication

The console requires a login by default (single account, session cookie):

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--auth-user` | `SEATUNNEL_WEB_USER` | `admin` | Login username |
| `--auth-password` | `SEATUNNEL_WEB_PASSWORD` | `admin` (+warning) | Login password |
| `--auth-ttl-mins` | — | `720` | Session lifetime |
| `--auth-disable` | — | off | Disable auth (local dev only) |

```bash
# Production-style launch with an explicit password.
SEATUNNEL_WEB_PASSWORD='s3cret!' seatunnel-web --master 127.0.0.1:5800
```

Behavior:

- `POST /api/v1/login` sets an `HttpOnly; SameSite=Strict` session cookie
  (HMAC-SHA256 signed with a random per-boot key, so restarting the
  console invalidates all sessions).
- Everything under `/api/*` except `/api/v1/login`, `/api/v1/logout` and
  `/api/v1/health` returns `401` without a valid session; `/metrics` and
  the static SPA assets stay public.
- The SPA probes `GET /api/v1/whoami` on load: no session → login screen;
  an expired session mid-use redirects back to it.
- Failed logins answer `401` after a short fixed delay to slow down
  brute-forcing.


Pages:

- **Overview** — job counts by state, cluster summary, health badge.
- **Jobs** — list, submit (YAML/TOML/HOCON editor dialog), cancel with
  confirmation. The table auto-refreshes every 5 seconds (toggle in the
  top bar).
- **Job detail** — basic info, per-task status with processed-record
  counters, **throughput (rec/s)** and **idle time** (seconds since the
  task last processed a record — the key lag signal for streaming sync;
  green < 10s, amber < 30s, red beyond), live per-task logs (lifecycle
  events, checkpoints, sampled data rows), and checkpoint history.
- **Cluster** — registered workers, leader, heartbeats.

## REST API

All endpoints are JSON under `/api/v1`:

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/login` | Exchange username/password for a session cookie |
| POST | `/api/v1/logout` | Clear the session cookie |
| GET | `/api/v1/whoami` | Identity of the current session |
| GET | `/api/v1/health` | Web liveness + master reachability probe |
| GET | `/api/v1/overview` | Job counts by state + cluster summary |
| GET | `/api/v1/jobs` | List jobs (newest first) |
| GET | `/api/v1/jobs/{id}` | Job status incl. parallelism, per-task errors and checkpoint counters |
| POST | `/api/v1/jobs` | Submit a job config |
| POST | `/api/v1/jobs/{id}/cancel` | Cancel a job |
| POST | `/api/v1/jobs/{id}/restart` | Restart a historical job (same id + retained config; checkpoint restore) |
| DELETE | `/api/v1/jobs/{id}` | Delete a TERMINAL job from history (state + checkpoint metadata) |
| GET | `/api/v1/jobs/{id}/checkpoints` | Checkpoint history metadata |
| GET | `/api/v1/jobs/{id}/logs` | Per-task live log lines |
| GET | `/api/v1/jobs/{id}/logs/stream` | SSE stream of per-task log deltas (~1 s cadence; full snapshot on connect) |
| GET | `/api/v1/jobs/{id}/history` | Console-side sampled throughput/sink-latency series for the charts |
| GET | `/api/v1/cluster` | Workers (incl. cpu, owned task ids) and leader info |
| GET | `/api/v1/cluster/workers/{worker_id}` | Worker drill-down: the task summaries this worker owns |
| GET | `/api/v1/cluster/history` | Console-side sampled worker load/memory/cpu series |
| GET | `/api/v1/logs/files` | Node's daily rolling log files (`--log-dir` required) |
| GET | `/api/v1/logs/files/{name}?tail=&level=&q=&download=1` | Filtered tail of one log file (raw attachment with `download=1`) |
| GET | `/api/v1/logs/files/{name}/stream?level=&q=&tail=` | SSE tail of one log file: initial snapshot, then only new lines |
| GET | `/metrics` | Prometheus exposition |

Submit body:

```json
{
  "config_text": "env:\n  job.name: demo\nsource:\n  Fake:\n    row.num: 100\nsink:\n  Console: {}",
  "format": "yaml",
  "job_name": "demo",
  "parallelism": 1
}
```

`format` is `yaml` (default), `toml` or `hocon`. The config is parsed and
validated (`source`/`sink` must be present) before it reaches the master;
parse errors return HTTP 400 with an `{"error": "..."}` body.

Prometheus metrics: `seatunnel_jobs{state}`, `seatunnel_workers`,
`seatunnel_running_tasks`, `seatunnel_task_processed_records{job,task}`,
`seatunnel_task_records_per_second{job,task}`,
`seatunnel_task_idle_seconds{job,task}` and HTTP request
counters/histograms for the console itself. Engine gauges are refreshed by
a background poller (default 5 s, `--refresh-interval-secs`).

Throughput and idle are derived server-side from consecutive reads;
workers ship record counters, the last-record timestamp and incremental
log lines with each 2 s heartbeat.

## Console features

- **Bilingual UI** — the console ships English and 中文 dictionaries with a
  topbar language toggle (persisted to localStorage, browser language as
  the default).
- **Charts** — the job detail page plots per-task throughput and sink
  latency; the cluster page plots per-worker load/memory/CPU. Series come
  from the console's own sampling ring (default 240 samples at the 5 s
  refresh interval ≈ 20 minutes; `--history-points` on the standalone
  binary). The ring lives in the console process: it restarts empty.
- **Job management** — filter by state, search by name/ID, sort, paginate
  (50/page), multi-select batch stop, submit from text or file (format
  auto-detected, JSON pre-checked client-side), stop (final checkpoint =
  savepoint semantics), delete terminal jobs from history, restart or
  edit-and-restart with checkpoint restore.
- **Cluster** — worker table with CPU column and leader badge, Masters
  (raft members) panel, worker drill-down page with the tasks a worker
  owns.
- **Node logs** — the Logs page live-tails the node's daily rolling log
  files over the same SSE transport, with level/substring filters and
  download. The embedded console derives the directory from the engine's
  `--log-dir` automatically; the standalone `seatunnel-web` binary takes
  the same flag.
- **Live log viewer** — job task logs and node logs stream in near
  real time (~1 s server-side cadence) instead of periodic full
  refreshes. The pane follows the newest line automatically; scrolling
  away pauses following and shows a "back to bottom" button, and
  following resumes on return. The whole panel toggles fullscreen
  (Esc exits).
- **Dark mode** — topbar toggle, persisted; the whole palette is themed
  via CSS variables.

## Streaming demo

`examples/web-streaming-demo.yaml` runs an unbounded FakeSource
(`row.num: -1`, ~100 rec/s via `sleep.ms: 10`) into the Console sink —
submit it from the Jobs page (it is the dialog's default config) to watch
throughput, idle, checkpoints and live logs in real time. Cancel it from
the table when done.

## Frontend development

The UI lives in `seatunnel-web/ui`, a standalone crate
(Leptos 0.8 CSR + trunk) that is intentionally **not** a workspace member:
it only compiles for `wasm32-unknown-unknown`. The built `dist/` is
committed, so building or running the console with plain `cargo` requires
no frontend toolchain.

Developing the UI requires:

```bash
rustup target add wasm32-unknown-unknown
cargo install trunk
```

- Hot-reload dev loop: run `seatunnel-web` on `127.0.0.1:8080`, then
  `trunk serve` inside `seatunnel-web/ui` (it proxies
  `/api/*` and `/metrics` to `127.0.0.1:8080` — see `Trunk.toml`).
- Production build: `scripts/build-web-ui.sh` (it runs
  `trunk build --release` and refuses a bundle that looks like a debug
  build) regenerates `dist/`; commit the result and rebuild
  `seatunnel-web` to embed it. Never commit a `dist/` produced by a bare
  `trunk build`: its debug-profile wasm is ~33 MB, which once made the
  console take ~10 s per page load. The release bundle (size-tuned
  profile + wasm-opt) is a few MB, and the server compresses assets on
  the fly (gzip/brotli, static fallback only — SSE streams are never
  compressed) and marks hashed assets `immutable`, so the console loads
  in well under a second.

## Architecture notes

- `seatunnel-web/src/engine.rs` defines the `EngineOps` trait (list /
  status / submit / cancel / cluster / checkpoints). Production wires the
  gRPC `EngineClient`; unit tests use an in-memory fake, so handlers are
  tested without a cluster.
- Checkpoint history needs the `GetJobCheckpoints` RPC introduced in
  `seatunnel-engine-comm` (`master.proto`), plus the
  `checkpoint_interval_ms` / `checkpoints_completed` / `end_time` fields on
  `JobStatus` / `JobSummary`.
- In the default cluster checkpoint mode the payload bytes stay on the
  workers, so the history panel shows the interval and completed counter;
  per-task entries list when the master-backed checkpoint store is used.

## Cluster page — dynamic admission visibility

The workers table shows each worker's measured admission state (no slot
counts exist to show):

- **Status** — `accepting`, or `OVERLOADED` while the worker is past a
  pressure watermark (it then receives no new tasks and its pending
  tasks may be stolen by healthy peers)
- **Load** — measured pressure 0-100% (the max of the signal ratios);
  a color-graded bar (blue → amber → red)
- **Lag (ms)** — event-loop lag EMA, the runtime saturation signal
- **Memory** — process RSS as a percentage of usable memory
- **Running tasks** — tasks currently executing on the worker

The header card "Overloaded workers" counts non-accepting workers, and
the leader line shows the fencing term and node role. The same signals
are exported as Prometheus gauges (`seatunnel_worker_load_score`,
`seatunnel_worker_overloaded`, `seatunnel_worker_lag_ms`,
`seatunnel_worker_mem_ratio`).

## Job detail — edit and restart

The job detail page carries an **编辑配置并重启 (Edit & restart)** button
(**以同 ID 重新提交** for terminal jobs). It opens an editor pre-filled
with the job configuration EXACTLY as submitted (stored verbatim at
submission time and returned by the status API), plus a format selector —
"auto-detect" (default), JSON, YAML or TOML, so the pasted config can be
in the same YAML the job files are authored in.

Confirming runs the update flow — identical to `seatunnel job update`:

1. cancel the running incarnation (its cancel path takes the automatic
   exit checkpoint: final sink flush + source position);
2. wait for CANCELLED; on timeout the update ABORTS without resubmitting
   (old and new never run in parallel);
3. resubmit the edited config under the same job id — tasks restore from
   their latest checkpoint and continue from the exact source position
   (at-least-once; exactly-once with transactional sinks).

The job name survives the update: an explicit `job_name` in the request
wins, then the edited config's own `env.job.name`, and when neither is
present the job keeps its current name (only a nameless, unknown job
falls back to the job id). `POST /api/v1/jobs` resolves names the same
way, falling back to a `job-<uuid prefix>` default.

The update validates the config BEFORE cancelling anything: YAML/TOML
edits go through the same parser as submit (source/sink presence
checked, then rebuilt into the canonical JSON document), so an unusable
edit is rejected with the parse error while the running job keeps its
current config. A failed update also stays on the page as a sticky
error banner (not just a transient toast), with the editor kept open.

The request may take up to the cancel timeout (~seconds normally); the
page shows the progress message while it runs and the status panel
refreshes automatically afterwards.

`POST /api/v1/jobs/{job_id}/update` with
`{"config_text": "<edited JSON>", "cancel_timeout_secs": 60}` is the
backing API.

## Restarting a historical job

`POST /api/v1/jobs/{job_id}/restart` re-runs a job with its retained
config under the SAME id — no request body needed. The engine cancels a
still-running incarnation first (exit checkpoint, same never-in-parallel
safety as the update flow), then resubmits from the config stored at
submission time; tasks resume from their latest checkpoint (batch jobs
without checkpoints cold-start). On a cancel timeout the restart aborts
without resubmitting and returns HTTP 400 explaining why.

Note this is the manual path — a full service restart recovers
non-terminal jobs automatically (see the restart-recovery section in
[production readiness](production-readiness.md)); the endpoint is for
finished/failed/cancelled jobs you want to run again.
