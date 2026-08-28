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
> The demo script (or starting a worker manually) fixes it.

### Manual startup

Start a cluster (master + at least one worker), then the console:

```bash
seatunnel-engine-server --role master --addr 127.0.0.1:5800
seatunnel-engine-server --role worker --addr 127.0.0.1:5801 \
    --master 127.0.0.1:5800 --worker-id w1

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
| GET | `/api/v1/jobs/{id}` | Job status incl. tasks and checkpoint counters |
| POST | `/api/v1/jobs` | Submit a job config |
| POST | `/api/v1/jobs/{id}/cancel` | Cancel a job |
| GET | `/api/v1/jobs/{id}/checkpoints` | Checkpoint history metadata |
| GET | `/api/v1/jobs/{id}/logs` | Per-task live log lines |
| GET | `/api/v1/cluster` | Workers and leader info |
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
- Production build: `trunk build --release` regenerates `dist/`; commit
  the result and rebuild `seatunnel-web` to embed it.

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
submission time and returned by the status API).

Confirming runs the update flow — identical to `seatunnel job update`:

1. cancel the running incarnation (its cancel path takes the automatic
   exit checkpoint: final sink flush + source position);
2. wait for CANCELLED; on timeout the update ABORTS without resubmitting
   (old and new never run in parallel);
3. resubmit the edited config under the same job id — tasks restore from
   their latest checkpoint and continue from the exact source position
   (at-least-once; exactly-once with transactional sinks).

The request may take up to the cancel timeout (~seconds normally); the
page shows the progress message while it runs and the status panel
refreshes automatically afterwards.

`POST /api/v1/jobs/{job_id}/update` with
`{"config_text": "<edited JSON>", "cancel_timeout_secs": 60}` is the
backing API.
