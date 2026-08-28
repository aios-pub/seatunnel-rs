# Engine Configuration (`seatunnel.yaml`)

The engine server (master or worker) reads a startup config file —
`--config config/seatunnel.yaml` (short `-f`). The key layout mirrors the
Java Zeta engine's `seatunnel.yaml` where the concepts map onto this
implementation; precedence is **`--state-dir` > `SEATUNNEL_STATE_DIR` >
config file > built-in defaults**.

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
    checkpoint:
      interval: 30000
      keep-checkpoint-count: 3
      storage:
        type: localfile
        namespace: .seatunnel-state
        auto-clean: true
        clean-grace-minutes: 10
        clean-interval-minutes: 60
```

## Key reference (Java → this project)

| Key | Default | Java counterpart | Behavior |
| --- | --- | --- | --- |
| `seatunnel.engine.history-job-expire-minutes` | 1440 (24h) | same key | TTL sweep removes a job's local state after this much idleness |
| `seatunnel.engine.worker-soft-timeout-ms` | 30000 | — (two-level, this project) | a worker silent longer than this is suspected: it gets no NEW task assignments (still registered, running tasks untouched) |
| `seatunnel.engine.worker-timeout-ms` | 60000 | Hazelcast `max.no.heartbeat.seconds` (180s) analogue | hard eviction: silent longer than this → removed from the registry, its tasks become claimable (failover) |
| `seatunnel.engine.heartbeat-interval-ms` | 2000 | — | worker → master heartbeat period; the master may adjust it per response (`next_interval_ms`) |
| `seatunnel.engine.slot-num` | — (deprecated) | `slot-num` | IGNORED. Static slot budgets are gone; admission is dynamic (see the four keys below). |
| `seatunnel.engine.overload-lag-ms` | 500 | — (this project) | a worker whose event-loop lag EMA reaches this stops accepting new tasks (hysteresis applies); 0 disables the signal |
| `seatunnel.engine.memory-watermark-percent` | 75 | — | a worker whose RSS crosses this percent of usable memory (cgroup v2 limit when present, else physical RAM) stops accepting; 0 disables |
| `seatunnel.engine.overload-cooldown-secs` | 10 | — | recovery hysteresis: an overloaded worker accepts again only after every signal stayed healthy this long |
| `seatunnel.engine.dispatch-batch-limit` | 16 | — | rate fuse for the 1-3s admission blind window: max tasks handed to one worker per heartbeat (0 = unlimited). A RATE, not a slot count |
| `seatunnel.engine.checkpoint-timeout-ms` | 30000 | `checkpoint.timeout` analogue | a coordinated checkpoint that has not collected every task's prepare by then is aborted (workers unwind) |
| `seatunnel.engine.replication-interval-ms` | 5000 | — | master-to-master state replication period (HA standby sync) |
| `seatunnel.engine.worker-address` | 127.0.0.1:5001 | — | this worker's advertised address |
| `seatunnel.engine.checkpoint.interval` | 30000 | `checkpoint.interval` | engine-wide default checkpoint interval (ms); a job's `env.checkpoint.interval` overrides it per job |
| `seatunnel.engine.checkpoint.keep-checkpoint-count` | 3 | same key | checkpoint files retained per task; older ones are pruned on every write |
| `seatunnel.engine.checkpoint.storage.type` | localfile | same key | `localfile` \| `master` (shared store on the master) \| `s3` (direct writes) |
| `seatunnel.engine.checkpoint.storage.namespace` | .seatunnel-state | `plugin-config.namespace` analogue | local directory for checkpoint files |
| `...storage.auto-clean` | true | — (this project) | enable terminal-job state cleanup |
| `...storage.clean-grace-minutes` | 10 | — | delay after a job cancel before its state is deleted (restore window) |
| `...storage.clean-interval-minutes` | 60 | — | how often the TTL sweep runs (plus once at startup) |

Dispatch is long-polled: workers send `wait_ms` with every heartbeat and
the master parks the request until work appears, so task handout does not
wait out the heartbeat interval.

**Dynamic admission (no slot counts).** "Is this worker full?" is answered
by measured pressure, never by a number: a worker accepts new tasks while
its event-loop lag EMA stays under `overload-lag-ms` and its RSS under
`memory-watermark-percent` of usable memory. An overloaded worker gets no
new tasks and its PENDING (never-dispatched) tasks are stolen by healthy
peers — RUNNING tasks are never stolen (eviction only, protecting
checkpoint consistency). When every worker is over a watermark,
submissions still succeed and tasks queue as SCHEDULED until pressure
clears. The signals and the admission verdict are visible in the web
console's cluster page and as Prometheus gauges.

Failure-detection defaults are deliberately conservative (soft 30 s /
hard 60 s): the Java engine ships a 180 s heartbeat tolerance because a
27-second full-GC stall once crossed a 60 s timeout and split the
cluster — false evictions are worse than slow failover. See
[Cluster HA Design](cluster-ha-design.md).

Java keys with no equivalent yet (ignored if present): `backup-count`,
`queue-type`, `slot-service.*`, `classloader-cache-mode`,
`print-execution-info-interval`, telemetry/metrics sections.

## Disk lifecycle (no manual cleanup needed)

- **While a job runs**: each task keeps at most
  `keep-checkpoint-count` snapshot files (auto-pruned on write).
- **Job cancelled**: its whole state directory is deleted
  `clean-grace-minutes` after the cancel.
- **Anything else** (finished bounded jobs, failed jobs, orphans from a
  crashed worker): the background TTL sweep — run at worker startup and
  every `clean-interval-minutes` — removes job directories whose newest
  file is older than `history-job-expire-minutes`.
- Setting `auto-clean: false` disables all automatic deletion (behavior
  before this feature).

## Checkpoint & watermark status vs Java

- **Local mode (`seatunnel run -m local`)** ships the full Java-style
  protocol: a `LocalCheckpointDriver` coordinates global checkpoint ids,
  triggers a barrier on every live task (prepare_commit → sink snapshot →
  reader snapshot), aggregates all reports into one durable envelope
  (atomic tmp+fsync+rename), then broadcasts completion so sink committers
  run 2PC phase 2 and readers commit external offsets. Restart restores
  readers/writers from the newest envelope and continues the id sequence.
  Exactly-once sinks: Kafka (per-checkpoint transactions, stable
  transactional id fencing zombies) and JdbcXa (MySQL XA, strict 2PC with
  `XA RECOVER` settlement). CDC checkpoints land on transaction boundaries
  (after-XID of the last fully emitted transaction). Graceful SIGINT/SIGTERM
  takes a final savepoint checkpoint before tasks are cancelled.
- **Cluster mode** checkpoints are master-driven and coordinated
  (Stage 2): the master assigns the checkpoint id per pipeline, every
  running task cuts the barrier (flush sink → snapshot source, prepare),
  and only after the master collected all prepares do workers run 2PC
  phase 2 (`SinkCommitter::commit` + reader offset commit). Failures and
  timeouts (`checkpoint-timeout-ms`) abort and unwind. Restore works for
  all CDC sources (binlog position/GTID, TiDB resolved_ts, PG LSN) and
  Kafka offsets, always re-reading the durable stores (local disk >
  master store / S3).
- Watermarks: the CDC `WatermarkBuffer` exists but no connector uses it;
  there is no event-time/watermark processing in the engine
  (processing-time pass-through).
