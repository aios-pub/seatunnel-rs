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
| `seatunnel.engine.slot-num` | 8 | `slot-num` | task slot budget this worker advertises |
| `seatunnel.engine.replication-interval-ms` | 5000 | — | master-to-master state replication period (HA standby sync) |
| `seatunnel.engine.worker-address` | 127.0.0.1:5001 | — | this worker's advertised address |
| `seatunnel.engine.checkpoint.interval` | 30000 | `checkpoint.interval` | engine-wide default checkpoint interval (ms); a job's `env.checkpoint.interval` overrides it per job |
| `seatunnel.engine.checkpoint.keep-checkpoint-count` | 3 | same key | checkpoint files retained per task; older ones are pruned on every write |
| `seatunnel.engine.checkpoint.storage.type` | localfile | same key | `localfile` \| `master` (shared store on the master) \| `s3` (direct writes) |
| `seatunnel.engine.checkpoint.storage.namespace` | .seatunnel-state | `plugin-config.namespace` analogue | local directory for checkpoint files |
| `...storage.auto-clean` | true | — (this project) | enable terminal-job state cleanup |
| `...storage.clean-grace-minutes` | 10 | — | delay after a job cancel before its state is deleted (restore window) |
| `...storage.clean-interval-minutes` | 60 | — | how often the TTL sweep runs (plus once at startup) |

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
- **Cluster mode** checkpoints remain per-task, interval-driven
  flush-sinks-then-snapshot-reader (**at-least-once**); restore works for
  all CDC sources (binlog position/GTID, TiDB resolved_ts, PG LSN) and
  Kafka offsets.
- Watermarks: the CDC `WatermarkBuffer` exists but no connector uses it;
  there is no event-time/watermark processing in the engine
  (processing-time pass-through).
