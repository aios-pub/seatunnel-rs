# Startup Modes & Resuming from a Point in Time

How each source positions itself when a job starts, and how to begin
syncing from a **specified timestamp**.

## Overview

| Connector | `initial` (snapshot+stream) | `earliest` / `latest` | **`timestamp`** | `specific` / offsets |
| --- | --- | --- | --- | --- |
| MySQL-CDC | ✅ | ✅ | ✅ `startup.timestamp` (ms) | ✅ file + position + GTID set |
| TiDB-CDC | ✅ | ✅ | ✅ `startup.timestamp` (ms) | — |
| PostgreSQL-CDC | ✅ | ✅ | ❌ (protocol limit, see below) | resumes from slot LSN |
| Kafka | — | ✅ | ✅ `startup.timestamp` (ms) | ✅ `startup.specific-offsets` |
| JDBC | bounded snapshot (no stream) | — | — | — |

All CDC sources also restore from the last **checkpoint** (binlog
file/position, resolved_ts, or LSN) when the job is resubmitted — explicit
startup modes only apply when no checkpoint exists.

## MySQL-CDC: `startup.mode = timestamp`

```yaml
source:
  MySQL-CDC:
    ...
    startup.mode: timestamp
    startup.timestamp: 1667232000000   # milliseconds since epoch
```

Semantics (verified by `scripts/e2e-mysql-cdc-timestamp.sh`):

- **streaming-only** — no snapshot; rows existing before the timestamp are
  not emitted;
- the reader registers its binlog dump at the **earliest retained binary
  log** and replays it, discarding every event whose header timestamp is
  older than `startup.timestamp` (a tight warm-up loop drains historical
  events at full speed);
- the first event at/after the boundary switches the reader into normal
  streaming;
- if the requested time is older than the binlog retention
  (`binlog_expire_logs_seconds`), replay starts at the oldest retained
  event — same limitation as any MySQL replica.

### Choosing the timestamp

`startup.timestamp` is an integer **milliseconds since the Unix epoch
(UTC)** — timezone-independent, because binlog event headers store epoch
seconds. Both the `startup.timestamp` and `startup_timestamp` spellings
are accepted; a missing or non-positive value fails the job at submit
time.

Generate it at the moment that should become the boundary:

```bash
date +%s%3N                                            # GNU date (Linux)
echo "$(date +%s)000"                                  # macOS date (second precision)
mysql -h <host> -e 'SELECT ROUND(UNIX_TIMESTAMP(NOW(3))*1000)'   # the source server's own clock
```

Two caveats verified against a live server:

- **±1s boundary granularity** — binlog event headers carry second
  precision, so changes committed in the same second as the boundary can
  land on either side. Leave a small gap before the boundary when the
  exact cut matters.
- **retention-gap check** — if the timestamp is OLDER than the earliest
  retained binlog event, the purged gap can never be captured, and the
  task FAILS at startup with the exact UTC timestamps involved
  (`startup.timestamp.retention-check: warn` downgrades this to an ERROR
  log and starts at the oldest retained event anyway).

`startup.mode = specific` starts at an exact position:

```yaml
    startup.mode: specific
    startup.specific.file: binlog.000003
    startup.specific.pos: 987
    # startup.specific.gtid-set: "626cb905-...:1-134"   # optional
```

## TiDB-CDC: `startup.mode = timestamp`

```yaml
source:
  TiDB-CDC:
    ...
    startup.mode: timestamp
    startup.timestamp: 1667232000000   # milliseconds since epoch
```

TiDB TSOs encode `physical_milliseconds << 18 | logical`, so the wall-clock
start time is converted directly into the MVCC scan point
(`checkpoint_ts`). Two consequences:

- no snapshot — the TiKV change feed replays versions committed at/after
  the target TSO;
- the time must lie within the **GC lifetime** (`tikv_gc_life_time`,
  default 10 minutes in test clusters, often hours in production); older
  timestamps are rejected by TiKV.

## Kafka: `startup.mode = timestamp`

```yaml
source:
  Kafka:
    ...
    startup.mode: timestamp
    startup.timestamp: 1667232000000
```

Each assigned partition resolves its start offset via the broker's
`offsetsForTimes` (first message with `ts >= target`). Also available:
`earliest`, `latest`, `group-offsets`, and
`startup.specific-offsets: "0:100,1:250"`.

## PostgreSQL-CDC: no timestamp mode

Logical replication exposes no time→LSN mapping; a slot replays from its
confirmed LSN. `earliest`/`latest` delegate to the slot position. This
matches the Java implementation's limitation.

## Checkpoint restore precedence

For every CDC source, restore order on job resubmission is:

1. durable checkpoint state (last completed checkpoint), if present;
2. otherwise the configured `startup.mode`.

## Updating a running job's configuration

Changing a RUNNING job's config is **stop-and-restart under the same job
id** — never run old and new in parallel (they would double-consume the
source).

```bash
# CLI: cancel (automatic exit checkpoint) → resubmit same id → restore
seatunnel job update -c job.v2.yaml -i <job-id> [-a master] [--cancel-timeout-secs 60]
```

The same flow is available from the web console (job detail →
**编辑配置并重启**) and as a shared library call
(`seatunnel_engine_client::update_job`) — one implementation, three
entry points.

How data is preserved:

1. cancel triggers the **exit checkpoint** (final sink flush, then source
   position) — the de-facto savepoint;
2. the flow waits for CANCELLED and aborts (never resubmitting) on
   timeout — a partially-stopped job is never raced with a new one;
3. resubmission with the same job id restores every task from its latest
   checkpoint (`restored checkpoint cp-N` in worker logs): at-least-once,
   exactly-once with transactional sinks (Kafka transactions, JdbcXa).

Preconditions: cross-worker restore requires
`checkpoint.storage.type: s3 | master` (localfile restores only on the
same worker); keep the same parallelism (task ids and partition splits
are parallelism-bound); resubmit within the clean-grace window
(default 10 min — `job update` does it in seconds).
