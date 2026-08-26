# MySQL CDC Connector

Snapshot + binlog change data capture over the MySQL protocol
(mysql_async, ROW binlog format, GTID-aware).

## Requirements

- `binlog_format = ROW`, `binlog_row_image = FULL`
- `REPLICATION SLAVE, REPLICATION CLIENT` privileges for the user
- a unique `server-id` per dump connection (auto-derived when unset)

## Configuration

```yaml
source:
  MySQL-CDC:
    hostname: "127.0.0.1"
    port: 3306
    username: root
    password: root
    database-name: seatunnel
    table-name: users          # trailing % wildcard supported
    startup.mode: initial      # initial | snapshot-only | earliest | latest | timestamp | specific
    split.column: id           # snapshot chunking key (default id)
    parallelism: 4
    # server-id: 65001         # auto-unique when omitted
    # schema-evolution.enabled: true
```

### Startup modes

| Mode | Behavior |
| --- | --- |
| `initial` | snapshot (parallel keyset chunks partitioned across subtasks) then live binlog stream |
| `snapshot-only` | stop after the snapshot |
| `earliest` / `latest` | streaming only |
| `timestamp` | streaming only, replaying the retained binlog and discarding events older than `startup.timestamp` (ms) |
| `specific` | stream from `startup.specific.file` / `.pos` / optional `.gtid-set` |

See [startup modes](../startup-modes.md) for details and semantics.

## How it works

1. **Snapshot**: subtask 0 records the binlog baseline (`SHOW MASTER
   STATUS`) and starts the dump **before** scanning; other subtasks scan
   disjoint primary-key ranges with keyset pagination. Binlog rows decoded
   during the scan are buffered and replayed after the snapshot (dedup
   below max snapshot pk).
2. **Incremental**: only subtask 0 streams; row events are decoded
   positionally, UPDATEs become delete-before + insert-after.
3. **Checkpointing**: binlog file/position (+ GTID set), split cursors and
   the pending split layout are persisted; restart resumes exactly.
   Rotations are tracked so reconnects stay valid across binlog files.
4. **Schema evolution** (opt-in): `ALTER TABLE` statements captured from
   binlog query events are parsed into typed schema changes and forwarded
   to sinks — see [schema evolution](../schema-evolution.md).

Delivery semantics: at-least-once.
