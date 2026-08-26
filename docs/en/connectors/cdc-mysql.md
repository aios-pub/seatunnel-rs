# MySQL CDC Connector

Snapshot + binlog change data capture over the MySQL protocol
(mysql_async, ROW binlog format, GTID-aware), with the official 2.3.x
option set including multi-table / pattern selection.

## Requirements

- `binlog_format = ROW`, `binlog_row_image = FULL`
- `REPLICATION SLAVE, REPLICATION CLIENT` privileges for the user
- a unique `server-id` per dump connection (auto-derived when unset)

## Configuration

```yaml
source:
  MySQL-CDC:
    url: "jdbc:mysql://127.0.0.1:3306/seatunnel"   # official form; or hostname/port
    username: root
    password: root
    database-names: seatunnel                      # or database-name (single)
    table-names: seatunnel.users,seatunnel.orders  # exact db.table refs
    # table-pattern: "seatunnel\\.events_.*"       # regex over db.table
    startup.mode: initial
    split.column: id
```

### Connection

| Option | Default | Notes |
| --- | --- | --- |
| `url` | — | `jdbc:mysql://host:port/db`; hostname/port remain as simpler alternatives |
| `username` / `password` | — | credentials |
| `server-id` | auto-unique | single id (`5400`) or range (`5400-5408`, ids assigned inside the range) |
| `server-time-zone` | UTC | informational |
| `connection.pool.size` | 20 | pool max connections |
| `connect.timeout.ms` / `connect.max-retries` | 30000 / 3 | accepted; reconnect loop handles retries |

### Table selection

| Option | Notes |
| --- | --- |
| `database-name` / `database-names` | single name or comma list (exact) |
| `database-pattern` | regex over the database name |
| `table-name` / `table-names` | single name (trailing `%` wildcard) or comma list of `db.table` refs |
| `table-pattern` | regex over the qualified `db.table` |
| `table-names-config` | JSON list: `{"table": "db.tbl", "primaryKeys": [...], "snapshotSplitColumn": "..."}` — the split-column override is applied |

Official options **replace** the legacy single-name forms when present.
Selection applies to both snapshot enumeration (matched tables resolved
via `information_schema`, snapshotted in parallel chunks) and the binlog
filter. Schema-evolution watchers are created per captured table.

### Startup / stop

| Option | Values |
| --- | --- |
| `startup.mode` | `initial` \| `snapshot-only` \| `earliest` \| `latest` \| `timestamp` \| `specific` |
| `startup.timestamp` | ms since epoch (for `timestamp`; streaming-only, replays retained binlog discarding older events) |
| `startup.specific-offset.file` / `.pos` / `.gtid-set` | exact binlog position (aliases: `startup.specific.*`) |
| `stop.mode` | `never` (default) \| `latest` \| `specific` \| `timestamp` |
| `stop.specific-offset.file` / `.pos`, `stop.timestamp` | stop boundary; the reader EOFs once the boundary is passed and buffered rows are drained |

See [startup modes](../startup-modes.md).

### Snapshot tuning

| Option | Default | Notes |
| --- | --- | --- |
| `snapshot.split.size` | 8096 | rows per snapshot chunk (caps the per-subtask span) |
| `snapshot.fetch.size` | 1024 | page size of each keyset query |
| `split.column` | id | chunking key |

### Accepted for compatibility (logged, not implemented)

`exactly_once` (delivery stays at-least-once), `format`, `debeziumConfig`,
`chunk-key.even-distribution.factor.*`, `sample-sharding.threshold`,
`inverse-sampling.rate`, `int_type_narrowing` — each is warned about once
at reader open with its actual behavior.

`schema-evolution.enabled` (alias of the Java `schema-changes.enabled`)
enables DDL capture from binlog query events — see
[schema evolution](../schema-evolution.md).

## How it works

1. **Snapshot**: subtask 0 records the binlog baseline (`SHOW MASTER
   STATUS`) and starts the dump **before** scanning; subtasks scan
   disjoint keyset chunks of every matched table (round-robin
   chunk→subtask assignment). Binlog rows decoded during the scan are
   buffered and replayed after the snapshot.
2. **Incremental**: only subtask 0 streams; row events are decoded
   positionally, UPDATEs become delete-before + insert-after; binlog
   rotations are tracked so checkpoints stay valid across files.
3. **Checkpointing**: binlog file/position (+ GTID set), split cursors
   and the split layout are persisted; restart resumes exactly.

Delivery semantics: at-least-once.
