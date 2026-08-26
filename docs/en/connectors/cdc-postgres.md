# PostgreSQL CDC Connector

Logical replication (`pgoutput`) via rustcdc, with automatic slot and
publication provisioning, a parallel snapshot phase and the official
2.3.x option set.

## Requirements

- `wal_level = logical`
- `REPLICATION` privilege
- tables need `REPLICA IDENTITY FULL` for full before-images (the
  publication is auto-provisioned under an advisory lock)

## Configuration

```yaml
source:
  Postgres-CDC:
    url: "jdbc:postgresql://127.0.0.1:5432/seatunnel"  # official form; or hostname/port
    username: postgres
    password: postgres
    schema-name: public
    table-names: seatunnel.public.users,analytics.events  # db.table / db.schema.table refs
    # table-pattern: ".*\\.events_.*"                    # regex over schema.table / db.table
    slot.name: seatunnel_slot
    startup.mode: initial
    split.column: id
```

### Options

| Option | Default | Notes |
| --- | --- | --- |
| `url` | — | `jdbc:postgresql://host:port/db`; hostname/port remain as alternatives |
| `username` / `password` | — | credentials |
| `database-name` / `database-names` | — | one database per job (extra `database-names` entries are warned about) |
| `schema-name` | public | default schema for legacy single-table selection |
| `table-name` / `table-names` | — | single name or comma list; entries may be `db.table` or `db.schema.table` — the last component is the table |
| `table-pattern` | — | regex over `schema.table` / `db.table` |
| `slot.name` (alias `slot-name`) | seatunnel_slot | auto-created when absent |
| `publication-name` | seatunnel_pub | auto-provisioned (`ALTER PUBLICATION ... ADD TABLE`) |
| `decoding.plugin.name` | pgoutput | only `pgoutput` is supported; other plugins are warned about |
| `startup.mode` | initial | `initial` \| `snapshot-only` \| `earliest` \| `latest` |
| `snapshot.split.size` | 8096 | rows per snapshot chunk (caps the per-subtask span) |
| `snapshot.fetch.size` | 1024 | page size of each keyset query |
| `split.column` | id | chunking key |

Multi-table selections snapshot tables **sequentially** (per table:
layout cache, ranges, scan) and route streamed events by their
`schema.table`, each decoded with that table's cached layout. Checkpoints
persist the remaining table queue.

### Accepted for compatibility (logged, not implemented)

`exactly_once`, `format`, `debeziumConfig`, `chunk-key.*` factors,
`sample-sharding.threshold`, `inverse-sampling.rate`, `connect.*` — each
is warned about once at reader open.

There is **no timestamp startup mode** — logical replication has no
time→LSN mapping (same limitation as the Java implementation); see
[startup modes](../startup-modes.md).

## How it works

1. **Snapshot**: baseline `pg_current_wal_lsn()`, then disjoint keyset
   ranges per subtask; WAL events decoded during the scan are buffered and
   replayed afterwards.
2. **Incremental** (subtask 0): logical stream decoded to insert/update/
   delete rows; schema-evolution polls `pg_attribute` and refreshes cached
   layouts — see [schema evolution](../schema-evolution.md).
3. **Checkpoint**: LSN + split cursors persisted; restart resumes the slot.

Delivery semantics: at-least-once.
