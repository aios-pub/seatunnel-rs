# PostgreSQL CDC Connector

Logical replication (`pgoutput`) via rustcdc, with automatic slot and
publication provisioning, plus a parallel snapshot phase.

## Requirements

- `wal_level = logical`
- `REPLICATION` privilege
- tables need `REPLICA IDENTITY FULL` for full before-images (the
  publication is auto-provisioned under an advisory lock)

## Configuration

```yaml
source:
  Postgres-CDC:
    hostname: "127.0.0.1"
    port: 5432
    username: postgres
    password: postgres
    database-name: seatunnel
    schema-name: public
    table-name: users
    slot-name: seatunnel_slot        # auto-created when missing
    publication-name: seatunnel_pub  # auto-provisioned (ALTER PUBLICATION ... ADD TABLE)
    auto-create-slot: true
    startup.mode: initial            # initial | snapshot-only | earliest | latest
    split.column: id
    parallelism: 4
    # schema-evolution.enabled: true
```

## How it works

1. **Snapshot**: baseline `pg_current_wal_lsn()`, then disjoint keyset
   ranges per subtask; WAL events decoded during the scan are buffered and
   replayed afterwards.
2. **Incremental** (subtask 0): logical stream decoded to insert/update/
   delete rows using the cached column layout.
3. **Checkpoint**: LSN persisted; restart resumes the slot.

### Startup modes

`initial`, `snapshot-only`, `earliest`/`latest` (delegate to the slot's
confirmed LSN). There is **no timestamp mode** — logical replication has
no time→LSN mapping (same limitation as the Java implementation). See
[startup modes](../startup-modes.md).

Schema evolution polls `pg_attribute` on an interval and refreshes the
cached column layout — see [schema evolution](../schema-evolution.md).

Delivery semantics: at-least-once.
