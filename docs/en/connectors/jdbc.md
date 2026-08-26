# JDBC Connector (MySQL / TiDB / PostgreSQL)

A unified JDBC-style connector speaking the native wire protocol of each
database family: MySQL and TiDB over the MySQL protocol (mysql_async),
PostgreSQL over libpq (tokio-postgres), both through pooled async
endpoints shared by source and sink.

## Source

Bounded snapshot read with parallel keyset splits (Java: `JdbcSource` +
`FixedChunkSplitter`). The schema is discovered from the catalog; the
primary key (or an explicit `partition.column`) is split into
`partition.num` ranges and each subtask reads its own slice with keyset
pagination (`WHERE pk > ? AND pk < ? ORDER BY pk LIMIT fetch.size`).

```yaml
source:
  JDBC:
    url: "jdbc:mysql://127.0.0.1:3306/seatunnel"   # or jdbc:postgresql://host:5432/db
    username: root
    password: root
    table: users                                   # db-qualified names allowed
    partition.column: id        # optional; defaults to the primary key
    partition.num: 4            # optional; defaults to parallelism
    fetch.size: 1024
    # query: "SELECT ... WHERE ..."  # custom query: single split, subtask 0 only
```

Non-integer split columns fall back to a single full-table split with
offset paging executed by subtask 0.

## Sink

Batched writes with dialect-native upserts
(`INSERT ... ON DUPLICATE KEY UPDATE` / `ON CONFLICT ... DO UPDATE SET`),
primary-key deletes, save modes and mid-stream schema evolution.

```yaml
sink:
  JDBC:
    url: "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username: root
    password: root
    table: users_sink
    primary-keys: f0            # key column(s); positional rows use f0..fN names
    enable-upsert: true         # default true
    batch.size: 1000
    max-retries: 3
    schema-save-mode: create_when_not_exist   # recreate_schema | create_when_not_exist | error_when_schema_not_exist | ignore
    data-save-mode: append_data               # drop_data | append_data | error_when_data_exists | custom_processing
    # custom-sql: "TRUNCATE ..."              # for custom_processing
    # columns: id,name,score                  # optional names for positional rows
```

### Save modes

| Mode | Behavior |
| --- | --- |
| `recreate_schema` | DROP + CREATE the target table (from the discovered/provided/inferred schema) |
| `create_when_not_exist` | CREATE the table when missing; inferred from the first batch otherwise |
| `error_when_schema_not_exist` | Fail when the table is missing |
| `ignore` | Never touch the schema |
| `drop_data` | TRUNCATE existing rows after the table is ready |
| `custom_processing` | Execute `custom-sql` |

### Schema evolution

The sink implements `apply_schema_change`: buffered rows are flushed,
then `ALTER TABLE ... ADD/DROP/MODIFY/RENAME COLUMN` is executed per the
dialect before any row with the new shape is written. For auto-created
(positional `f0..fN`) sink tables, source column names are translated by
ordinal — see [schema evolution](../schema-evolution.md).

## Notes

- Postgres binds most values as text with explicit placeholder casts
  (`$1::bigint`, `$2::numeric`, `$3::date`, ...) for full type coverage.
- Both drivers use persistent pools (mysql_async `Pool` / an internal
  async Postgres pool) — connections are reused across batches.
