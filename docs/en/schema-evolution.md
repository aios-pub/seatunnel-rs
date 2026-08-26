# Schema Evolution

Automatic handling of upstream DDL (add column, drop column, modify
column type, rename column) through the whole pipeline, mirroring the
Java fork's schema-evolution subsystem
(`connector-cdc` `schema-changes.enabled` + `SupportSchemaEvolutionSink`).

## Pipeline

```
 CDC source detects DDL
        │
        ▼  PollResult::SchemaChange(SchemaChangeEvent)
 engine TaskGroup
        │  (schema-change events overtake rows of the new shape)
        ▼  SinkWriter::apply_schema_change(event)
 sink flushes old-shape buffer, then applies the change to its storage
```

- **MySQL-CDC**: `ALTER TABLE` statements are captured directly from the
  binlog (`QueryEvent`) and parsed into typed changes — zero added
  latency.
- **TiDB-CDC**: TiKV EventFeedV2 carries no DDL, so column metadata is
  polled from `information_schema` on an interval and diffed; the decode
  schema of the TiKV engine is refreshed in place.
- **Postgres-CDC**: `pg_attribute` is polled and diffed; the cached
  column layout used to decode logical-stream rows is refreshed.

Rename detection heuristic: a diff that drops exactly one column and
adds exactly one column with an identical definition is emitted as a
rename (metadata polling cannot distinguish it from drop+add).

## Enabling

Off by default (Java parity). Enable on the CDC source:

```yaml
source:
  MySQL-CDC:
    ...
    schema-evolution.enabled: true
    schema-evolution.poll-interval-ms: 10000   # TiDB / Postgres polling only
```

## Sink support

| Sink | ADD | DROP | MODIFY | RENAME |
| --- | --- | --- | --- | --- |
| JDBC (MySQL/TiDB) | `ALTER TABLE ... ADD COLUMN` | `DROP COLUMN` | `MODIFY COLUMN` | `RENAME COLUMN` |
| JDBC (Postgres) | `ADD COLUMN` | `DROP COLUMN` | `ALTER COLUMN TYPE` (+ `SET/DROP NOT NULL`) | `RENAME COLUMN` |
| Elasticsearch | `PUT _mapping` (new field) | unsupported (logged) | unsupported (logged) | unsupported (logged) |
| Redis / Kafka / Console | schemaless / positional — old-shape rows are flushed, writes continue with the new shape | | | |

DDL re-application on at-least-once replay is tolerated: duplicate-column
and missing-column errors are treated as already-applied.

## Positional translation

CDC rows arrive positionally and auto-created sink tables know columns as
`f0..fN`. Schema changes carry the column's ordinal (`position`) in the
new layout, and `translate_positional` maps the source column name onto
the sink's `fN` name before the DDL is generated — e.g. a source
`ALTER TABLE users ADD COLUMN email VARCHAR(64)` becomes
`ALTER TABLE users_sink ADD COLUMN f3 TEXT` when the sink table was
auto-created with positional names. Sinks that preserve real column
names (pre-created tables, ES mappings with explicit names) use the
original name.

## End-to-end verification

`scripts/e2e-schema-evolution.sh` runs the full loop against a live
MySQL: snapshot → `ADD COLUMN` + insert → `MODIFY COLUMN` (INT→BIGINT) +
wide update → `DROP COLUMN` + insert, asserting every step landed in the
sink table (schema and data).
