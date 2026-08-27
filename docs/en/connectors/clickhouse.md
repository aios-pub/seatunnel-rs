# ClickHouse Connector

Source and sink over the official `clickhouse` crate (HTTP transport,
rustls TLS, LZ4 compression).

## Sink

Dynamic rows cannot ride the crate's typed `insert<T>()` path, so rows
are serialized to `JSONEachRow` and streamed through
`insert_formatted_with` (the crate's pre-formatted insert channel).
At-least-once: batches flush on size, linger, checkpoint and close.

```yaml
sink:
  ClickHouse:
    url: http://127.0.0.1:8123        # HTTP interface endpoint
    database: default
    table: events
    username: default
    password: ""
    primary-keys: id                  # ReplacingMergeTree ORDER BY → idempotent replays
    max-batch-size: 1000
    max-retry-count: 3
    schema-save-mode: create_when_not_exist
    data-save-mode: append_data       # drop_data (TRUNCATE) | error_when_data_exists
    # columns: "id,name"              # explicit output column names
```

- With `primary-keys` the auto-created table is a `ReplacingMergeTree`
  ordered by those keys, so checkpoint replays deduplicate on merge;
  without keys it is a plain `MergeTree`. Column types are inferred from
  the first row (explicit `columns` names the columns).
- `Bytes` fields are hex-encoded and `Decimal` values quoted into String
  columns; temporals use ClickHouse's `YYYY-MM-DD hh:mm:ss` text form.
- DELETE / UPDATE_BEFORE rows are skipped with a warning — ClickHouse
  has no synchronous row deletes; use `ReplacingMergeTree` upserts.

## Source

Bounded read driven by `toJSONString(tuple(*))`: the server serializes
each row into a single JSON string that is decoded positionally using
the column order from `system.columns` / `DESCRIBE`. With exactly one
`primary-keys` column the reader pages with a `WHERE pk > last` cursor,
so checkpoint restore resumes exactly where the snapshot stopped;
otherwise LIMIT/OFFSET paging is used.

```yaml
source:
  ClickHouse:
    url: http://127.0.0.1:8123
    database: default
    table: events
    username: default
    password: ""
    primary-keys: id                  # cursor column for resumable reads
    fetch-size: 1000
    # query: "SELECT * FROM events WHERE region = 'eu'"   # custom base query
```

A custom `query` must be deterministic (stable ordering) — cursor paging
falls back to LIMIT/OFFSET over the query result.
