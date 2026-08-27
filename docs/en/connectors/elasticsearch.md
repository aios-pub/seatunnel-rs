# Elasticsearch Connector

REST-based source and sink over `_bulk` / scroll APIs (reqwest, HTTP or
HTTPS hosts, basic auth).

## Sink

Batched `_bulk` writes:

- with `primary-keys`: `update` actions with `doc_as_upsert: true`
  (upsert semantics) and `delete` actions for DELETE / UPDATE_BEFORE rows;
- without keys: plain `index` actions.

```yaml
sink:
  Elasticsearch:
    hosts: "127.0.0.1:9200"        # comma list; https:// prefixes allowed
    # username: elastic
    # password: changeme
    index: users_idx                # ${fN}, ${database_name}, ${schema_name}, ${table_name}
    primary-keys: f0                # document _id fields, joined by key-delimiter
    key-delimiter: "_"
    max-batch-size: 200
    max-retry-count: 3
    schema-save-mode: create_when_not_exist
    data-save-mode: append_data     # drop_data | error_when_data_exists | custom_processing
    custom-processing-query: '{"query": {"term": {"stale": true}}}'   # for custom_processing
    enable-doc-delete: true         # Java defaults to false; deletes stay on here
```

Index placeholders resolve from the upstream table identifier
(`schema.table` → `${database_name}`/`${schema_name}`/`${table_name}`)
and, like the Java IndexTemplate, row values fill `${fN}` slots.

Schema save mode creates the index with an explicit mapping derived from
the (inferred) column types — STRING→keyword, INT64→long, DATE family→
date, JSON→object, arrays inherit the element type. `data-save-mode:
drop_data` clears existing documents via `_delete_by_query`;
`custom_processing` runs the `custom-processing-query` DSL through
`_delete_by_query` instead of the blanket match-all delete.

**Why not the official `elasticsearch` crate:** it is a generated,
fully-typed client pinned to the ES 8.x line (including cloud/XPack
modules), while this connector uses five REST endpoints (`_bulk`,
scroll search, index CRUD, `_count`, `_delete_by_query`) that are stable
across ES 7.x/8.x. The ~200-line reqwest client keeps compatibility with
both major versions without version-locking the dependency.

**Schema evolution:** `ADD COLUMN` is applied as a mapping update
(`PUT /{index}/_mapping`). Elasticsearch cannot drop fields or change
their types in place, so DROP/RENAME/MODIFY are logged as unsupported
after flushing the old-shape buffer.

## Source

Bounded scroll read of an index.

```yaml
source:
  Elasticsearch:
    hosts: "127.0.0.1:9200"
    index: users_idx
    scroll-time: 1m
    scroll-size: 100
    # query: '{"term": {"status": "active"}}'   # DSL; default match_all
```

Each hit becomes a row `[ _id, <sorted _source fields...> ]`; the scroll
context is deleted when exhausted or on close.
