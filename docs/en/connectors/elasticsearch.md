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
    index: users_idx                # supports ${fN} variable placeholders
    primary-keys: f0                # document _id fields, joined by key-delimiter
    key-delimiter: "_"
    max-batch-size: 200
    max-retry-count: 3
    schema-save-mode: create_when_not_exist
    data-save-mode: append_data     # drop_data (delete_by_query) | error_when_data_exists
```

Schema save mode creates the index with an explicit mapping derived from
the (inferred) column types — STRING→keyword, INT64→long, DATE family→
date, JSON→object, arrays inherit the element type. `data-save-mode:
drop_data` clears existing documents via `_delete_by_query`.

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
