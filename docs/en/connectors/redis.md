# Redis Connector

Source and sink over a single-node or cluster Redis, using a multiplexed
async connection (`ConnectionManager`) or a cluster connection with
automatic routing.

## Source

`SCAN`-based bounded read over a key pattern. Every scanned key's type is
probed (`TYPE`) and materialized accordingly:

| Redis type | Row shape |
| --- | --- |
| string | `[key, value]` |
| hash | `[key, field, value]` per field |
| list | `[key, member]` per member |
| set | `[key, member]` per member |
| zset | `[key, member, score]` per member |

```yaml
source:
  Redis:
    host: 127.0.0.1
    port: 6379
    # auth: secret
    # user: default
    # db-num: 0
    # mode: cluster           # single (default) | cluster
    # nodes: "10.0.0.1:6379,10.0.0.2:6379"
    keys: "user:*"            # SCAN MATCH pattern, default *
    batch-size: 100
```

## Sink

Batched pipeline writes per `data-type`, with row-kind-aware deletes and
optional key TTL. Keys and hash fields support `${fN}` field placeholders
(rows arrive positionally as `f0..fN`).

```yaml
sink:
  Redis:
    host: 127.0.0.1
    port: 6379
    data-type: hash          # string | hash | list | set | zset
    key: "users:${f0}"       # default ${f0}
    hash-field: "${f1}"      # hash data-type only
    # value-field: f2        # write a single field instead of the row
    format: json             # json | text
    # field-delimiter: ","
    batch-size: 100
    # expire: 3600           # key TTL seconds; -1 disables
```

| RowKind | string | hash | list | set | zset |
| --- | --- | --- | --- | --- | --- |
| INSERT / UPDATE_AFTER | `SET` | `HSET` | `LPUSH` | `SADD` | `ZADD` (score 1) |
| DELETE | `DEL` | `HDEL` | `LREM` | `SREM` | `ZREM` |

Redis is schemaless: schema-change events force a flush of rows
serialized with the old shape, then writes continue with the new layout.
