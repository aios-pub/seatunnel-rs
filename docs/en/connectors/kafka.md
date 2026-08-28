# Kafka Connector

## Source

```toml
source {
  kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my-topic"
    format = "json"
    startup.mode = "earliest"
    # also: latest | group-offsets | timestamp (with startup.timestamp)
    #       | specific-offsets (with startup.specific-offsets: "0:100,1:250")
  }
}
```

## Canal-Client format (`format: canal_client_json`)

Reproduces the Java `AbstractCanalClient` message shape for the Kafka sink:

```json
{
  "requestId": "32-hex UUID without dashes",
  "dbName": "seatunnel", "tableName": "users",
  "eventType": "insert | update | delete",
  "data": {"id": 1, "name": "alice"},
  "oldData": {"id": 1, "name": "bob"}
}
```

Transformation rules (faithful to the Java implementation):

- table name → camelCase (`l_class_student` → `lClassStudent`) selects the
  per-table field mapping from `canal-client.sub-table-fields` (the Java
  `subTableFieldsJson` shape: `{"lClassStudent": {"key": "id", "must":
  {"db_col": "target"}, "update": {...}}}`);
- `must` fields always map; `update` fields only when the column changed;
  updates where no configured column changed are **filtered** (no message);
- deletes are never filtered and carry the must-fields of the before image;
- values: strict `yyyy-MM-dd HH:mm:ss` dates → epoch-millis numbers,
  interpreted in the **server's local timezone by default** (matching
  Java's `SimpleDateFormat` default); override with
  `canal-client.server-time-zone` (`local` | `UTC` | `+08:00` |
  `Asia/Shanghai` | …);
  `"0"` / zero-leading-free digit strings → longs (leading zeros preserved
  as strings); everything else stays a string;
- the Kafka partition key is the configured primary-key value;
- the CDC `UPDATE_BEFORE`+`UPDATE_AFTER` row pair of one UPDATE (explicit
  RowKind tags, mirroring the Java CDC contract) is merged into a single
  `update` message with `oldData`; real DELETE / INSERT rows are encoded
  and delivered immediately — the merge is deterministic and never holds
  them (a torn before-image without its partner is emitted as a real
  delete after `canal-client.pairing-window-ms`, default 100ms).

```yaml
sink:
  Kafka:
    bootstrap.servers: "127.0.0.1:9092"
    topic: users-canal-client
    format: canal_client_json
    canal-client.database-name: seatunnel
    canal-client.table-name: users
    canal-client.columns: "id,name,score"        # positional → db column
    canal-client.sub-table-fields: >-
      { "users": { "key": "id",
                   "must": { "id": "id", "name": "name" },
                   "update": { "score": "score" } } }
```

### Multi-table sources & per-table topics

With a CDC source that captures **several tables** (e.g.
`table-pattern: "shop\\..*"`), each row carries its origin
`database.table` identity and the encoder keeps one state per table:

- the message `dbName` / `tableName` follow the row's **real** table
  (not a static config value);
- every table maps with its **own** columns and primary key, taken from
  its per-table initial-schema event (automatic mode);
- the update pairing state is per-table — same-key rows of different
  tables never mis-pair into updates.

Per-message topic routing:

1. `topic-routes` — a JSON **array** of `{"pattern": ..., "topic": ...}`
   entries with ANCHORED regexes over the origin `database.table`.
   EVERY matching entry receives a copy of the message (the Java
   `table_topic_mappings` fan-out: a table listed under several topics
   is delivered to each); routes rendering the same topic name deliver
   only once. Use it to group several tables into one topic, or to
   double-route one table into several topics.
2. records matching no route fall back to the `topic` value, which may
   contain `${database}` / `${table}` placeholders (one topic per
   table). A template `topic` without row origin (non-CDC sources) is a
   configuration error — keep a literal topic for those.

```yaml
sink:
  Kafka:
    bootstrap.servers: "127.0.0.1:9092"
    topic: "cdc_${table}"                  # fallback: one topic per table
    format: canal_client_json
    topic-routes: >-
      [
        {"pattern": "seatunnel\\.orders_.*", "topic": "topic_orders"},
        {"pattern": "seatunnel\\.(entity_question|entity_school_ksystem)",
         "topic": "question_html_update_binlog"}
      ]
    canal-client.server-time-zone: local   # per-table automatic mapping
```

Copies fan out with the SAME payload and `requestId`, so consumers
seeing several topics can dedupe or join by `requestId`. Ordering per
topic is preserved (one producer, same partition key); delivery stays
at-least-once per topic — with `transactions.enabled=true` the copies
become atomically visible together at each checkpoint. See
`examples/mysql-cdc-multi-table-to-kafka-canal.yaml` for a full job.

## Sink

```toml
sink {
  kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my-sink"
    format = "json"
    transactions.enabled = true
  }
}
```
