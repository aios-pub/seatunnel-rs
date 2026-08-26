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
- values: strict `yyyy-MM-dd HH:mm:ss` dates → epoch-millis numbers;
  `"0"` / zero-leading-free digit strings → longs (leading zeros preserved
  as strings); everything else stays a string;
- the Kafka partition key is the configured primary-key value;
- the CDC delete(before)+insert(after) pair of one UPDATE is re-paired into
  a single `update` message with `oldData` (2s pairing window; an
  unpaired before-image is emitted as a real delete).

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
