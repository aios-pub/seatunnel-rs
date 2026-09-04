# RabbitMQ Connector

AMQP source and sink over the `lapin` client (Java: `connector-rabbitmq`).

## Source

Consumes a queue with manual acknowledgements. Deliveries are only acked
in `notify_checkpoint_complete` — the deferred-ack pattern the Kafka
source uses for offsets — so a crash before the checkpoint completes
causes the broker to redeliver (**at-least-once**). Undecodable (poison)
payloads are acked immediately so they cannot redeliver forever.

Parallel subtasks each run their own consumer on the same queue; the
broker round-robins deliveries between them.

```yaml
source:
  RabbitMQ:
    host: 127.0.0.1
    port: 5672
    virtual-host: /                  # percent-encoded automatically
    username: guest
    password: guest
    queue-name: events
    exchange: ""                     # optional: bind queue to an exchange
    exchange-type: direct            # direct | fanout | topic | headers
    routing-key: ""                  # optional binding key
    prefetch-count: 250
    format: json                     # any seatunnel-formats format
    # columns: "id,name,age"         # optional; enables schema-based decoding
    poll.timeout.ms: 250
```

Rows are positional, mirroring the Kafka source: with `columns` set,
messages are decoded against that schema; otherwise JSON objects map
sorted keys to positions, JSON arrays elementwise, and TEXT payloads
become a single string field.

## Sink

Batched publishes flushed on batch size, linger, checkpoint and close.
With `publisher-confirm: true` (default) each publish is awaited until
the broker acks it. An empty `exchange` routes straight to the queue
through the default exchange.

```yaml
sink:
  RabbitMQ:
    host: 127.0.0.1
    port: 5672
    username: guest
    password: guest
    queue-name: events_out           # durable; declared (and bound) on open
    exchange: ""                     # or an existing exchange + routing-key
    exchange-type: direct            # used only when the exchange must be created
    routing-key: ""
    persistent: true                 # delivery_mode = 2
    publisher-confirm: true
    format: json
    max-batch-size: 100
    batch.timeout.ms: 100
```

## Topology declaration

Exchanges and queues are declared **passive-first**: when the entity
already exists on the broker it is left untouched — its type, durability
and flags may differ from the connector defaults without failing the job
(RabbitMQ rejects a mismatched active redeclare with
`PRECONDITION_FAILED`). `exchange-type` (default `direct`) only applies
when the exchange does not exist and has to be created. The queue binding
is applied afterwards and is idempotent.

## Canal-client format (CDC → RabbitMQ)

With `format: canal_client_json` the sink encodes CDC rows through the
same stateful canal-client encoder the Kafka sink uses, producing the
Java-`AbstractCanalClient`-compatible envelope:

```json
{
  "requestId": "32-hex UUID (fresh per message)",
  "dbName": "neworiental_user",
  "tableName": "entity_user",
  "eventType": "insert | update | delete",
  "data":    { "<column>": "<value>", ... },
  "oldData": { "<column>": "<value>", ... }
}
```

UPDATE binlog pairs merge into ONE message (`data` = after image,
`oldData` = before image, both full column maps); a held before-image
whose after-partner never arrives within
`canal-client.pairing-window-ms` (default 100) is emitted as a delete.
Column mapping is automatic: the sink registers each table from the
source's initial-schema events (the writer must NOT receive rows before
any schema event), or you can pin an explicit mapping with
`canal-client.columns` + `canal-client.sub-table-fields`. Options
(mirroring the Kafka sink): `canal-client.database-name`,
`canal-client.table-name`, `canal-client.columns`,
`canal-client.sub-table-fields`, `canal-client.server-time-zone`,
`canal-client.pairing-window-ms`. All messages share the configured
exchange/routing-key — `message.table`-based routing is a Kafka-topic
concept and is ignored here.

```yaml
sink:
  RabbitMQ:
    host: 127.0.0.1
    port: 5672
    queue-name: cdc_events
    exchange: exchange_canal_sync_user
    routing-key: cdc_events
    format: canal_client_json
    canal-client.server-time-zone: local   # per-table automatic mapping
```
