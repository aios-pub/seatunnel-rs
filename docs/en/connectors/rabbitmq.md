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
