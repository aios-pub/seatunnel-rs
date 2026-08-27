# SeaTunnel Rust

A Rust implementation of Apache SeaTunnel — a distributed data integration engine.

**Verified end-to-end**: MySQL binlog CDC → cluster submit → master scheduling →
worker execution → Kafka sink → periodic checkpoints, exercised continuously by
`scripts/e2e-cdc-kafka.sh` and the `seatunnel-e2e` test crate.

## Features

- **Dual Execution Modes**: Local (embedded) and Cluster (gRPC-based Master/Worker)
- **Chained Pipelines**: Source → Transform → Sink executed per subtask with real dataflow
- **Checkpoint Fault Tolerance**: local mode runs the full Java-style coordinator protocol — global barrier checkpoints, durable envelopes, two-phase commit (Kafka transactions / MySQL XA), `kill -9` recovery with no data loss
- **Real CDC Connectors**: MySQL (binlog+GTID, keyset-paginated snapshot, partitioned ranges), TiDB (TiKV CDC), PostgreSQL (logical replication)
- **Kafka Source/Sink**: consumer groups, optional transactional producer, batch flush at checkpoint boundaries (KRaft-mode compose stack, no ZooKeeper)
- **11 Data Formats**: JSON, Text, Canal JSON, Debezium JSON, Compatible Debezium, Kafka Connect, OGG JSON, Maxwell JSON, Avro, Protobuf, Native
- **Type-Safe Data Model**: Field enum, Row, ColumnType, TableSchema

## Project Structure

```
seatunnel-rs/
├── seatunnel-api/                  # Core data model + connector traits
├── seatunnel-config/               # TOML/YAML/HOCON config parsing
├── seatunnel-formats/              # 11 data format serializers/deserializers
├── seatunnel-engine/
│   ├── seatunnel-engine-core/      # DAG, checkpoint, state backend
│   ├── seatunnel-engine-comm/      # gRPC (tonic + prost)
│   ├── seatunnel-engine-server/    # Master + Worker
│   └── seatunnel-engine-client/    # Client library
├── seatunnel-connectors/
│   ├── seatunnel-connector-common/ # Shared base classes
│   ├── seatunnel-connector-kafka/  # Kafka Source + Sink
│   ├── seatunnel-connector-cdc-base/ # CDC framework
│   ├── seatunnel-connector-cdc-mysql/  # MySQL CDC
│   ├── seatunnel-connector-cdc-tidb/   # TiDB CDC
│   ├── seatunnel-connector-cdc-postgres/ # PostgreSQL CDC
│   └── seatunnel-connector-jdbc/       # JDBC source
├── seatunnel-transforms/           # Filter, Map, Fanout, Rename, Select
├── seatunnel-cli/                  # CLI: local runs + cluster job management
├── seatunnel-macros/               # Factory registration macros
├── seatunnel-benchmarks/           # Criterion benchmarks
├── seatunnel-e2e/                    # Docker-based end-to-end tests
├── scripts/e2e-cdc-kafka.sh         # Automated CDC→Kafka verification
├── Dockerfile
├── docker-compose.yml
├── config/
└── docs/
```

## Quick Start

### Build

```bash
cargo build --release
```

### MySQL CDC → Kafka in 5 commands

```bash
docker compose up -d kafka mysql
seatunnel-engine-server --role master --addr 0.0.0.0:5800 &
SEATUNNEL_STATE_DIR=./state seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id w1 --addr 0.0.0.0:5001 &
seatunnel job submit -c examples/mysql-cdc-to-kafka.yaml -a 127.0.0.1:5800
```

See [Quick Start](docs/en/quickstart.md) for the full walkthrough, and
`scripts/e2e-cdc-kafka.sh` for an automated verification of the whole loop.

### Run Tests

```bash
cargo test --workspace
```

The docker-based closed-loop test (Kafka + MySQL required):

```bash
docker compose up -d kafka mysql
cargo test -p seatunnel-e2e --test e2e
```

## Architecture

### Execution Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| Local | Embedded Master + Worker in single process | Development, small jobs |
| Cluster | Separate Master/Worker nodes via gRPC | Production, multi-node |

### Checkpoint Protocol

1. Worker task reaches its checkpoint interval
2. Sink `prepare_commit` flushes buffered records downstream first
3. Source state (binlog file/position/GTID) is captured after the flush
4. State persists to the worker's local store and reports to the master;
   a restarted task resumes from the newest checkpoint

Delivery semantics (local mode): exactly-once for MySQL XA sinks; for
Kafka transactional sinks every checkpoint is one atomic transaction with
**no data loss** and a bounded duplicate window across kill -9 restarts
(keyed upserts absorb it). Cluster mode remains **at-least-once** (bounded duplicate window across the
snapshot/stream overlap). The Kafka sink additionally supports an optional
transactional producer (`transactions.enabled: true`) for read_committed
consumers.

### Data Flow

```
Source (MySQL CDC / Kafka / TiDB / PostgreSQL)
  → Transform (Filter)
  → Sink (Kafka / JDBC / Console)
```

Each subtask chains all three stages in one TaskGroup; parallelism splits the
source (e.g. disjoint MySQL id ranges) across subtasks distributed over workers.

## Connectors

| Connector | Type | Features |
|-----------|------|----------|
| MySQL CDC | Source | Binlog streaming + GTID, keyset snapshot with partitioned ranges, checkpoint resume |
| Kafka Source | Source | Consumer groups, startup modes, offset checkpoints |
| Kafka Sink | Sink | Batching, optional transactional producer, format encoding |
| TiDB CDC | Source | TiKV key range, resolved_ts, CDC client |
| PostgreSQL CDC | Source | Logical replication, LSN, publication/slot |
| JDBC | Source/Sink | MySQL/PostgreSQL dialects, batched reads, prepared writes |
| Console | Sink | JSON lines to stdout (local runs / smoke tests) |

## Documentation

- [Quick Start](docs/en/quickstart.md)
- [Architecture](docs/en/architecture.md)
- [Kafka Connector](docs/en/connectors/kafka.md)

## License

Apache License 2.0
