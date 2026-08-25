# SeaTunnel Rust

A Rust implementation of Apache SeaTunnel — a distributed data integration engine.

## Features

- **Dual Execution Modes**: Local (embedded) and Cluster (gRPC-based Master/Worker)
- **Checkpoint Fault Tolerance**: Barrier-based alignment, 2PC commit, exactly-once semantics
- **CDC Connectors**: MySQL (binlog+GTID), TiDB (TiKV CDC), PostgreSQL (logical replication)
- **Kafka Source/Sink**: Partition splits, 5 startup modes, format-based serialization
- **11 Data Formats**: JSON, Text, Canal JSON, Debezium JSON, Compatible Debezium, Kafka Connect, OGG JSON, Maxwell JSON, Avro, Protobuf, Native
- **Type-Safe Data Model**: Field enum (21 variants), Row, ColumnType, TableSchema

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
├── seatunnel-cli/                  # CLI with clap + ratatui
├── seatunnel-macros/               # Factory registration macros
├── seatunnel-benchmarks/           # Criterion benchmarks
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

### Run Tests

```bash
cargo test --workspace
# 114 tests pass across 23 crates
```

### Run Benchmarks

```bash
cargo bench
```

### Run CLI (Local Mode)

```bash
cargo run --bin seatunnel -- run -c config/v2.stream.template.conf -m local
```

### Docker

```bash
docker build -t seatunnel-rust .
docker run -p 5000:5000 seatunnel-rust run -c /opt/seatunnel/config/v2.stream.template.conf -m local
```

### Docker Compose (Full Stack)

```bash
docker-compose up -d
```

Spawns: Zookeeper + Kafka + MySQL + PostgreSQL + SeaTunnel Engine

## Architecture

### Execution Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| Local | Embedded Master + Worker in single process | Development, small jobs |
| Cluster | Separate Master/Worker nodes via gRPC | Production, multi-node |

### Checkpoint Protocol

1. Master sends Barrier to all source readers
2. Readers forward barrier through pipeline
3. Sinks ack barrier → Master collects checkpoints
4. 2PC commit (prepare → commit/abort)

### Data Flow

```
Source (Kafka/MySQL/TiDB/PostgreSQL)
  → Transform (Filter/Map/Fanout)
  → Sink (Kafka/Console/JDBC)
```

## Connectors

| Connector | Type | Features |
|-----------|------|----------|
| Kafka Source | Source | Partition splits, 5 startup modes, all formats |
| Kafka Sink | Sink | 2PC commit, batch flush, all formats |
| MySQL CDC | Source | Binlog streaming, GTID, snapshot+incremental |
| TiDB CDC | Source | TiKV key range, resolved_ts, CDC client |
| PostgreSQL CDC | Source | Logical replication, LSN, publication/slot |
| JDBC | Source | Generic JDBC-compatible |

## Documentation

- [Quick Start](docs/en/quickstart.md)
- [Architecture](docs/en/architecture.md)
- [Kafka Connector](docs/en/connectors/kafka.md)

## License

Apache License 2.0
