# SeaTunnel Rust Quick Start

## Installation

```bash
cargo build --release
# binaries land in $CARGO_TARGET_DIR/release (or ./target/release)
```

Two binaries:

| Binary | Purpose |
|--------|---------|
| `seatunnel` | CLI: local runs, cluster job submission & management |
| `seatunnel-engine-server` | Cluster node: `--role master` or `--role worker` |

> Note: macOS reserves port 5000 (AirPlay), so the engine defaults to **5800**.

## Start a Cluster

```bash
# terminal 1 — master
seatunnel-engine-server --role master --addr 0.0.0.0:5800

# terminal 2 — worker
seatunnel-engine-server --role worker --master 127.0.0.1:5800 \
  --worker-id worker-1 --addr 0.0.0.0:5001
```

## MySQL CDC → Kafka (cluster mode)

1. Start infrastructure and seed data:

```bash
docker compose up -d kafka mysql
docker exec seatunnel-rs-mysql-1 mysql -uroot -proot -e "
  CREATE DATABASE IF NOT EXISTS seatunnel;
  USE seatunnel;
  CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
  INSERT INTO users(name,score) VALUES ('alice',90),('bob',85);"
```

2. Submit the example job (`examples/mysql-cdc-to-kafka.yaml`):

```bash
seatunnel job submit -c examples/mysql-cdc-to-kafka.yaml -a 127.0.0.1:5800
# Submitted job job-xxxx
seatunnel job status --job-id job-xxxx -a 127.0.0.1:5800
seatunnel job list   -a 127.0.0.1:5800
seatunnel cluster    -a 127.0.0.1:5800
seatunnel job cancel --job-id job-xxxx -a 127.0.0.1:5800
```

3. Watch rows stream into Kafka — snapshot first, then live binlog changes:

```bash
docker exec seatunnel-rs-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic users-cdc --from-beginning
```

Or run the whole loop automatically:

```bash
./scripts/e2e-cdc-kafka.sh
```

## Local Mode

Local runs execute the same connector chain in-process (no master needed):

```bash
seatunnel run -c examples/mysql-cdc-to-kafka.yaml -m local
```

Streaming sources keep running until Ctrl-C.

## Checkpointing

- Interval comes from `env.checkpoint.interval` (ms) in the job config.
- Every checkpoint flushes the sink **before** recording the source offset
  (at-least-once, no-loss).
- Workers persist offsets under `$SEATUNNEL_STATE_DIR`
  (default `./.seatunnel-state`) and resume from them on restart.

## Development

```bash
cargo test --workspace                       # unit + integration tests
cargo test -p seatunnel-e2e --test e2e       # docker-based end-to-end test
cargo bench                                  # criterion benchmarks
```

## PostgreSQL CDC → Kafka

Same flow, logical replication instead of binlog (publication + slot are
provisioned automatically):

```bash
docker exec seatunnel-rs-postgres-1 psql -U postgres -d seatunnel -c "
  CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(64), score INT);
  INSERT INTO users(name,score) VALUES ('alice',90);"
seatunnel job submit -c examples/postgres-cdc-to-kafka.yaml -a 127.0.0.1:5800
```

Kafka runs in **KRaft mode** (`docker-compose.yml`) — a single broker process
with no ZooKeeper dependency.
