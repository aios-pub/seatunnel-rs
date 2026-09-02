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
| `seatunnel-engine-server` | Cluster node: `--role hybrid` (recommended), `master`, or `worker` |

> Note: macOS reserves port 5000 (AirPlay), so the engine defaults to **5800**.

## Start a Cluster

One hybrid process = coordinator (Raft voter) + worker — the recommended
single-machine form:

```bash
seatunnel-engine-server --role hybrid --addr 127.0.0.1:5800
# or: ./scripts/start-hybrid.sh   # builds, waits for readiness, prints status
```

For HA, run 3 symmetric hybrid nodes — Raft elects the coordinator, and
voter counts must be odd (two voters are rejected at startup):

```bash
# on each of the three machines (same member-list everywhere):
seatunnel-engine-server --role hybrid --addr 0.0.0.0:5800 \
  --advertise-addr node1.example.com:5800 -f config/seatunnel-3node.yaml
# local rehearsal on one machine:
./scripts/start-hybrid-cluster.sh   # 3 voters on 127.0.0.1:5800/5810/5820
```

Large clusters can instead separate the roles — 3 masters (Raft voters)
plus N dedicated workers:

```bash
seatunnel-engine-server --role master --addr 0.0.0.0:5800
seatunnel-engine-server --role worker --master 127.0.0.1:5800 \
  --worker-id worker-1 --addr 0.0.0.0:5001
```

See [Cluster HA Design](cluster-ha-design.md) for the trade-offs.

## Logging

The engine server logs to **stdout** (containers, foreground runs) and, in
addition, to **daily rolling files** named `<role>.YYYY-MM-DD` under
`<state-dir>/logs`, keeping at most **30 files** (~30 days; the oldest is
pruned on rotation). Master and worker processes write separate files
(`master.*` / `worker.*`); a `hybrid` process runs both in one process and
writes its own `hybrid.*` files:

```
<state-dir>/logs/
  master.2026-09-01
  master.2026-09-02
  worker.2026-09-02
```

| Flag / env | Default | Behavior |
|------------|---------|----------|
| `--log-dir` / `SEATUNNEL_LOG_DIR` | `<state-dir>/logs` | directory for the rolling files |
| `--log-level` / `SEATUNNEL_LOG` | `info` | filter: `--log-level` > `--debug` > `RUST_LOG` > `info` |

Pass `--log-dir none` to disable file logging (stdout only). If the log
directory cannot be opened, the server starts anyway and logs a warning to
stdout. The startup scripts discard the child's stdout (the engine writes
the rolling files itself) and keep stderr — panics and pre-logging startup
failures — in `console.err` next to the `logs/` directory.

> Note: the file-rotation date is computed by the `time` crate, which may
> fall back to UTC in multi-threaded processes on Unix — the day boundary
> of the file name can then be local midnight shifted by your UTC offset.
> Rotation and retention are unaffected; log timestamps use local time.

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
