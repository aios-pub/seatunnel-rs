#!/usr/bin/env bash
# End-to-end verification: TiDB CDC (TiKV EventFeedV2) → SeaTunnel cluster → Kafka.
#
# Verifies LIVE delta streaming: rows inserted AFTER the job started must
# reach Kafka within seconds (this is the regression the memcomparable
# span-encoding fix guards against — subscribing with raw keys resolves to
# the wrong region and no PREWRITE/COMMIT is ever delivered).
#
# Prerequisites:
#   docker compose --profile tidb up -d        # pd + tikv + tidb + kafka
#   docker compose up -d kafka                 # (kafka is not in the tidb profile)
#   cargo build --release -p seatunnel-cli -p seatunnel-engine-server
#   A mysql client container able to reach TiDB at host.docker.internal:14000
#   (the seatunnel-rs-mysql-1 container provides one).
#
# Usage: ./scripts/e2e-tidb-cdc.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-seatunnel-rs-mysql-1}
KAFKA_CONTAINER=${KAFKA_CONTAINER-seatunnel-rs-kafka-1}
TIDB_HOST=${TIDB_HOST:-host.docker.internal}
TIDB_PORT=${TIDB_PORT:-14000}
STATE_DIR=$(mktemp -d /tmp/st-e2e-tidb-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-tidb-run.XXXXXX)

mysql_cmd() {
  docker exec "$MYSQL_CONTAINER" mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root "$@"
}

cleanup() {
  if [[ "$KEEP" != "--keep" ]]; then
    kill "${MASTER_PID:-}" "${WORKER_PID:-}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 60); do
    nc -z 127.0.0.1 "$1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "port $1 never came up" >&2; exit 1
}

echo "== Seed TiDB table =="
mysql_cmd -e "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
INSERT INTO users(name, score) VALUES ('alice',90),('bob',85);" 2>/dev/null

echo "== Start master + worker =="
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 127.0.0.1:5800 \
  >"$RUN_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port 5800

SEATUNNEL_STATE_DIR="$STATE_DIR" \
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id e2e-tidb --addr 127.0.0.1:5001 \
  >"$RUN_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 3   # allow registration

echo "== Submit TiDB CDC job =="
JOB_ID=$($BIN_DIR/seatunnel job submit -c examples/tidb-cdc-to-kafka.yaml -a 127.0.0.1:5800 | awk '{print $3}')
echo "job: $JOB_ID"

echo "== Wait for snapshot rows in Kafka =="
for _ in $(seq 1 30); do
  COUNT=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic tidb-users-cdc --from-beginning --timeout-ms 5000 2>/dev/null | grep -c "^\[" || true)
  [[ "${COUNT:-0}" -ge 2 ]] && break
  sleep 2
done
echo "snapshot messages: $COUNT"
[[ "$COUNT" -ge 2 ]] || { echo "FAIL: no snapshot rows reached Kafka"; exit 1; }

echo "== Live writes AFTER registration (the delta path) =="
mysql_cmd -e "
USE seatunnel;
INSERT INTO users(name, score) VALUES ('live_dave', 66);
UPDATE users SET score = 99 WHERE name = 'alice';
DELETE FROM users WHERE name = 'bob';" 2>/dev/null

echo "== Verify live deltas reached Kafka =="
for _ in $(seq 1 20); do
  INC=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic tidb-users-cdc --from-beginning --timeout-ms 5000 2>/dev/null || true)
  echo "$INC" | grep -q "live_dave" && break
  sleep 2
done
echo "$INC" | grep -q "live_dave" || { echo "FAIL: live insert missing from Kafka"; exit 1; }
echo "$INC" | grep -qE '\[1,"alice",99\]' || { echo "FAIL: live update missing from Kafka"; exit 1; }
echo "live delta messages delivered:"
echo "$INC" | grep -E "live_dave|alice\",99|\"bob\"" || true

echo "== Cancel job =="
$BIN_DIR/seatunnel job cancel --job-id "$JOB_ID" -a 127.0.0.1:5800

echo
echo "PASS: TiDB CDC live streaming → Kafka verified."
echo "  logs:      $RUN_DIR"
echo "  state dir: $STATE_DIR"
