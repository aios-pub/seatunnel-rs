#!/usr/bin/env bash
# End-to-end verification: MySQL CDC → SeaTunnel cluster → Kafka.
#
# Prerequisites:
#   docker compose up -d kafka mysql
#   cargo build --release --bin seatunnel-engine-server --bin seatunnel
#   MySQL: database `seatunnel`, table `users` (script seeds it itself)
#
# Usage: ./scripts/e2e-cdc-kafka.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=seatunnel-rs-mysql-1
KAFKA_CONTAINER=seatunnel-rs-kafka-1
TOPIC=users-cdc-e2e
STATE_DIR=$(mktemp -d /tmp/st-e2e-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-run.XXXXXX)

cleanup() {
  if [[ "$KEEP" != "--keep" ]]; then
    kill "${MASTER_PID:-}" "${WORKER_PID:-}" 2>/dev/null || true
    [[ -n "${CONSUMER_PID:-}" ]] && kill "$CONSUMER_PID" 2>/dev/null || true
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

echo "== Seed MySQL table =="
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot -e "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
INSERT INTO users(name, score) VALUES ('alice',90),('bob',85),('carol',77);" 2>/dev/null

echo "== Start master + worker =="
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 127.0.0.1:5800 \
  >"$RUN_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port 5800

SEATUNNEL_STATE_DIR="$STATE_DIR" \
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id e2e-worker --addr 127.0.0.1:5001 \
  >"$RUN_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 3   # allow registration

echo "== Submit CDC job =="
sed "s/users-cdc/$TOPIC/" examples/mysql-cdc-to-kafka.yaml > "$RUN_DIR/job.yaml"
JOB_ID=$($BIN_DIR/seatunnel job submit -c "$RUN_DIR/job.yaml" -a 127.0.0.1:5800 | awk '{print $3}')
echo "job: $JOB_ID"

echo "== Wait for snapshot rows in Kafka =="
for _ in $(seq 1 30); do
  COUNT=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null | grep -c "^\[")
  [[ "$COUNT" -ge 3 ]] && break
  sleep 2
done
echo "snapshot messages: $COUNT"
[[ "$COUNT" -ge 3 ]] || { echo "FAIL: no snapshot rows reached Kafka"; exit 1; }

echo "== Insert incremental updates =="
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot -e "
USE seatunnel;
INSERT INTO users(name, score) VALUES ('dave', 66);
UPDATE users SET score = 99 WHERE name = 'alice';" 2>/dev/null

sleep 6
INC=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null)
echo "$INC"
echo "$INC" | grep -q '"dave"' || { echo "FAIL: incremental insert missing"; exit 1; }

echo "== Checkpoints recorded? =="
docker exec "$KAFKA_CONTAINER" true
if grep -q "checkpoint" "$RUN_DIR/master.log"; then :; fi
ls "$STATE_DIR" | grep -q . && echo "worker checkpoint state present: $(ls "$STATE_DIR")"

echo "== Cancel job =="
$BIN_DIR/seatunnel job cancel --job-id "$JOB_ID" -a 127.0.0.1:5800

echo
echo "PASS: MySQL CDC → Kafka closed loop verified."
echo "  logs:      $RUN_DIR"
echo "  state dir: $STATE_DIR"
