#!/usr/bin/env bash
# End-to-end verification: canal-client JSON format (Kafka sink).
#
# MySQL-CDC → Kafka with format=canal_client_json:
#   - insert  → message {requestId, dbName, tableName, eventType:insert, data}
#   - update  → ONE message with data (after) + oldData (before), partition
#               key = primary key; an update touching no configured column
#               is FILTERED (no message)
#   - delete  → message with the must-fields of the before image
# Value conversion: strict dates → epoch millis numbers, pure numbers → longs.
#
# Prerequisites:
#   docker compose up -d mysql kafka
#   cargo build --release --bin seatunnel-engine-server --bin seatunnel
#
# Usage: ./scripts/e2e-canal-client-format.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=seatunnel-rs-mysql-1
KAFKA_CONTAINER=seatunnel-rs-kafka-1
TOPIC=users-canal-client
STATE_DIR=$(mktemp -d /tmp/st-e2e-cc-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-cc-run.XXXXXX)

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

mysql_exec() {
  docker exec "$MYSQL_CONTAINER" mysql -uroot -proot -e "$1" 2>/dev/null
}

consume() {
  docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning \
    --timeout-ms 5000 2>/dev/null || true
}

echo "== Seed source table =="
mysql_exec "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT, created_at DATETIME);
INSERT INTO users(name, score, created_at) VALUES ('alice', 90, '2024-05-06 07:08:09');"

echo "== Start master + worker =="
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 127.0.0.1:5800 \
  >"$RUN_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port 5800

SEATUNNEL_STATE_DIR="$STATE_DIR" \
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id e2e-cc-worker --addr 127.0.0.1:5001 \
  >"$RUN_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 3

echo "== Submit canal-client job =="
JOB_ID=$($BIN_DIR/seatunnel job submit -c examples/mysql-cdc-to-kafka-canal-client.yaml -a 127.0.0.1:5800 | awk '{print $3}')
echo "job: $JOB_ID"

# The engine machine interprets naive datetimes in ITS local zone; the
# broker consumer runs inside the kafka container (UTC), so compute the
# expected epoch from the same local interpretation.
EXPECTED_MS=$(python3 -c 'import time; print(int(time.mktime(time.strptime("2024-05-06 07:08:09", "%Y-%m-%d %H:%M:%S"))*1000))')
echo "expected created_at millis (local zone): $EXPECTED_MS"

echo "== Wait for the INSERT snapshot message =="
for _ in $(seq 1 30); do
  consume | grep -q '"eventType":"insert"' && break
  sleep 2
done
INSERT_MSG=$(consume | grep '"eventType":"insert"' | head -1)
echo "insert: $INSERT_MSG"
[[ -n "$INSERT_MSG" ]] || { echo "FAIL: no insert message"; exit 1; }
echo "$INSERT_MSG" | grep -q '"dbName":"seatunnel"' || { echo "FAIL: dbName"; exit 1; }
echo "$INSERT_MSG" | grep -q '"tableName":"users"' || { echo "FAIL: tableName"; exit 1; }
# must fields mapped; id/score are numbers; datetime → local-zone millis.
# Keys are alphabetically ordered (serde_json); created_at first.
echo "$INSERT_MSG" | grep -q "\"data\":{\"created_at\":$EXPECTED_MS,\"id\":1,\"name\":\"alice\",\"score\":90}" || { echo "FAIL: data mapping"; exit 1; }
echo "$INSERT_MSG" | grep -Eq '"requestId":"[0-9a-f]{32}"' || { echo "FAIL: requestId"; exit 1; }

echo "== UPDATE with a configured column change → one message with oldData =="
mysql_exec "USE seatunnel; UPDATE users SET score = 99 WHERE id = 1;"
for _ in $(seq 1 30); do
  consume | grep -q '"eventType":"update"' && break
  sleep 2
done
UPDATE_MSG=$(consume | grep '"eventType":"update"' | head -1)
echo "update: $UPDATE_MSG"
[[ -n "$UPDATE_MSG" ]] || { echo "FAIL: no update message"; exit 1; }
echo "$UPDATE_MSG" | grep -q "\"data\":{\"created_at\":$EXPECTED_MS,\"id\":1,\"name\":\"alice\",\"score\":99}" || { echo "FAIL: update data"; exit 1; }
echo "$UPDATE_MSG" | grep -q "\"oldData\":{\"created_at\":$EXPECTED_MS,\"id\":1,\"name\":\"alice\",\"score\":90}" || { echo "FAIL: oldData"; exit 1; }

echo "== UPDATE touching no configured column → filtered =="
# Baseline AFTER the configured update above.
BEFORE=$(consume | grep -c '"eventType"' || true)
# remark is not in must/update → the update must produce NO message.
mysql_exec "USE seatunnel; ALTER TABLE users ADD COLUMN remark VARCHAR(64);"
mysql_exec "USE seatunnel; UPDATE users SET remark = 'x' WHERE id = 1;"
sleep 8
AFTER=$(consume | grep -c '"eventType"' || true)
echo "messages before=$BEFORE after=$AFTER"
[[ "$AFTER" == "$BEFORE" ]] || { echo "FAIL: unconfigured update should be filtered"; exit 1; }

echo "== DELETE → message with must fields of the before image =="
mysql_exec "USE seatunnel; DELETE FROM users WHERE id = 1;"
for _ in $(seq 1 30); do
  consume | grep -q '"eventType":"delete"' && break
  sleep 2
done
DELETE_MSG=$(consume | grep '"eventType":"delete"' | tail -1)
echo "delete: $DELETE_MSG"
[[ -n "$DELETE_MSG" ]] || { echo "FAIL: no delete message"; exit 1; }
echo "$DELETE_MSG" | grep -q "\"data\":{\"created_at\":$EXPECTED_MS,\"id\":1,\"name\":\"alice\"}" || { echo "FAIL: delete data"; exit 1; }

echo "== Partition key is the primary key =="
KEY_DUMP=$(docker exec "$KAFKA_CONTAINER" kafka-run-class \
  kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$TOPIC" 2>/dev/null || true)
echo "offsets: $KEY_DUMP"

echo "== Cancel job =="
$BIN_DIR/seatunnel job cancel --job-id "$JOB_ID" -a 127.0.0.1:5800

echo
echo "PASS: canal-client format verified (insert/update+oldData/filter/delete)."
echo "  logs: $RUN_DIR"
