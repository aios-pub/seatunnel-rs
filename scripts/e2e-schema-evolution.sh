#!/usr/bin/env bash
# End-to-end verification: MySQL CDC (schema evolution) → JDBC sink.
#
# Proves the automatic schema-evolution pipeline:
#   snapshot rows land in an auto-created sink table,
#   ALTER TABLE ADD/MODIFY/DROP COLUMN on the source is captured from the
#   binlog, forwarded through the engine, and applied to the sink table
#   (ALTER TABLE) before rows with the new shape arrive.
#
# Prerequisites:
#   docker compose up -d mysql
#   cargo build --release --bin seatunnel-engine-server --bin seatunnel
#
# Usage: ./scripts/e2e-schema-evolution.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=seatunnel-rs-mysql-1
STATE_DIR=$(mktemp -d /tmp/st-e2e-schev-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-schev-run.XXXXXX)

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

# Poll a SQL query until its output matches the expected value (or timeout).
wait_for_sql() {
  local expected="$1" sql="$2"
  for _ in $(seq 1 30); do
    local got
    got=$(mysql_exec "$sql" | tail -1 || true)
    if [[ "${got:-}" == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

echo "== Seed source table =="
mysql_exec "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
INSERT INTO users(name, score) VALUES ('alice',90),('bob',85),('carol',77);
DROP TABLE IF EXISTS users_sink;"

echo "== Start master + worker =="
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 127.0.0.1:5800 \
  >"$RUN_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port 5800

SEATUNNEL_STATE_DIR="$STATE_DIR" \
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id e2e-schev-worker --addr 127.0.0.1:5001 \
  >"$RUN_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 3

echo "== Submit schema-evolution job =="
JOB_ID=$($BIN_DIR/seatunnel job submit -c examples/mysql-cdc-to-jdbc-schema-evolution.yaml -a 127.0.0.1:5800 | awk '{print $3}')
echo "job: $JOB_ID"

echo "== Wait for snapshot rows in sink table =="
COUNT=0
for _ in $(seq 1 30); do
  COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_sink;" | tail -1 || true)
  [[ "${COUNT:-0}" -ge 3 ]] && break
  sleep 2
done
echo "snapshot rows in sink: $COUNT"
[[ "${COUNT:-0}" -ge 3 ]] || { echo "FAIL: snapshot rows never reached sink"; exit 1; }

echo "== Schema evolution: ADD COLUMN =="
mysql_exec "USE seatunnel; ALTER TABLE users ADD COLUMN email VARCHAR(64);"
sleep 2
mysql_exec "USE seatunnel; INSERT INTO users(name, score, email) VALUES ('dave', 66, 'dave@example.com');"

# The auto-created sink is positional (f0..fN): the source `email` column
# arrives as f3 (ordinal 3) via the position-aware translation.
if ! wait_for_sql 1 "SELECT COUNT(*) FROM information_schema.columns \
  WHERE TABLE_SCHEMA='seatunnel' AND TABLE_NAME='users_sink' AND COLUMN_NAME='f3';"; then
  echo "FAIL: ADD COLUMN not propagated to sink"; exit 1
fi
echo "sink f3 column present: 1"

if ! wait_for_sql "dave@example.com" \
  "SELECT f3 FROM seatunnel.users_sink WHERE f0=(SELECT id FROM seatunnel.users WHERE name='dave');"; then
  echo "FAIL: row with new column not in sink"; exit 1
fi
echo "dave row in sink (f3): dave@example.com"

echo "== Schema evolution: MODIFY COLUMN =="
mysql_exec "USE seatunnel; ALTER TABLE users MODIFY COLUMN score BIGINT;"

if ! wait_for_sql "bigint" "SELECT COLUMN_TYPE FROM information_schema.columns \
  WHERE TABLE_SCHEMA='seatunnel' AND TABLE_NAME='users_sink' AND COLUMN_NAME='f2';" ; then
  echo "FAIL: MODIFY COLUMN not propagated to sink"; exit 1
fi
echo "sink f2 type after modify: bigint"

mysql_exec "USE seatunnel; UPDATE users SET score = 99999999999 WHERE name = 'alice';"
if ! wait_for_sql "99999999999" "SELECT f2 FROM seatunnel.users_sink WHERE f1='alice';"; then
  echo "FAIL: wide value not written after MODIFY"; exit 1
fi
echo "alice wide score in sink: 99999999999"

echo "== Schema evolution: DROP COLUMN =="
mysql_exec "USE seatunnel; ALTER TABLE users DROP COLUMN email;"
if ! wait_for_sql 0 "SELECT COUNT(*) FROM information_schema.columns \
  WHERE TABLE_SCHEMA='seatunnel' AND TABLE_NAME='users_sink' AND COLUMN_NAME='f3';"; then
  echo "FAIL: DROP COLUMN not propagated to sink"; exit 1
fi
echo "sink f3 column remaining: 0"

mysql_exec "USE seatunnel; INSERT INTO users(name, score) VALUES ('erin', 42);"
if ! wait_for_sql 1 "SELECT COUNT(*) FROM seatunnel.users_sink WHERE f1='erin';"; then
  echo "FAIL: rows not flowing after DROP COLUMN"; exit 1
fi
echo "erin rows after drop: 1"

echo "== Cancel job =="
$BIN_DIR/seatunnel job cancel --job-id "$JOB_ID" -a 127.0.0.1:5800

echo
echo "PASS: schema evolution closed loop verified (add/modify/drop column)."
echo "  logs: $RUN_DIR"
