#!/usr/bin/env bash
# End-to-end verification: MySQL CDC startup from a specified timestamp.
#
# Contract under test (startup.mode = timestamp):
#   1. NO snapshot — rows existing before the target time must NOT appear.
#   2. Transactions committed BEFORE the target time must NOT appear.
#   3. Transactions committed AFTER the target time MUST appear.
#
# Prerequisites:
#   docker compose up -d mysql
#   cargo build --release --bin seatunnel-engine-server --bin seatunnel
#
# Usage: ./scripts/e2e-mysql-cdc-timestamp.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=seatunnel-rs-mysql-1
STATE_DIR=$(mktemp -d /tmp/st-e2e-ts-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-ts-run.XXXXXX)

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

wait_for_count() {
  local expected="$1" sql="$2"
  for _ in $(seq 1 30); do
    local got
    got=$(mysql_exec "$sql" | tail -1 || true)
    if [[ "${got:-0}" == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

echo "== Seed source table (pre-target rows) =="
mysql_exec "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
INSERT INTO users(name, score) VALUES ('snapshot-should-not-appear', 1);
DROP TABLE IF EXISTS users_ts_sink;
CREATE TABLE users_ts_sink (f0 BIGINT PRIMARY KEY, f1 TEXT, f2 BIGINT);"

echo "== Pre-target transaction (must NOT be captured) =="
mysql_exec "USE seatunnel; INSERT INTO users(name, score) VALUES ('pre-target', 2);"
sleep 1
TARGET_TS=$(python3 -c 'import time; print(int(time.time()*1000))')
echo "target timestamp: $TARGET_TS ms"

echo "== Start master + worker =="
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 127.0.0.1:5800 \
  >"$RUN_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port 5800

SEATUNNEL_STATE_DIR="$STATE_DIR" \
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id e2e-ts-worker --addr 127.0.0.1:5001 \
  >"$RUN_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 3

echo "== Submit timestamp-startup job =="
cat > "$RUN_DIR/job.yaml" <<EOF
env:
  job.name: mysql-cdc-timestamp-e2e
  parallelism: 1
  checkpoint.interval: 3000
source:
  MySQL-CDC:
    hostname: "127.0.0.1"
    port: 13306
    username: root
    password: root
    database-name: seatunnel
    table-name: users
    startup.mode: timestamp
    startup.timestamp: $TARGET_TS
sink:
  JDBC:
    url: "jdbc:mysql://127.0.0.1:13306/seatunnel"
    username: root
    password: root
    table: users_ts_sink
    primary-keys: f0
    enable-upsert: true
    schema-save-mode: ignore
    data-save-mode: append_data
EOF
JOB_ID=$($BIN_DIR/seatunnel job submit -c "$RUN_DIR/job.yaml" -a 127.0.0.1:5800 | awk '{print $3}')
echo "job: $JOB_ID"
# Give the warm-up drain (full retained binlog replay) time to reach the tail.
sleep 8

echo "== Assert streaming-only semantics (no snapshot, no pre-target rows) =="
COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_ts_sink;" | tail -1 || true)
echo "sink rows before post-target insert: ${COUNT:-0}"
[[ "${COUNT:-0}" == "0" ]] || {
  echo "FAIL: expected 0 rows (streaming-only), got $COUNT"; exit 1; }

echo "== Post-target transaction (MUST be captured) =="
mysql_exec "USE seatunnel; INSERT INTO users(name, score) VALUES ('post-target', 42);"
if ! wait_for_count 1 "SELECT COUNT(*) FROM seatunnel.users_ts_sink;"; then
  echo "FAIL: post-target row never reached the sink"; exit 1
fi
ROW=$(mysql_exec "SELECT CONCAT_WS('|', f1, f2) FROM seatunnel.users_ts_sink WHERE f1='post-target';" | tail -1)
echo "post-target row in sink: $ROW"
[[ "$ROW" == "post-target|42" ]] || {
  echo "FAIL: unexpected row content: $ROW"; exit 1; }

echo "== Cancel job =="
$BIN_DIR/seatunnel job cancel --job-id "$JOB_ID" -a 127.0.0.1:5800

echo
echo "PASS: timestamp startup verified (snapshot skipped, pre-target excluded, post-target captured)."
echo "  logs: $RUN_DIR"
