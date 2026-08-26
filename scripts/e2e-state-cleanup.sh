#!/usr/bin/env bash
# End-to-end verification: automatic state cleanup (seatunnel.yaml).
#
# 1. A bounded job (JDBC → Console) finishes; its state dir is swept by
#    the TTL sweeper (short interval + short TTL via a test config).
# 2. A cancelled CDC job's state dir is removed after the grace window.
#
# Prerequisites:
#   docker compose up -d mysql
#   cargo build --release --bin seatunnel-engine-server --bin seatunnel
#
# Usage: ./scripts/e2e-state-cleanup.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=seatunnel-rs-mysql-1
STATE_DIR=$(mktemp -d /tmp/st-e2e-clean-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-clean-run.XXXXXX)

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

echo "== Engine config with aggressive cleanup =="
cat > "$RUN_DIR/engine.yaml" <<EOF
seatunnel:
  engine:
    history-job-expire-minutes: 1
    checkpoint:
      interval: 2000
      keep-checkpoint-count: 2
      storage:
        type: localfile
        namespace: $STATE_DIR
        auto-clean: true
        clean-grace-minutes: 1
        clean-interval-minutes: 1
EOF

echo "== Seed table =="
mysql_exec "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
INSERT INTO users(name, score) VALUES ('alice', 90);"

echo "== Start master + worker with --config =="
RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 127.0.0.1:5800 \
  >"$RUN_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port 5800

$BIN_DIR/seatunnel-engine-server --role worker \
  --master 127.0.0.1:5800 --worker-id e2e-clean-worker --addr 127.0.0.1:5001 \
  --config "$RUN_DIR/engine.yaml" \
  >"$RUN_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 3

grep -q "Engine config" "$RUN_DIR/worker.log" || { echo "FAIL: worker did not log engine config"; exit 1; }
grep -q "auto-clean=true" "$RUN_DIR/worker.log" || { echo "FAIL: auto-clean not active"; exit 1; }

echo "== Bounded job runs and checkpoints =="
cat > "$RUN_DIR/job.yaml" <<'EOF'
env:
  job.name: bounded-cleanup-test
  parallelism: 1
  checkpoint.interval: 2000
source:
  JDBC:
    url: "jdbc:mysql://127.0.0.1:13306/seatunnel"
    username: root
    password: root
    table: users
sink:
  Console: {}
EOF
JOB_ID=$($BIN_DIR/seatunnel job submit -c "$RUN_DIR/job.yaml" -a 127.0.0.1:5800 | awk '{print $3}')
echo "job: $JOB_ID"

# Wait for checkpoint files to appear.
for _ in $(seq 1 20); do
  [[ -d "$STATE_DIR/$JOB_ID" ]] && find "$STATE_DIR/$JOB_ID" -name 'cp-*.state' | grep -q . && break
  sleep 1
done
CP_COUNT=$(find "$STATE_DIR/$JOB_ID" -name 'cp-*.state' 2>/dev/null | wc -l | tr -d ' ')
echo "checkpoint files: $CP_COUNT"
[[ "$CP_COUNT" -ge 1 ]] || { echo "FAIL: no checkpoints written"; exit 1; }

echo "== Retention: keep-checkpoint-count = 2 bounds the files =="
sleep 8
CP_COUNT=$(find "$STATE_DIR/$JOB_ID" -name 'cp-*.state' 2>/dev/null | wc -l | tr -d ' ')
echo "checkpoint files after more checkpoints: $CP_COUNT"
[[ "$CP_COUNT" -le 2 ]] || { echo "FAIL: retention not honored"; exit 1; }

echo "== Terminal job → state removed (cancel-grace or TTL sweep) =="
# The bounded job may already be terminal; cancel is best-effort.
$BIN_DIR/seatunnel job cancel --job-id "$JOB_ID" -a 127.0.0.1:5800 >/dev/null 2>&1 || true
for _ in $(seq 1 120); do
  [[ ! -d "$STATE_DIR/$JOB_ID" ]] && break
  sleep 2
done
[[ ! -d "$STATE_DIR/$JOB_ID" ]] && echo "state dir removed" || {
  echo "FAIL: terminal job state not removed"; exit 1; }

echo "== Cancelled CDC job gets grace deletion =="
cat > "$RUN_DIR/cdc.yaml" <<'EOF'
env:
  job.name: cdc-cleanup-test
  parallelism: 1
  checkpoint.interval: 2000
source:
  MySQL-CDC:
    hostname: "127.0.0.1"
    port: 13306
    username: root
    password: root
    database-name: seatunnel
    table-name: users
    startup.mode: latest
sink:
  Console: {}
EOF
CDC_JOB=$($BIN_DIR/seatunnel job submit -c "$RUN_DIR/cdc.yaml" -a 127.0.0.1:5800 | awk '{print $3}')
sleep 6
[[ -d "$STATE_DIR/$CDC_JOB" ]] || { echo "FAIL: CDC job wrote no state"; exit 1; }
$BIN_DIR/seatunnel job cancel --job-id "$CDC_JOB" -a 127.0.0.1:5800 >/dev/null
for _ in $(seq 1 90); do
  [[ ! -d "$STATE_DIR/$CDC_JOB" ]] && break
  sleep 2
done
[[ ! -d "$STATE_DIR/$CDC_JOB" ]] && echo "CDC state dir removed after grace" || {
  echo "FAIL: cancelled CDC job state not removed"; exit 1; }

echo
echo "PASS: state auto-cleanup verified (retention + terminal/cancel deletion)."
echo "  state dir: $STATE_DIR"
echo "  logs:      $RUN_DIR"
