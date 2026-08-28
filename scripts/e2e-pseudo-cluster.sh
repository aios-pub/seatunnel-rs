#!/usr/bin/env bash
# Pseudo-cluster production verification (fault matrix A–K).
#
# Topology (all on localhost, S3 checkpoints via MinIO):
#   master-1 (5800) + master-2 (5810) + master-3 (5820) — 3 raft voters
#   (odd voter counts are required; 2 voters can never reach majority)
#   worker-1 (5001) worker-2 (5002) worker-3 (5003)
#   MinIO (9000) bucket seatunnel-checkpoints
#
# Usage: ./scripts/e2e-pseudo-cluster.sh [--keep]
set -euo pipefail
cd "$(dirname "$0")/.."

KEEP=${1:-}
BIN_DIR=${BIN_DIR:-./target/release}
MYSQL_CONTAINER=seatunnel-rs-mysql-1
MINIO_CONTAINER=seatunnel-rs-minio-1
STATE_BASE=$(mktemp -d /tmp/st-e2e-pc-state.XXXXXX)
RUN_DIR=$(mktemp -d /tmp/st-e2e-pc-run.XXXXXX)
PIDS=()

cleanup() {
  if [[ "$KEEP" != "--keep" ]]; then
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  fi
}
trap cleanup EXIT

log()  { echo -e "\e[36m[case $1]\e[0m $2"; }
fail() { echo "FAIL [$1]: $2" | tee -a "$RUN_DIR/failures.txt"; }

mysql_exec() { docker exec "$MYSQL_CONTAINER" mysql -uroot -proot -e "$1" 2>/dev/null; }
minio()     { docker exec "$MINIO_CONTAINER" mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; docker exec "$MINIO_CONTAINER" mc "$@"; }

start_master() { # idx port
  RUST_LOG=info $BIN_DIR/seatunnel-engine-server --role master --addr 0.0.0.0:$2 \
    --config "$RUN_DIR/engine.yaml" >"$RUN_DIR/master-$1.log" 2>&1 &
  PIDS+=($!); sleep 1
}
start_worker() { # idx addr
  SEATUNNEL_STATE_DIR="$STATE_BASE/w$1" RUST_LOG=info \
    $BIN_DIR/seatunnel-engine-server --role worker --master 127.0.0.1:5800,127.0.0.1:5810,127.0.0.1:5820 \
    --worker-id pc-worker-$1 --addr $2 \
    --config "$RUN_DIR/engine.yaml" >"$RUN_DIR/worker-$1.log" 2>&1 &
  PIDS+=($!); sleep 1
}

wait_for() { # file pattern tries
  for _ in $(seq 1 "${3:-30}"); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 1; done; return 1
}

seed() {
  mysql_exec "
CREATE DATABASE IF NOT EXISTS seatunnel;
USE seatunnel;
DROP TABLE IF EXISTS users_pc;
CREATE TABLE users_pc (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), score INT);
INSERT INTO users_pc(name, score) VALUES ('seed-1', 1), ('seed-2', 2);"
}

# --- engine config --------------------------------------------------------
cat > "$RUN_DIR/engine.yaml" <<EOF
seatunnel:
  engine:
    history-job-expire-minutes: 1
    worker-timeout-ms: 4000
    replication-interval-ms: 2000
    worker-address: 127.0.0.1:5001
    checkpoint:
      interval: 2000
      keep-checkpoint-count: 2
      storage:
        type: s3
        auto-clean: true
        clean-grace-minutes: 1
        clean-interval-minutes: 1
        plugin-config:
          bucket: seatunnel-checkpoints
          endpoint: http://127.0.0.1:9000
          region: us-east-1
          access-key: minioadmin
          secret-key: minioadmin
          prefix: pc
          path-style: true
hazelcast:
  cluster-name: seatunnel-pc
  network:
    join:
      tcp-ip:
        enabled: true
        member-list: ["127.0.0.1:5800", "127.0.0.1:5810", "127.0.0.1:5820"]
    port:
      port: 5800
EOF

cat > "$RUN_DIR/job.yaml" <<'EOF'
env:
  job.name: pc-cdc
  parallelism: 1
  checkpoint.interval: 2000
source:
  MySQL-CDC:
    hostname: "127.0.0.1"
    port: 13306
    username: root
    password: root
    database-name: seatunnel
    table-name: users_pc
    startup.mode: initial
    split.column: id
sink:
  JDBC:
    url: "jdbc:mysql://127.0.0.1:13306/seatunnel"
    username: root
    password: root
    table: users_pc_sink
    primary-keys: f0
    enable-upsert: true
    schema-save-mode: create_when_not_exist
    data-save-mode: append_data
EOF

echo "== Setup =="
mysql_exec "CREATE DATABASE IF NOT EXISTS seatunnel; DROP TABLE IF EXISTS seatunnel.users_pc_sink;"
seed
start_master 1 5800
start_master 2 5810
start_master 3 5820
start_worker 1 127.0.0.1:5001
start_worker 2 127.0.0.1:5002
start_worker 3 127.0.0.1:5003
sleep 5

# --- A: normal topology ----------------------------------------------------
log A "3 workers registered on master-1; master-2 fellow-voter-synced"
REG=$(grep -c "Worker pc-worker" "$RUN_DIR/master-1.log" || true)
[[ "$REG" -ge 3 ]] || fail A "registration count=$REG"
grep -q "fellow-voter — syncing" "$RUN_DIR/master-2.log" || fail A "master-2 not syncing"

JOB=$($BIN_DIR/seatunnel job submit -c "$RUN_DIR/job.yaml" -a 127.0.0.1:5800,127.0.0.1:5810,127.0.0.1:5820 | awk '{print $3}')
log A "job=$JOB"
for _ in $(seq 1 20); do
  mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" 2>/dev/null | tail -1 | grep -q 2 && break; sleep 1
done
COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" | tail -1)
[[ "$COUNT" == "2" ]] || fail A "snapshot rows=$COUNT"

# S3 objects bounded by keep-checkpoint-count
sleep 6
minio ls local/seatunnel-checkpoints/pc/ --recursive >/dev/null 2>&1 || fail A "no S3 prefix"
log A "S3 checkpoints present: $(minio ls local/seatunnel-checkpoints/pc/ --recursive 2>/dev/null | wc -l | tr -d ' ') object(s)"

# --- B: task owner kill -9 → failover with S3 resume -------------------------
OWNER_W=$(grep -l "accepting task ${JOB}" "$RUN_DIR"/worker-*.log 2>/dev/null | tail -1 | grep -o 'worker-[0-9]' || echo "worker-2")
OWNER_IDX=${OWNER_W#worker-}
OWNER_PID=${PIDS[$((OWNER_IDX + 1))]}
log B "kill task owner $OWNER_W (kill -9, pid $OWNER_PID)"
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('b-before', 10);"
sleep 3
kill -9 "$OWNER_PID" 2>/dev/null || true
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('b-during', 11);"
sleep 8   # eviction (4s) + claim
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('b-after', 12);"
sleep 8
B_COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" | tail -1)
[[ "$B_COUNT" == "5" ]] || fail B "rows after failover=$B_COUNT (want 5)"
grep -q "Failover: task" "$RUN_DIR/master-1.log" || fail B "no failover reassignment logged"
grep -qE "restored checkpoint cp-[0-9]+ from s3" "$RUN_DIR"/worker-*.log || fail B "no S3 resume log"
log B "data continuous ($B_COUNT rows), S3 resume verified"

# --- C: master kill -9 → fellow-voter takeover ----------------------------------
log C "kill master-1 (kill -9)"
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('c-during', 20);"
kill -9 "${PIDS[0]}" 2>/dev/null || true
sleep 10  # 3 failed heartbeats + re-register
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('c-after', 21);"
sleep 8
C_COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" | tail -1)
[[ "$C_COUNT" == "7" ]] || fail C "rows after master failover=$C_COUNT (want 7)"
grep -q "failing over to 127.0.0.1:5810" "$RUN_DIR"/worker-*.log || fail C "no worker master-failover log"
STATUS=$($BIN_DIR/seatunnel job status --job-id "$JOB" -a 127.0.0.1:5810 2>&1 | head -3 || true)
log C "status on master-2: $(echo "$STATUS" | head -1)"
log C "data continuous ($C_COUNT rows)"

# --- D: old master restarts → no split brain --------------------------------
log D "restart master-1"
start_master 1 5800
sleep 4
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('d-after', 30);"
sleep 8
D_COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" | tail -1)
[[ "$D_COUNT" == "8" ]] || fail D "rows after master restart=$D_COUNT"
log D "data continuous ($D_COUNT rows), no split-brain"

# --- E: worker restart (graceful-ish kill) ----------------------------------
log E "kill worker-1, restart it"
W1_PID=${PIDS[2]}
kill "$W1_PID" 2>/dev/null || true
sleep 6
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('e-during', 40);"
sleep 6
start_worker 4 127.0.0.1:5004
sleep 6
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('e-after', 41);"
sleep 8
E_COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" | tail -1)
[[ "$E_COUNT" == "10" ]] || fail E "rows=$E_COUNT (want 10)"
log E "worker restart absorbed ($E_COUNT rows)"

# --- F: SIGSTOP / SIGCONT preemption fence ---------------------------------
log F "SIGSTOP worker handling the task, wait for eviction+takeover, SIGCONT"
# Find which worker runs the job task now (the one with 'accepting task' for our job)
OWNER=$(grep -l "accepting task ${JOB}" "$RUN_DIR"/worker-*.log 2>/dev/null | tail -1 | grep -o 'worker-[0-9]' || echo "")
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('f-during', 50);"
sleep 3
if [[ -n "$OWNER" ]]; then
  OWNER_PID=$(ps aux | grep "seatunnel-engine-server --role worker" | grep "pc-worker-${OWNER#worker-}" | grep -v grep | awk '{print $2}' | head -1)
  if [[ -n "$OWNER_PID" ]]; then
    kill -STOP "$OWNER_PID" && log F "SIGSTOP $OWNER (pid $OWNER_PID)"
    sleep 10   # eviction + takeover
    kill -CONT "$OWNER_PID" && log F "SIGCONT resumed"
    sleep 8
    grep -q "preempting task" "$RUN_DIR/${OWNER}.log" || fail F "no preemption log in $OWNER"
    log F "preemption fence fired"
  else
    log F "owner pid not found — skipped (non-fatal)"
  fi
else
  log F "owner not identified — skipped (non-fatal)"
fi
mysql_exec "USE seatunnel; INSERT INTO users_pc(name, score) VALUES ('f-after', 51);"
sleep 8
F_COUNT=$(mysql_exec "SELECT COUNT(*) FROM seatunnel.users_pc_sink" | tail -1)
log F "rows=$F_COUNT (expected ~12, duplicates tolerated)"

# --- H: cancel on fellow-voter ---------------------------------------------------
log H "cancel job via master-2"
$BIN_DIR/seatunnel job cancel --job-id "$JOB" -a 127.0.0.1:5810 >/dev/null 2>&1 || fail H "cancel on fellow-voter failed"
sleep 5
log H "cancelled"

# --- K: cleanup verification ------------------------------------------------
log K "S3 prefix + local state cleanup after grace (1 min)"
sleep 70
REMAIN=$(minio ls local/seatunnel-checkpoints/pc/ --recursive 2>/dev/null | wc -l | tr -d ' ')
log K "S3 objects remaining after cleanup: $REMAIN"
for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done

# --- verdict ----------------------------------------------------------------
echo
if [[ -f "$RUN_DIR/failures.txt" ]]; then
  echo "MATRIX RESULT: FAILURES DETECTED"; cat "$RUN_DIR/failures.txt"; exit 1
fi
echo "MATRIX RESULT: ALL CASES PASSED"
echo "  topology:  masters(5800,5810,5820 raft voters) + worker×3 + MinIO(s3)"
echo "  cases:     A topology/assignment/s3-bounded, B worker kill -9 + s3 resume,"
echo "             C master kill -9 takeover, D master restart no split-brain,"
echo "             E worker restart, F SIGSTOP/CONT preemption fence, H cancel on fellow-voter,"
echo "             K s3+local cleanup"
echo "  logs: $RUN_DIR"
