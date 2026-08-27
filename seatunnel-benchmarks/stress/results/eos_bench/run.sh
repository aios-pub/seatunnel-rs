#!/bin/zsh
# EOS overhead micro-benchmark: CDC -> Kafka, transactional vs non-transactional,
# checkpoints ON in both runs (3s interval).
set -e
DB=eos_bench; TOPIC_BASE=eos-bench
mysql() { docker exec seatunnel-rs-mysql-1 mysql -uroot -proot -N -B -e "$1" 2>/dev/null; }

prepare() {
  mysql "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;
         CREATE TABLE $DB.t000 (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, ts_ms BIGINT, seq BIGINT, payload VARCHAR(96));"
  docker exec seatunnel-rs-kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic "$1" >/dev/null 2>&1 || true
  sleep 0.5
  docker exec seatunnel-rs-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic "$1" --partitions 1 --replication-factor 1 >/dev/null
}

run_case() {
  local LABEL=$1 TXN=$2 TOPIC=$3 OUT=/tmp/eos-bench-$1
  rm -rf "$OUT"; mkdir -p "$OUT"
  prepare "$TOPIC"
  local SINK_EXTRA=""
  if [ "$TXN" = "txn" ]; then SINK_EXTRA="semantics: exactly-once"; fi
  cat > "$OUT/job.yaml" <<YML
env:
  job:
    name: eos-$LABEL
  parallelism: 1
  checkpoint:
    interval: 3000
pipelines:
  - name: p0
    source:
      MySQL-CDC:
        url: jdbc:mysql://127.0.0.1:13306/$DB
        username: root
        password: root
        database-names: $DB
        table-pattern: ".*"
        startup.mode: initial
        server-id: 6500
    sinks:
      - Kafka:
          bootstrap.servers: 127.0.0.1:9092
          topic: $TOPIC
          format: json
          $SINK_EXTRA
YML
  RUST_LOG=warn ./target/debug/seatunnel run -c "$OUT/job.yaml" -m local --job-id eos-$LABEL --state-dir "$OUT/state" > "$OUT/engine.log" 2>&1 &
  local PID=$!
  sleep 5   # snapshot drain of the 500 seed rows
  ./target/debug/stress_gen --url mysql://root:root@127.0.0.1:13306 --databases $DB --tables 1 --rate 1000 --duration-sec 20 --workers 4 --batch 5 --out "$OUT/gen.json" > "$OUT/gen.log" 2>&1
  sleep 8   # drain
  kill -TERM $PID; sleep 4
  ./target/debug/stress_probe --bootstrap 127.0.0.1:9092 --topics "$TOPIC" --max-runtime-sec 15 --idle-exit-sec 5 --out "$OUT/probe.json" > "$OUT/probe.log" 2>&1
  python3 - "$OUT/probe.json" "$TOPIC" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
t=d["per_topic"][sys.argv[2]]
print(f"  load_delivered={t['load_count']} snapshot={t['snapshot_count']} p50={t['p50_ms']}ms p99={t['p99_ms']}ms max={t['max_ms']}ms")
PY
}

echo "== baseline: checkpoint ON, transactions OFF"
run_case plain notxn ${TOPIC_BASE}-plain
echo "== exactly-once: checkpoint ON + Kafka transactions"
run_case txn txn ${TOPIC_BASE}-txn
