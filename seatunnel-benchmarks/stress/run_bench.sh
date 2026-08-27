#!/usr/bin/env bash
# Single-machine stress run: 2 MySQL-CDC sources (250 tables each) fan out
# to 4 Kafka sinks through the engine in local mode.
#
# Usage: run_bench.sh <label> <engine-binary> <rate> <duration-sec> [gen-batch]
#
# Phases:
#   1. fresh Kafka topics per label
#   2. engine starts (startup.mode=initial → snapshot of the 50k seed rows)
#   3. probe waits until every topic has drained its snapshot (25k msgs)
#   4. load generator runs at <rate> rows/s for <duration-sec>
#   5. probe drains, then writes the latency report JSON
set -euo pipefail

LABEL="${1:?label}"
ENGINE_BIN="${2:?engine binary path}"
RATE="${3:-2000}"
DURATION="${4:-120}"
BATCH="${5:-1}"

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${BENCH_OUT_DIR:-/tmp/seatunnel-stress}/${LABEL}"
MYSQL_URL="mysql://root:root@127.0.0.1:13306"
BOOTSTRAP="127.0.0.1:9092"
GEN_BIN="${ROOT}/../../target/release/stress_gen"
PROBE_BIN="${ROOT}/../../target/release/stress_probe"
SNAPSHOT_PER_TOPIC=$((250 * 100)) # 250 tables x 100 seed rows

mkdir -p "$OUT_DIR"
for f in "$ENGINE_BIN" "$GEN_BIN" "$PROBE_BIN"; do
  [ -x "$f" ] || { echo "missing executable: $f" >&2; exit 1; }
done

# Fresh topics for this run (avoid stale-message pollution between runs).
TOPICS=("${LABEL}_a1" "${LABEL}_a2" "${LABEL}_b1" "${LABEL}_b2")
for t in "${TOPICS[@]}"; do
  docker exec seatunnel-rs-kafka-1 kafka-topics --bootstrap-server localhost:9092 \
    --delete --topic "$t" >/dev/null 2>&1 || true
  docker exec seatunnel-rs-kafka-1 kafka-topics --bootstrap-server localhost:9092 \
    --create --topic "$t" --partitions 1 --replication-factor 1 >/dev/null
done

cat > "$OUT_DIR/job.yaml" <<EOF
env:
  job.name: stress-${LABEL}
  parallelism: 1
pipelines:
  - name: cdc-a
    source:
      MySQL-CDC:
        url: jdbc:mysql://127.0.0.1:13306/perf_a
        username: root
        password: root
        database-names: perf_a
        table-pattern: ".*"
        startup.mode: initial
        server-id: 5401
        server-timezone: "+08:00"
    sinks:
      - Kafka:
          bootstrap.servers: ${BOOTSTRAP}
          topic: ${LABEL}_a1
          format: json
      - Kafka:
          bootstrap.servers: ${BOOTSTRAP}
          topic: ${LABEL}_a2
          format: json
  - name: cdc-b
    source:
      MySQL-CDC:
        url: jdbc:mysql://127.0.0.1:13306/perf_b
        username: root
        password: root
        database-names: perf_b
        table-pattern: ".*"
        startup.mode: initial
        server-id: 5402
        server-timezone: "+08:00"
    sinks:
      - Kafka:
          bootstrap.servers: ${BOOTSTRAP}
          topic: ${LABEL}_b1
          format: json
      - Kafka:
          bootstrap.servers: ${BOOTSTRAP}
          topic: ${LABEL}_b2
          format: json
EOF

echo "== [$LABEL] starting engine: $(basename "$ENGINE_BIN") =="
RUST_LOG=info "$ENGINE_BIN" run -c "$OUT_DIR/job.yaml" -m local \
  > "$OUT_DIR/engine.log" 2>&1 &
ENGINE_PID=$!

cleanup() {
  kill "$ENGINE_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "== [$LABEL] starting probe =="
"$PROBE_BIN" --bootstrap "$BOOTSTRAP" \
  --topics "$(IFS=,; echo "${TOPICS[*]}")" \
  --tag "$LABEL" --idle-exit-sec 60 --max-runtime-sec 3000 \
  --out "$OUT_DIR/probe.json" > "$OUT_DIR/probe.log" 2>&1 &
PROBE_PID=$!

# Wait for the snapshot phase: every topic must have served its 25k seed rows
# (or gone quiet for 30s having served >=80%, in case flush boundaries leave
# a tail pending until the next write).
echo "== [$LABEL] waiting for snapshot drain ($SNAPSHOT_PER_TOPIC msgs/topic) =="
WAITED=0
LAST_COUNTS=""
QUIET_FOR=0
while true; do
  if ! kill -0 "$ENGINE_PID" 2>/dev/null; then
    echo "engine exited early; log tail:" >&2
    tail -30 "$OUT_DIR/engine.log" >&2
    exit 1
  fi
  LAST=$(grep -o 'snapshot=\[[^]]*\]' "$OUT_DIR/probe.log" | tail -1 || true)
  MIN=$(echo "$LAST" | grep -o '[0-9]\+' | sort -n | head -1 || echo 0)
  if [ "${MIN:-0}" -ge "$SNAPSHOT_PER_TOPIC" ]; then
    echo "snapshot complete after ${WAITED}s ($LAST)"
    break
  fi
  if [ "$LAST" = "$LAST_COUNTS" ]; then
    QUIET_FOR=$((QUIET_FOR + 5))
  else
    QUIET_FOR=0
    LAST_COUNTS="$LAST"
  fi
  if [ "$QUIET_FOR" -ge 30 ] && [ "${MIN:-0}" -ge $((SNAPSHOT_PER_TOPIC * 8 / 10)) ]; then
    echo "snapshot quiet after ${WAITED}s at >=80% ($LAST); continuing" >&2
    break
  fi
  if [ "$WAITED" -ge 900 ]; then
    echo "snapshot not complete after 900s ($LAST); continuing anyway" >&2
    break
  fi
  sleep 5
  WAITED=$((WAITED + 5))
done

SNAPSHOT_END=$(date +%s)
echo "== [$LABEL] load phase: rate=$RATE rows/s duration=${DURATION}s batch=$BATCH =="
"$GEN_BIN" --url "$MYSQL_URL" --databases perf_a,perf_b --tables 250 \
  --rate "$RATE" --duration-sec "$DURATION" --workers 8 --batch "$BATCH" \
  --out "$OUT_DIR/gen.json" > "$OUT_DIR/gen.log" 2>&1
GEN_END=$(date +%s)

echo "== [$LABEL] generator done in $((GEN_END - SNAPSHOT_END))s; draining =="
wait "$PROBE_PID"
kill "$ENGINE_PID" 2>/dev/null || true

sleep 2
echo "== [$LABEL] results =="
cat "$OUT_DIR/gen.json" || true
python3 - "$OUT_DIR/probe.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
print(f"probe total load msgs: {r['total_load_messages']}  snapshot: {r['snapshot_counts']}")
o = r["overall"]
print(f"overall: avg={o['avg_ms']:.1f}ms p50={o['p50_ms']} p90={o['p90_ms']} p95={o['p95_ms']} p99={o['p99_ms']} p999={o['p999_ms']} max={o['max_ms']}ms")
for t, s in r["per_topic"].items():
    print(f"  {t}: n={s['load_count']} p50={s['p50_ms']} p99={s['p99_ms']} max={s['max_ms']}ms snapshot={s['snapshot_count']}")
PY
echo "== [$LABEL] artifacts in $OUT_DIR =="
