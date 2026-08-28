#!/usr/bin/env bash
# Single-node web console demo: master + worker + seatunnel-web plus the
# streaming demo job, started by one command.
#
# Usage:
#   ./scripts/web-demo.sh                 # build + run, Ctrl+C stops all
#   SEATUNNEL_WEB_PASSWORD=secret ./scripts/web-demo.sh
#
# Console: http://127.0.0.1:8080 (admin / admin unless overridden).
# Ports can be changed via MASTER_ADDR / WORKER_ADDR / WEB_LISTEN.
set -euo pipefail
cd "$(dirname "$0")/.."

MASTER_ADDR=${MASTER_ADDR:-127.0.0.1:5800}
WORKER_ADDR=${WORKER_ADDR:-127.0.0.1:5801}
WEB_LISTEN=${WEB_LISTEN:-127.0.0.1:8080}
WEB_USER=${SEATUNNEL_WEB_USER:-admin}
WEB_PASSWORD=${SEATUNNEL_WEB_PASSWORD:-admin}

command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "error: python3 is required" >&2; exit 1; }

# Refuse to run on top of an older demo instance.
for addr in "$MASTER_ADDR" "$WORKER_ADDR" "$WEB_LISTEN"; do
  port=${addr##*:}
  if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "error: port $port already in use — stop the old instance or set MASTER_ADDR/WORKER_ADDR/WEB_LISTEN" >&2
    exit 1
  fi
done

echo "==> building master / worker / web console (debug)"
cargo build -p seatunnel-engine-server -p seatunnel-web

STATE_DIR=$(mktemp -d /tmp/seatunnel-web-demo.XXXXXX)
COOKIE=$(mktemp)
PIDS=()
cleanup() {
  echo
  echo "==> stopping demo and cleaning $STATE_DIR"
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$STATE_DIR" "$COOKIE"
}
trap cleanup EXIT

BIN=./target/debug

echo "==> starting master on $MASTER_ADDR"
"$BIN/seatunnel-engine-server" --role master --addr "$MASTER_ADDR" \
  --state-dir "$STATE_DIR/master" >"$STATE_DIR/master.log" 2>&1 &
PIDS+=($!)

echo "==> starting worker on $WORKER_ADDR (-> $MASTER_ADDR)"
"$BIN/seatunnel-engine-server" --role worker --addr "$WORKER_ADDR" --master "$MASTER_ADDR" \
  --worker-id demo-worker-1 --state-dir "$STATE_DIR/worker" >"$STATE_DIR/worker.log" 2>&1 &
PIDS+=($!)

echo "==> starting web console on $WEB_LISTEN (user: $WEB_USER)"
"$BIN/seatunnel-web" --master "$MASTER_ADDR" --listen "$WEB_LISTEN" \
  --auth-user "$WEB_USER" --auth-password "$WEB_PASSWORD" >"$STATE_DIR/web.log" 2>&1 &
PIDS+=($!)

web="http://$WEB_LISTEN"
for _ in $(seq 1 30); do
  curl -sf "$web/api/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "$web/api/v1/health" >/dev/null || {
  echo "error: web console did not become healthy — tail of $STATE_DIR/web.log:" >&2
  tail -20 "$STATE_DIR/web.log" >&2
  exit 1
}

curl -sf -X POST "$web/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$WEB_USER\",\"password\":\"$WEB_PASSWORD\"}" -c "$COOKIE" >/dev/null

# Wait until the worker is registered — submitting earlier is rejected
# with "no worker registered".
for _ in $(seq 1 30); do
  workers=$(curl -sf -b "$COOKIE" "$web/api/v1/cluster" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["available_workers"])' || echo 0)
  [[ "$workers" -ge 1 ]] && break
  sleep 1
done
if [[ "${workers:-0}" -lt 1 ]]; then
  echo "error: worker did not register — tail of $STATE_DIR/worker.log:" >&2
  tail -20 "$STATE_DIR/worker.log" >&2
  exit 1
fi

# Submit the streaming demo (unbounded FakeSource, ~100 rec/s).
config_json=$(python3 -c 'import json; print(json.dumps(open("examples/web-streaming-demo.yaml").read()))')
JOB_ID=$(curl -sf -b "$COOKIE" -X POST "$web/api/v1/jobs" -H 'content-type: application/json' \
  -d "{\"job_name\":\"streaming-demo\",\"config_text\":$config_json}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')

echo
echo "=============================================================="
echo "  Web console : $web   (login: $WEB_USER / $WEB_PASSWORD)"
echo "  Demo job    : $JOB_ID"
echo "  Open Jobs -> the job id to watch throughput, idle time and"
echo "  live logs (auto-refresh every 5s)."
echo "  Ctrl+C stops everything and removes demo state."
echo "=============================================================="
wait
