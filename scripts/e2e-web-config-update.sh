#!/usr/bin/env bash
# E2E: the web console's edit-and-restart must actually take effect.
#
# Starts one master + one worker + the web console (standalone binary),
# submits the streaming demo job, then:
#   1. POST /api/v1/jobs/{id}/update with an EDITED YAML (sleep.ms
#      10 -> 50), format auto-detected — no explicit format field;
#   2. asserts the response reports the cancel + same-id resubmit;
#   3. GETs the job detail and asserts job_config now carries the new
#      value — the submit-time snapshot was really replaced, so a
#      cluster restart would restore the EDITED config;
#   4. POSTs an unusable YAML (no sink section) and asserts a 400 that
#      names the missing section — rejected BEFORE the old incarnation
#      is cancelled, so the job keeps running untouched.
#
# Usage: ./scripts/e2e-web-config-update.sh
set -euo pipefail
cd "$(dirname "$0")/.."

MASTER_ADDR=${MASTER_ADDR:-127.0.0.1:5800}
WORKER_ADDR=${WORKER_ADDR:-127.0.0.1:5801}
WEB_LISTEN=${WEB_LISTEN:-127.0.0.1:8090}
WEB_USER=${SEATUNNEL_WEB_USER:-e2e}
WEB_PASSWORD=${SEATUNNEL_WEB_PASSWORD:-e2e}

command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "error: python3 is required" >&2; exit 1; }

for addr in "$MASTER_ADDR" "$WORKER_ADDR" "$WEB_LISTEN"; do
  port=${addr##*:}
  if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "error: port $port already in use — set MASTER_ADDR/WORKER_ADDR/WEB_LISTEN" >&2
    exit 1
  fi
done

STATE_DIR=$(mktemp -d /tmp/st-e2e-web-update.XXXXXX)
COOKIE=$(mktemp)
PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$STATE_DIR" "$COOKIE"
}
trap cleanup EXIT

BIN=${BIN_DIR:-./target/debug}

echo "==> building master / worker / web console (debug)"
cargo build -q -p seatunnel-engine-server -p seatunnel-web

"$BIN/seatunnel-engine-server" --role master --addr "$MASTER_ADDR" \
  --state-dir "$STATE_DIR/master" >"$STATE_DIR/master.log" 2>&1 &
PIDS+=($!)
"$BIN/seatunnel-engine-server" --role worker --addr "$WORKER_ADDR" --master "$MASTER_ADDR" \
  --worker-id e2e-update-worker --state-dir "$STATE_DIR/worker" >"$STATE_DIR/worker.log" 2>&1 &
PIDS+=($!)
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

for _ in $(seq 1 30); do
  workers=$(curl -sf -b "$COOKIE" "$web/api/v1/cluster" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["available_workers"])' || echo 0)
  [[ "$workers" -ge 1 ]] && break
  sleep 1
done
[[ "${workers:-0}" -ge 1 ]] || { echo "error: worker did not register" >&2; exit 1; }

# --- submit the demo job (unbounded FakeSource -> Console) -----------------
config_json=$(python3 -c 'import json; print(json.dumps(open("examples/web-streaming-demo.yaml").read()))')
JOB_ID=$(curl -sf -b "$COOKIE" -X POST "$web/api/v1/jobs" -H 'content-type: application/json' \
  -d "{\"job_name\":\"e2e-update\",\"config_text\":$config_json}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "==> submitted demo job $JOB_ID"

edit_yaml() { # edit_yaml <sleep-ms-value> — demo config with a new pacing
  sed "s/sleep.ms: 10/sleep.ms: $1/" examples/web-streaming-demo.yaml
}

# --- case 1: edit-and-restart with an auto-detected YAML edit --------------
echo "==> [1] update with edited YAML (sleep.ms 10 -> 50)"
edited_json=$(python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' <<<"$(edit_yaml 50)")
update_body=$(curl -sf -b "$COOKIE" -X POST "$web/api/v1/jobs/$JOB_ID/update" \
  -H 'content-type: application/json' -d "{\"config_text\":$edited_json}")
echo "$update_body" | python3 -c '
import json, sys
r = json.load(sys.stdin)
assert r["cancelled"] is True, f"expected a cancel + resubmit: {r}"
assert r["job_id"].startswith("job-"), r
print("    update reported: cancelled in", r["cancel_wait_ms"], "ms —", r["message"])'

# --- case 2: the submit-time config snapshot really changed ----------------
echo "==> [2] job detail must show the edited config"
for _ in $(seq 1 5); do
  detail=$(curl -sf -b "$COOKIE" "$web/api/v1/jobs/$JOB_ID")
  ok=$(echo "$detail" | python3 -c '
import json, sys
d = json.load(sys.stdin)
config = json.loads(d["job_config"])
sleep_ms = config["source"][0]["FakeSource"]["sleep.ms"]
print("yes" if sleep_ms == 50 else "no")' || echo no)
  [[ "$ok" == "yes" ]] && break
  sleep 1
done
[[ "$ok" == "yes" ]] || { echo "FAIL: job_config still carries the old value:" >&2; echo "$detail" >&2; exit 1; }
echo "    job_config now carries sleep.ms = 50 (snapshot replaced)"

# --- case 3: unusable config rejected BEFORE cancel ------------------------
echo "==> [3] update without a sink section must be rejected up front"
status=$(curl -s -o "$STATE_DIR/reject.json" -w '%{http_code}' -b "$COOKIE" \
  -X POST "$web/api/v1/jobs/$JOB_ID/update" -H 'content-type: application/json' \
  -d '{"config_text":"source:\n  Console: {}\n","format":"yaml"}')
[[ "$status" == "400" ]] || { echo "FAIL: expected 400, got $status:" >&2; cat "$STATE_DIR/reject.json" >&2; exit 1; }
grep -q "sink" "$STATE_DIR/reject.json" || {
  echo "FAIL: error must name the missing section:" >&2; cat "$STATE_DIR/reject.json" >&2; exit 1; }
echo "    rejected with 400: $(python3 -c 'import json;print(json.load(open("'"$STATE_DIR"'/reject.json"))["error"])')"

# --- cleanup: cancel the streaming job -------------------------------------
curl -sf -b "$COOKIE" -X POST "$web/api/v1/jobs/$JOB_ID/cancel" >/dev/null || true

echo
echo "PASS: web edit-and-restart applied the edited config (job $JOB_ID)"
