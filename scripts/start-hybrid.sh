#!/usr/bin/env bash
# Single-node hybrid engine: one process = Raft coordinator (single voter)
# + embedded worker executor. The recommended local / single-machine form
# (Java MASTER_AND_WORKER parity).
#
# Usage:
#   ./scripts/start-hybrid.sh                       # build (debug) + run
#   NO_BUILD=1 BIN_DIR=./target/release ./scripts/start-hybrid.sh
#
# Ctrl+C stops the node; the state dir is KEPT so resubmitted jobs resume
# from their latest checkpoint.
#
# Env:
#   HYBRID_ADDR  bind address               (default 127.0.0.1:5800)
#   STATE_DIR    durable state directory    (default .seatunnel-state/hybrid)
#   BIN_DIR      binary directory           (default ./target/debug)
#   NO_BUILD     set to 1 to skip cargo build
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR=${HYBRID_ADDR:-127.0.0.1:5800}
STATE_DIR=${STATE_DIR:-.seatunnel-state/hybrid}
BIN_DIR=${BIN_DIR:-./target/debug}

port_open() { (exec 3<>"/dev/tcp/${1%:*}/${1##*:}") 2>/dev/null; }

if port_open "$ADDR"; then
  echo "error: $ADDR already in use — stop the old instance or set HYBRID_ADDR" >&2
  exit 1
fi

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "==> building engine server + CLI (debug; NO_BUILD=1 + BIN_DIR to override)"
  cargo build -p seatunnel-engine-server -p seatunnel-cli
fi
for bin in seatunnel-engine-server seatunnel; do
  [[ -x "$BIN_DIR/$bin" ]] || { echo "error: $BIN_DIR/$bin not found — build first or set BIN_DIR" >&2; exit 1; }
done

mkdir -p "$STATE_DIR"
PIDS=()
cleanup() {
  echo
  echo "==> stopping hybrid node (state kept in $STATE_DIR)"
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

echo "==> starting hybrid node on $ADDR (log: $STATE_DIR/server.log)"
RUST_LOG=${RUST_LOG:-info} "$BIN_DIR/seatunnel-engine-server" --role hybrid \
  --addr "$ADDR" --state-dir "$STATE_DIR" >"$STATE_DIR/server.log" 2>&1 &
PIDS+=($!)

for _ in $(seq 1 30); do port_open "$ADDR" && break; sleep 1; done
port_open "$ADDR" || {
  echo "error: node did not come up — tail of $STATE_DIR/server.log:" >&2
  tail -20 "$STATE_DIR/server.log" >&2
  exit 1
}

# Wait until the single voter has elected itself and serves cluster info.
CLUSTER=""
for _ in $(seq 1 15); do
  CLUSTER=$("$BIN_DIR/seatunnel" cluster -a "$ADDR" 2>/dev/null || true)
  if [ -n "$CLUSTER" ] && echo "$CLUSTER" | grep -q "Cluster leader" \
     && ! echo "$CLUSTER" | grep -q "Cluster leader   : -"; then
    break
  fi
  sleep 1
done
[ -n "$CLUSTER" ] || {
  echo "error: cluster info unreachable — tail of $STATE_DIR/server.log:" >&2
  tail -20 "$STATE_DIR/server.log" >&2
  exit 1
}

echo
echo "=================================================================="
echo "  Hybrid node is up — one process = coordinator + worker"
echo "$CLUSTER" | sed -n '1,2p' | sed 's/^/  /'
echo "  Log            : $STATE_DIR/server.log"
echo "  Submit a job   : $BIN_DIR/seatunnel job submit -c <job.yaml> -a $ADDR"
echo "  Cluster status : $BIN_DIR/seatunnel cluster -a $ADDR"
echo "  Ctrl+C stops the node; state in $STATE_DIR is kept, so a resubmitted"
echo "  job resumes from its latest checkpoint."
echo "=================================================================="
wait
