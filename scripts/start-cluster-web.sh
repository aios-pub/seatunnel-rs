#!/usr/bin/env bash
# N-node hybrid pseudo-cluster (default 3 voters) with the embedded web
# console on every node, running in the background under nohup: each
# process = Raft voter (coordinator candidate) + worker executor + web
# UI/REST API (--web). Leader failover takes ~2s; a dead node's tasks are
# re-claimed by the survivors.
#
# Usage:
#   ./scripts/start-cluster-web.sh              # build (debug) + start
#   ./scripts/start-cluster-web.sh start|stop|status|restart
#
# Packaged mode: when this script sits NEXT TO the binaries (the crate's
# build.rs copies it into target/<profile>), it skips build and runs them
# via ./ relative paths — run it from the package directory.
#
# Env:
#   NODE_PORTS   comma-separated voter ports (default 5800,5810,5820);
#                any odd count >= 3 works — two voters can never reach
#                majority, the engine rejects them at startup
#   WEB_LISTEN   web console base address (default 0.0.0.0:8080); node i
#                binds base port + (i-1) — 8080, 8081, 8082 by default
#   WEB_USER     console login username    (default admin)
#   WEB_PASSWORD console login password    (default "admin" + warning;
#                exported so it never shows up in `ps`)
#   STATE_BASE   durable state base        (default .seatunnel-state/cluster-web)
#   BIN_DIR      binary directory          (default ./target/debug)
#   NO_BUILD     set to 1 to skip cargo build
#
# Per-node state/logs/pid live in $STATE_BASE/node-$i; state is KEPT across
# stop/start so jobs resume from their latest checkpoint. Cross-node
# checkpoint restore uses storage type "master" so a task re-claimed by
# another node after a kill resumes instead of restarting (no MinIO
# required; switch to storage.type s3 for large production checkpoints).
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PACKAGE_MODE=0
if [[ -x "$SCRIPT_DIR/seatunnel-engine-server" ]]; then
  PACKAGE_MODE=1
  BIN_DIR=${BIN_DIR:-.}
  NO_BUILD=1
else
  cd "$SCRIPT_DIR/.."
  BIN_DIR=${BIN_DIR:-./target/debug}
fi
ACTION=${1:-start}

case "$ACTION" in
  start|restart|stop|status) ;;
  *)
    echo "usage: $0 [start|stop|status|restart]" >&2
    exit 1
    ;;
esac

IFS=',' read -r -a PORTS <<< "${NODE_PORTS:-5800,5810,5820}"
N=${#PORTS[@]}
if [ "$N" -lt 3 ] || [ $((N % 2)) -eq 0 ]; then
  echo "error: NODE_PORTS must list an odd count >= 3 of voters (got $N)" >&2
  exit 1
fi
STATE_BASE=${STATE_BASE:-.seatunnel-state/cluster-web}
WEB_LISTEN=${WEB_LISTEN:-0.0.0.0:8080}
WEB_HOST=${WEB_LISTEN%%:*}
WEB_PORT=${WEB_LISTEN##*:}
WEB_USER=${WEB_USER:-${SEATUNNEL_WEB_USER:-admin}}
WEB_PASSWORD=${WEB_PASSWORD:-${SEATUNNEL_WEB_PASSWORD:-}}

command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }

# Console of node i listens on WEB_PORT + (i-1); probe via 127.0.0.1 when
# the bind address is a wildcard.
HEALTH_HOST=$WEB_HOST
if [[ "$HEALTH_HOST" == "0.0.0.0" || "$HEALTH_HOST" == "::" ]]; then
  HEALTH_HOST=127.0.0.1
fi
console_port() { echo $((WEB_PORT + $1 - 1)); }
console_healthy() {
  curl -sf "http://${HEALTH_HOST}:$(console_port "$1")/api/v1/health" 2>/dev/null \
    | grep -q '"status":"ok"'
}

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

node_pid() {
  local f="$STATE_BASE/node-$1/node.pid"
  [[ -f "$f" ]] || return 1
  local pid
  pid=$(cat "$f" 2>/dev/null) || return 1
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

do_stop() {
  local i pid launched=0
  for i in $(seq 1 "$N"); do
    if pid=$(node_pid "$i"); then
      echo "==> stopping node $i (pid $pid, SIGINT)"
      kill -INT "$pid" 2>/dev/null || true
      launched=1
    else
      rm -f "$STATE_BASE/node-$i/node.pid" 2>/dev/null || true
    fi
  done
  if [[ "$launched" != "1" ]]; then
    echo "==> not running"
    return 0
  fi
  for _ in $(seq 1 15); do
    local alive=0
    for i in $(seq 1 "$N"); do
      node_pid "$i" >/dev/null && alive=1
    done
    [[ "$alive" == "0" ]] && break
    sleep 1
  done
  for i in $(seq 1 "$N"); do
    if pid=$(node_pid "$i"); then
      echo "==> node $i still alive after 15s — sending SIGTERM"
      kill -TERM "$pid" 2>/dev/null || true
    fi
    rm -f "$STATE_BASE/node-$i/node.pid" 2>/dev/null || true
  done
  echo "==> stopped (state kept in $STATE_BASE)"
}

do_start() {
  local i pid port
  for i in $(seq 1 "$N"); do
    if pid=$(node_pid "$i"); then
      echo "error: node $i already running (pid $pid) — use '$0 stop' first" >&2
      exit 1
    fi
  done
  rm -f "$STATE_BASE"/node-*/node.pid 2>/dev/null || true

  for port in "${PORTS[@]}"; do
    if port_open "$port"; then
      echo "error: engine port $port already in use — set NODE_PORTS" >&2
      exit 1
    fi
  done
  for i in $(seq 1 "$N"); do
    if port_open "$(console_port "$i")"; then
      echo "error: console port $(console_port "$i") already in use — set WEB_LISTEN" >&2
      exit 1
    fi
  done

  if [[ "${NO_BUILD:-0}" != "1" ]]; then
    echo "==> building engine server (debug; NO_BUILD=1 + BIN_DIR to override)"
    cargo build -p seatunnel-engine-server
  fi
  if [[ ! -x "$BIN_DIR/seatunnel-engine-server" ]]; then
    if [[ "$PACKAGE_MODE" == "1" ]]; then
      echo "error: ./seatunnel-engine-server not found — run the script from the package directory" >&2
    else
      echo "error: $BIN_DIR/seatunnel-engine-server not found — build first or set BIN_DIR" >&2
    fi
    exit 1
  fi

  if [[ -z "$WEB_PASSWORD" ]]; then
    WEB_PASSWORD=admin
    echo "==> warning: WEB_PASSWORD unset — using the default 'admin' (set SEATUNNEL_WEB_PASSWORD in production)"
  fi

  mkdir -p "$STATE_BASE"

  # Shared engine config: the ordered member-list IS the Raft voter set
  # (voter id = position in the list). Fast liveness + "master" checkpoint
  # storage so a killed node's tasks resume on the survivors.
  local MEMBERS=""
  for port in "${PORTS[@]}"; do
    MEMBERS+="${MEMBERS:+, }\"127.0.0.1:$port\""
  done
  cat > "$STATE_BASE/engine.yaml" <<EOF
# Generated by $0 — shared by all local nodes.
hazelcast:
  cluster-name: seatunnel-hybrid-cluster
  network:
    join:
      tcp-ip:
        enabled: true
        member-list: [$MEMBERS]
seatunnel:
  engine:
    # Faster liveness than the 30s/60s defaults so a killed node's tasks
    # are re-claimed within seconds; restore 30s/60s for production.
    worker-soft-timeout-ms: 5000
    worker-timeout-ms: 10000
    checkpoint:
      storage:
        type: master
EOF

  export SEATUNNEL_WEB_PASSWORD="$WEB_PASSWORD"
  local pids=()
  for i in $(seq 1 "$N"); do
    port=${PORTS[$((i - 1))]}
    local NODE_DIR="$STATE_BASE/node-$i"
    mkdir -p "$NODE_DIR"
    echo "==> starting node $i: engine 127.0.0.1:$port, console :$(console_port "$i") (log: $NODE_DIR/server.log)"
    nohup env RUST_LOG=${RUST_LOG:-info} "$BIN_DIR/seatunnel-engine-server" \
      --role hybrid --addr "127.0.0.1:$port" --advertise-addr "127.0.0.1:$port" \
      --worker-id "node-$i" --state-dir "$NODE_DIR" \
      --config "$STATE_BASE/engine.yaml" \
      --web --web-listen "${WEB_HOST}:$(console_port "$i")" \
      --web-auth-user "$WEB_USER" \
      >>"$NODE_DIR/server.log" 2>&1 &
    echo $! >"$NODE_DIR/node.pid"
    pids+=($!)
    sleep 1
  done

  local ready=0
  for _ in $(seq 1 30); do
    ready=1
    for i in $(seq 1 "$N"); do
      console_healthy "$i" || ready=0
    done
    [[ "$ready" == "1" ]] && break
    sleep 1
  done
  if [[ "$ready" != "1" ]]; then
    echo "error: cluster did not become healthy — node log tails:" >&2
    for i in $(seq 1 "$N"); do
      echo "--- $STATE_BASE/node-$i/server.log (tail) ---" >&2
      tail -10 "$STATE_BASE/node-$i/server.log" >&2
    done
    for i in $(seq 1 "$N"); do
      if pid=$(node_pid "$i"); then kill -INT "$pid" 2>/dev/null || true; fi
      rm -f "$STATE_BASE/node-$i/node.pid" 2>/dev/null || true
    done
    exit 1
  fi

  echo
  echo "=================================================================="
  echo "  $N-node hybrid cluster + web consoles is up (background)"
  echo "  Login: $WEB_USER (password via WEB_PASSWORD)"
  for i in $(seq 1 "$N"); do
    echo "  node-$i  engine 127.0.0.1:${PORTS[$((i - 1))]}  console http://${HEALTH_HOST}:$(console_port "$i")  pid $(cat "$STATE_BASE/node-$i/node.pid")"
  done
  echo "  Logs   : $STATE_BASE/node-*/server.log"
  echo "  Stop   : $0 stop"
  echo "  Try failover: kill -9 one node pid — a new leader is elected in"
  echo "  ~2s and the consoles keep working off the survivors."
  echo "=================================================================="
}

do_status() {
  local i pid
  for i in $(seq 1 "$N"); do
    if pid=$(node_pid "$i"); then
      if console_healthy "$i"; then
        echo "node-$i: running (pid $pid)  engine 127.0.0.1:${PORTS[$((i - 1))]}  console http://${HEALTH_HOST}:$(console_port "$i")  health ok"
      else
        echo "node-$i: running (pid $pid)  engine 127.0.0.1:${PORTS[$((i - 1))]}  health NOT responding (log: $STATE_BASE/node-$i/server.log)"
      fi
    else
      echo "node-$i: stopped"
    fi
  done
}

case "$ACTION" in
  start) do_start ;;
  stop) do_stop ;;
  status) do_status ;;
  restart) do_stop; do_start ;;
esac
