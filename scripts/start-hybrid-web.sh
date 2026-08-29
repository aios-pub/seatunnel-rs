#!/usr/bin/env bash
# Single-node hybrid engine with the embedded web console, running in the
# background under nohup: one process = Raft coordinator (single voter) +
# embedded worker executor + web UI/REST API (--web).
#
# Usage:
#   ./scripts/start-hybrid-web.sh              # build (debug) + start
#   ./scripts/start-hybrid-web.sh start|stop|status|restart
#
# Env:
#   HYBRID_ADDR   engine bind address          (default 127.0.0.1:5800)
#   WEB_LISTEN    web console listen address   (default 0.0.0.0:8080)
#   WEB_USER      console login username       (default admin)
#   WEB_PASSWORD  console login password       (default "admin" + warning;
#                 exported so it never shows up in `ps`)
#   STATE_DIR     durable state directory      (default .seatunnel-state/hybrid-web)
#   BIN_DIR       binary directory             (default ./target/debug)
#   NO_BUILD      set to 1 to skip cargo build
#
# Package mode: when this script sits NEXT TO the release binaries (copied
# into target/release by scripts/package-release.sh), it runs them from
# its own directory — fully self-contained, no cargo or repo needed.
#
# The state dir is KEPT across stop/start so resubmitted jobs resume from
# their latest checkpoint. Logs append to $STATE_DIR/server.log.
#
# Two run modes:
#   repo mode     — script inside a checkout: builds (unless NO_BUILD=1)
#                   and runs ./target/debug by default;
#   package mode  — script sits NEXT TO the binaries (the crate's build.rs
#                   copies it into target/<profile>): fully self-contained,
#                   all paths relative (./) — run it from the package dir.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PACKAGE_MODE=0
if [[ -x "$SCRIPT_DIR/seatunnel-engine-server" ]]; then
  # Package mode: binaries live beside this script (the build drops the
  # scripts into target/<profile>). Everything stays relative to the
  # CURRENT directory — run the script from the package dir (./).
  PACKAGE_MODE=1
  BIN_DIR=${BIN_DIR:-.}
  NO_BUILD=1
else
  cd "$SCRIPT_DIR/.."
  BIN_DIR=${BIN_DIR:-./target/debug}
fi

ADDR=${HYBRID_ADDR:-127.0.0.1:5800}
WEB_LISTEN=${WEB_LISTEN:-0.0.0.0:8080}
WEB_USER=${WEB_USER:-${SEATUNNEL_WEB_USER:-admin}}
WEB_PASSWORD=${WEB_PASSWORD:-${SEATUNNEL_WEB_PASSWORD:-}}
STATE_DIR=${STATE_DIR:-.seatunnel-state/hybrid-web}
PID_FILE="$STATE_DIR/hybrid-web.pid"
LOG_FILE="$STATE_DIR/server.log"
ACTION=${1:-start}

command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }

port_open() { (exec 3<>"/dev/tcp/${1%:*}/${1##*:}") 2>/dev/null; }

# Host to use when probing the console over HTTP (bind 0.0.0.0 -> 127.0.0.1).
HEALTH_HOST=${WEB_LISTEN%:*}
if [[ "$HEALTH_HOST" == "0.0.0.0" || "$HEALTH_HOST" == "::" ]]; then
  HEALTH_HOST=127.0.0.1
fi
HEALTH_URL="http://${HEALTH_HOST}:${WEB_LISTEN##*:}/api/v1/health"

healthy() { curl -sf "$HEALTH_URL" 2>/dev/null | grep -q '"status":"ok"'; }

running_pid() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

do_stop() {
  local pid
  if ! pid=$(running_pid); then
    echo "==> not running (no live pid in $PID_FILE)"
    rm -f "$PID_FILE"
    return 0
  fi
  echo "==> stopping hybrid node (pid $pid, state kept in $STATE_DIR)"
  # SIGINT = the server's graceful ctrl_c path (worker unregister, ...).
  kill -INT "$pid" 2>/dev/null || true
  for _ in $(seq 1 15); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "==> still alive after 15s — sending SIGTERM"
    kill -TERM "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "==> stopped"
}

do_start() {
  local pid
  if pid=$(running_pid); then
    echo "error: already running (pid $pid, log: $LOG_FILE) — use '$0 stop' first" >&2
    exit 1
  fi
  rm -f "$PID_FILE"

  for addr in "$ADDR" "$WEB_LISTEN"; do
    if port_open "$addr"; then
      echo "error: $addr already in use — stop the old instance or set HYBRID_ADDR/WEB_LISTEN" >&2
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

  mkdir -p "$STATE_DIR"
  echo "==> starting hybrid node on $ADDR with web console on $WEB_LISTEN (nohup, log: $LOG_FILE)"
  # The password rides in the environment, not argv, so it never shows up
  # in `ps` output.
  export SEATUNNEL_WEB_PASSWORD="$WEB_PASSWORD"
  nohup env RUST_LOG=${RUST_LOG:-info} "$BIN_DIR/seatunnel-engine-server" \
    --role hybrid --addr "$ADDR" --state-dir "$STATE_DIR" \
    --web --web-listen "$WEB_LISTEN" --web-auth-user "$WEB_USER" \
    >>"$LOG_FILE" 2>&1 &
  pid=$!
  echo "$pid" >"$PID_FILE"

  for _ in $(seq 1 30); do
    healthy && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if ! healthy; then
    echo "error: node did not become healthy — tail of $LOG_FILE:" >&2
    tail -20 "$LOG_FILE" >&2
    kill "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    exit 1
  fi

  echo
  echo "=================================================================="
  echo "  Hybrid node + web console is up (background, pid $pid)"
  echo "  Engine       : $ADDR"
  echo "  Console      : http://${HEALTH_HOST}:${WEB_LISTEN##*:}  (login: $WEB_USER)"
  echo "  Log          : $LOG_FILE"
  echo "  Stop         : $0 stop"
  echo "  State in $STATE_DIR is kept, so a resubmitted job resumes from"
  echo "  its latest checkpoint."
  echo "=================================================================="
}

do_status() {
  local pid
  if pid=$(running_pid); then
    echo "running (pid $pid)"
    echo "  engine : $ADDR"
    echo "  console: $HEALTH_URL"
    if healthy; then
      echo "  health : ok ($(curl -sf "$HEALTH_URL"))"
    else
      echo "  health : NOT responding (log: $LOG_FILE)"
    fi
  else
    echo "stopped"
  fi
}

case "$ACTION" in
  start) do_start ;;
  stop) do_stop ;;
  status) do_status ;;
  restart) do_stop; do_start ;;
  *)
    echo "usage: $0 [start|stop|status|restart]" >&2
    exit 1
    ;;
esac
