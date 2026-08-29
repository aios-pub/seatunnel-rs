#!/usr/bin/env bash
# Hybrid node + embedded web console from RELEASE binaries, running in the
# background under nohup with debug logging: one process = Raft coordinator
# (single voter) + embedded worker executor + web UI/REST API (--web).
#
# "debug" here means the LOG LEVEL (SEATUNNEL_LOG=debug), not the build
# profile — the binaries are always taken from target/release.
#
# Usage:
#   ./scripts/start-hybrid-web-debug.sh             # release build + stage ./bin + start
#   ./scripts/start-hybrid-web-debug.sh stop|status|restart
#
# Env (passed through to start-hybrid-web.sh):
#   HYBRID_ADDR / WEB_LISTEN / WEB_USER / WEB_PASSWORD / STATE_DIR
#   SEATUNNEL_LOG  log filter         (default debug)
#   NO_BUILD=1     repo mode: skip build AND ./bin refresh
#
# Packaged mode: when this script sits NEXT TO the binaries (the crate's
# build.rs copies it into target/<profile>), it skips build/staging and
# runs them via ./ relative paths — run it from the package directory.
#
# To run a debug-logging node next to a normal one, give it its own ports
# + state, e.g.
#   HYBRID_ADDR=127.0.0.1:15800 WEB_LISTEN=0.0.0.0:18080 \
#   STATE_DIR=.seatunnel-state/hybrid-web-debug ./scripts/start-hybrid-web-debug.sh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PACKAGE_MODE=0
if [[ -x "$SCRIPT_DIR/seatunnel-engine-server" ]]; then
  # Package mode: binaries beside this script; everything relative to the
  # CURRENT directory (./) — run the script from the package directory.
  PACKAGE_MODE=1
  BIN_DIR=${BIN_DIR:-.}
else
  cd "$SCRIPT_DIR/.."
  BIN_DIR=${BIN_DIR:-./bin}
fi
ACTION=${1:-start}

case "$ACTION" in
  start|restart|stop|status) ;;
  *)
    echo "usage: $0 [start|stop|status|restart]" >&2
    exit 1
    ;;
esac

# Building/staging only matters in repo mode when launching something.
if [[ "$PACKAGE_MODE" != "1" ]] && [[ "$ACTION" == "start" || "$ACTION" == "restart" ]]; then
  if [[ "${NO_BUILD:-0}" != "1" ]]; then
    echo "==> building engine server + CLI (release profile)"
    cargo build --release -p seatunnel-engine-server -p seatunnel-cli
    mkdir -p "$BIN_DIR"
    for bin in seatunnel-engine-server seatunnel; do
      src=./target/release/$bin
      [[ -x "$src" ]] || { echo "error: $src not found — build first or set NO_BUILD=1" >&2; exit 1; }
      cp -f "$src" "$BIN_DIR/$bin"
    done
    echo "==> staged release binaries into $BIN_DIR"
  fi
  for bin in seatunnel-engine-server seatunnel; do
    [[ -x "$BIN_DIR/$bin" ]] || { echo "error: $BIN_DIR/$bin not found — run without NO_BUILD or stage $BIN_DIR yourself" >&2; exit 1; }
  done
fi

# SEATUNNEL_LOG beats RUST_LOG/--debug inside the server; the inner script
# inherits the environment through nohup.
export SEATUNNEL_LOG=${SEATUNNEL_LOG:-debug}

# NO_BUILD=1 for the inner script: the binaries are already in place
# (package mode) or staged above (repo mode) — run $BIN_DIR as-is.
exec env NO_BUILD=1 BIN_DIR="$BIN_DIR" "$SCRIPT_DIR/start-hybrid-web.sh" "$@"
