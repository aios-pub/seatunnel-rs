#!/usr/bin/env bash
# The blessed way to (re)build the web console frontend bundle.
#
# Always builds with --release + the size-tuned profile: the committed
# seatunnel-web/ui/dist is embedded into the server binaries via rust-embed.
# A dist built WITHOUT --release carries a ~33 MB debug wasm (release+wasm-opt
# is ~3-5 MB) — that mistake once made the console load 33.8 MB per visit.
#
# Usage: scripts/build-web-ui.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../seatunnel-web/ui"

command -v trunk >/dev/null || {
  echo "error: trunk not found — install with: cargo install trunk" >&2
  exit 1
}

trunk build --release

# Guard the build against a silently skipped wasm-opt / wrong profile.
wasm=$(ls -S "$PWD/dist"/*.wasm 2>/dev/null | head -1 || true)
if [[ -z "$wasm" ]]; then
  echo "error: no wasm bundle produced" >&2
  exit 1
fi
size_mb=$(( $(stat -f%z "$wasm" 2>/dev/null || stat -c%s "$wasm") / 1024 / 1024 ))
if (( size_mb >= 10 )); then
  echo "error: dist wasm is ${size_mb} MB — this looks like a debug-profile build." >&2
  echo "       Committing it would bloat every served page; investigate before committing." >&2
  exit 1
fi
echo "==> OK: $(basename "$wasm") is ${size_mb} MB (release profile). Commit dist/ now."
