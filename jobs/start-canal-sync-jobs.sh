#!/usr/bin/env bash
# Render, validate and submit the canal-sync SeaTunnel job set
# (Kafka sinks emitting the canal-client JSON message shape with
# per-table topic routing).
#
# Two equivalent layouts exist under jobs/configs/ — submit ONE of
# them, never both (same pipelines / server-ids would collide and
# re-deliver every message):
#   alone/*.yaml                      split layout, one pipeline per
#                                     file (production best practice,
#                                     DEFAULT selection)
#   canal-sync-consolidated.yaml      single-file bundle with all five
#                                     pipelines (integration testing,
#                                     opt-in via --only)
#
# Commands:
#   start    render + validate + submit every selected job. Jobs already
#            submitted are compared against the recorded config hash —
#            unchanged ones are skipped, changed ones are restarted with
#            the SAME job id via `job update` (checkpoint restore).
#   render   render + validate only, never submits
#   status   show the status of the recorded jobs
#   cancel   cancel the recorded jobs
#   restart  unconditionally cancel + resubmit every selected job with
#            its current rendered config (same job id; submits fresh
#            when no job id is recorded yet)
#
# Flags:
#   --dry-run        alias of `render`
#   --only a,b,c     restrict processing to these file stems, matched
#                    against both layouts (e.g. --only v3-core,kefu or
#                    --only canal-sync-consolidated); without it only
#                    the alone/ split layout is processed
#   --master ADDR    cluster master address (default: $SEATUNNEL_MASTER
#                    or 127.0.0.1:5800)
#   --force          start: restart even when the rendered config is
#                    unchanged since submit
#
# Environment — every __TOKEN__ in a job file resolves from the
# CANAL_SYNC_<TOKEN> variable of the same name (empty value keeps the
# token in place and blocks submit). Per-pipeline MySQL tokens carry the
# canal instance port topology (v3 3001 / ailearn 3002 / recommand 3003
# / kefu 3019 on mysql-yace.wf):
#   SEATUNNEL_BIN                  seatunnel CLI (default "seatunnel")
#   CANAL_SYNC_MYSQL_USER          MySQL user          (all pipelines)
#   CANAL_SYNC_MYSQL_PASSWORD      MySQL password      (all pipelines)
#   CANAL_SYNC_V3_MYSQL_HOST/_PORT       v3-core + user-role (3001)
#   CANAL_SYNC_AILEARN_MYSQL_HOST/_PORT  ailearn           (3002)
#   CANAL_SYNC_RECOMMAND_MYSQL_HOST/_PORT recommand        (3003)
#   CANAL_SYNC_KEFU_MYSQL_HOST/_PORT     kefu              (3019)
#   CANAL_SYNC_KAFKA_SERVERS       business Kafka cluster
#   CANAL_SYNC_RABBITMQ_HOST/_PORT/_USER/_PASSWORD   user-role pipeline
#
# NOTE: values are substituted via sed; avoid `|`, `&`, `\` and quotes
# in credentials, or adapt escape_sed() below. Passwords are written
# into the rendered YAML inside double quotes — avoid `"` in them.
set -euo pipefail

# ── 默认配置（可通过同名环境变量覆盖）──
export CANAL_SYNC_MYSQL_USER="${CANAL_SYNC_MYSQL_USER:-svr_canal}"
export CANAL_SYNC_MYSQL_PASSWORD="${CANAL_SYNC_MYSQL_PASSWORD:-YjaV5><6T}"

export CANAL_SYNC_V3_MYSQL_HOST="${CANAL_SYNC_V3_MYSQL_HOST:-mysql-yace.wf}"
export CANAL_SYNC_V3_MYSQL_PORT="${CANAL_SYNC_V3_MYSQL_PORT:-3001}"
export CANAL_SYNC_AILEARN_MYSQL_HOST="${CANAL_SYNC_AILEARN_MYSQL_HOST:-mysql-yace.wf}"
export CANAL_SYNC_AILEARN_MYSQL_PORT="${CANAL_SYNC_AILEARN_MYSQL_PORT:-3002}"
export CANAL_SYNC_RECOMMAND_MYSQL_HOST="${CANAL_SYNC_RECOMMAND_MYSQL_HOST:-mysql-yace.wf}"
export CANAL_SYNC_RECOMMAND_MYSQL_PORT="${CANAL_SYNC_RECOMMAND_MYSQL_PORT:-3003}"
export CANAL_SYNC_KEFU_MYSQL_HOST="${CANAL_SYNC_KEFU_MYSQL_HOST:-mysql-yace.wf}"
export CANAL_SYNC_KEFU_MYSQL_PORT="${CANAL_SYNC_KEFU_MYSQL_PORT:-3019}"

export CANAL_SYNC_KAFKA_SERVERS="${CANAL_SYNC_KAFKA_SERVERS:-kafka20-yace.wf:9092}"

export CANAL_SYNC_RABBITMQ_HOST="${CANAL_SYNC_RABBITMQ_HOST:-mq-yace.xk12b.cn}"
export CANAL_SYNC_RABBITMQ_PORT="${CANAL_SYNC_RABBITMQ_PORT:-5555}"
export CANAL_SYNC_RABBITMQ_USER="${CANAL_SYNC_RABBITMQ_USER:-admin}"
export CANAL_SYNC_RABBITMQ_PASSWORD="${CANAL_SYNC_RABBITMQ_PASSWORD:-dsjw2014}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/configs"
BUILD_DIR="$SCRIPT_DIR/build"
RENDER_DIR="$BUILD_DIR/rendered"
IDS_DIR="$BUILD_DIR/job-ids"

SEATUNNEL_BIN="${SEATUNNEL_BIN:-seatunnel}"
MASTER="${SEATUNNEL_MASTER:-127.0.0.1:5800}"
DRY_RUN=0
FORCE=0
ONLY=""
COMMAND="start"

usage() { sed -n '2,49p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    start|render|status|cancel|restart) COMMAND="$1" ;;
    --dry-run) DRY_RUN=1 ;;
    --force) FORCE=1 ;;
    --only) ONLY="${2:?--only needs a value}"; shift ;;
    --only=*) ONLY="${1#*=}" ;;
    --master) MASTER="${2:?--master needs a value}"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage 1 ;;
  esac
  shift
done
[[ "$COMMAND" == "start" && "$DRY_RUN" == "1" ]] && COMMAND="render"

# --------------------------------------------------------------------------
# Selection: default = the split layout (configs/alone/*.yaml, production).
# --only matches file stems across BOTH layouts, which is also how the
# consolidated testing bundle (configs/canal-sync-consolidated.yaml) is
# submitted.
# --------------------------------------------------------------------------
candidate_files=()
for f in "$CONFIG_DIR"/alone/*.yaml "$CONFIG_DIR"/*.yaml; do
  [[ -f "$f" ]] && candidate_files+=("$f")
done

if [[ -n "$ONLY" ]]; then
  selected_files=()
  IFS=',' read -ra WANT <<< "$ONLY"
  for f in "${candidate_files[@]}"; do
    stem="$(basename "$f" .yaml)"
    for w in "${WANT[@]}"; do
      [[ "$stem" == "$w" ]] && { selected_files+=("$f"); break; }
    done
  done
  [[ ${#selected_files[@]} -gt 0 ]] || { echo "no job file matches --only $ONLY" >&2; exit 1; }
else
  selected_files=()
  for f in "$CONFIG_DIR"/alone/*.yaml; do [[ -f "$f" ]] && selected_files+=("$f"); done
  [[ ${#selected_files[@]} -gt 0 ]] || { echo "no job files under $CONFIG_DIR/alone (create the split layout or use --only)" >&2; exit 1; }
fi

# --------------------------------------------------------------------------
# Rendering: substitute every __TOKEN__ found in the file from the
# CANAL_SYNC_<TOKEN> variable (data-driven — new tokens need no code).
# Empty values keep the token so the placeholder gate skips submit.
# --------------------------------------------------------------------------
escape_sed() { # escape sed replacement metacharacters
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//|/\\|}"
  s="${s//&/\\&}"
  echo "$s"
}

render_file() { # render_file <yaml-path> -> rendered path on stdout
  local src="$1" token var value sedexpr=()
  while IFS= read -r token; do
    [[ -z "$token" ]] && continue
    var="CANAL_SYNC_${token#__}"
    var="${var%__}"
    value="${!var:-}"
    [[ -z "$value" ]] && continue
    sedexpr+=(-e "s|${token}|$(escape_sed "$value")|g")
  done < <(grep -oE '__[A-Z0-9_]+__' "$src" | sort -u)
  local out="$RENDER_DIR/$(basename "$src")"
  if [[ ${#sedexpr[@]} -gt 0 ]]; then
    sed "${sedexpr[@]}" "$src" > "$out"
  else
    cp "$src" "$out"
  fi
  echo "$out"
}

unfilled() { # unfilled <rendered-yaml> -> remaining __TOKEN__ lines
  # Comments routinely mention __TOKEN__ by name; only real (non-comment)
  # placeholders gate submission.
  grep -vE '^[[:space:]]*#' "$1" | grep -nE '__[A-Z0-9_]+__' || true
}

file_hash() { # file_hash <path> -> sha256 hex (portable macOS/Linux)
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

record_hash() { # record_hash <hash-file> <rendered-yaml>
  file_hash "$2" > "$1"
}

config_changed() { # config_changed <stem> <source-yaml> <rendered-yaml> <id-file> -> 0/1
  # Change detection against the recorded submit state: sha256 of the
  # rendered config when available, falling back to source mtime for
  # jobs submitted before hash recording existed. --force always reads
  # as changed.
  local stem="$1" src="$2" out="$3" idfile="$4"
  local hashfile="$IDS_DIR/$stem.hash"
  if [[ "$FORCE" == "1" ]]; then return 0; fi
  if [[ -f "$hashfile" ]]; then
    [[ "$(file_hash "$out")" != "$(cat "$hashfile")" ]]
  else
    [[ "$src" -nt "$idfile" ]]
  fi
}

do_submit() { # do_submit <stem> <rendered-yaml> — fresh submit, records id + hash
  local stem="$1" out="$2" outtext jobid
  echo "SUBMIT   $stem ..."
  if outtext="$("$SEATUNNEL_BIN" job submit -c "$out" -a "$MASTER" 2>&1)"; then
    jobid="$(echo "$outtext" | awk '/Submitted job/ {print $3; exit}')"
    if [[ -n "$jobid" ]]; then
      echo "$jobid" > "$IDS_DIR/$stem.id"
      record_hash "$IDS_DIR/$stem.hash" "$out"
      echo "OK       $stem -> job $jobid"
      return 0
    fi
    echo "WARN     $stem — submitted but no job id parsed:"
    echo "$outtext" | sed 's/^/          /'
    return 1
  fi
  echo "FAIL     $stem:"
  echo "$outtext" | sed 's/^/          /'
  return 1
}

do_update() { # do_update <stem> <rendered-yaml> <job-id>
  # Restart the recorded job onto the rendered config via the CLI's
  # update flow: cancel (exit checkpoint) → wait CANCELLED → resubmit
  # the SAME id (checkpoint restore). On cancel timeout it aborts
  # WITHOUT resubmitting, so the id/hash records stay valid for a retry.
  local stem="$1" out="$2" jobid="$3" outtext
  echo "SYNC     $stem — config changed since submit, restarting job $jobid with it ..."
  if outtext="$("$SEATUNNEL_BIN" job update -c "$out" -i "$jobid" -a "$MASTER" --watch=false 2>&1)"; then
    record_hash "$IDS_DIR/$stem.hash" "$out"
    echo "OK       $stem -> job $jobid now runs the current config"
    return 0
  fi
  echo "FAIL     $stem (job $jobid may still run the OLD config — inspect and retry):"
  echo "$outtext" | sed 's/^/          /'
  return 1
}

mkdir -p "$RENDER_DIR" "$IDS_DIR"

case "$COMMAND" in
  # ---------------------------------------------------------------- render
  render)
    ready=0; blocked=0
    for f in "${selected_files[@]}"; do
      out="$(render_file "$f")"
      if [[ -n "$(unfilled "$out")" ]]; then
        echo "BLOCKED  $(basename "$f") — fill these, then rerun:"
        unfilled "$out" | sed 's/^/          /'
        blocked=$((blocked + 1))
      else
        echo "ready    $(basename "$f") -> $out"
        ready=$((ready + 1))
      fi
    done
    echo "-------- $ready ready, $blocked blocked (unfilled connection tokens)"
    ;;

  # ----------------------------------------------------------------- start
  start)
    submitted=0; synced=0; skipped=0; failed=0
    for f in "${selected_files[@]}"; do
      stem="$(basename "$f" .yaml)"
      out="$(render_file "$f")"
      if [[ -n "$(unfilled "$out")" ]]; then
        echo "SKIP     $stem — unfilled placeholders:"
        unfilled "$out" | sed 's/^/          /'
        skipped=$((skipped + 1)); continue
      fi
      idfile="$IDS_DIR/$stem.id"
      if [[ ! -f "$idfile" ]]; then
        if do_submit "$stem" "$out"; then
          submitted=$((submitted + 1))
        else
          failed=$((failed + 1))
        fi
        continue
      fi
      jobid="$(cat "$idfile")"
      if ! config_changed "$stem" "$f" "$out" "$idfile"; then
        echo "SKIP     $stem — config unchanged since submit (job $jobid)"
        skipped=$((skipped + 1)); continue
      fi
      if [[ ! -f "$IDS_DIR/$stem.hash" ]]; then
        echo "NOTE     $stem — no hash recorded at submit time; changed judged by file mtime"
      fi
      if do_update "$stem" "$out" "$jobid"; then
        synced=$((synced + 1))
      else
        failed=$((failed + 1))
      fi
    done
    echo "-------- submitted=$submitted synced=$synced skipped=$skipped failed=$failed (master $MASTER)"
    [[ "$failed" == "0" ]] || exit 1
    ;;

  # ---------------------------------------------------------------- status
  status)
    found=0
    for f in "${selected_files[@]}"; do
      stem="$(basename "$f" .yaml)"
      idfile="$IDS_DIR/$stem.id"
      [[ -f "$idfile" ]] || continue
      note=""
      if [[ -f "$IDS_DIR/$stem.hash" ]]; then
        out="$(render_file "$f")"
        if [[ "$(file_hash "$out")" != "$(cat "$IDS_DIR/$stem.hash")" ]]; then
          note="  (config changed on disk — run 'start' to sync)"
        fi
      elif [[ "$f" -nt "$idfile" ]]; then
        note="  (config file edited after submit — run 'start' to sync)"
      fi
      echo "== $stem (job $(cat "$idfile"))$note"
      "$SEATUNNEL_BIN" job status --job-id "$(cat "$idfile")" -a "$MASTER" 2>&1 | sed 's/^/   /' || true
      found=$((found + 1))
    done
    [[ "$found" == "0" ]] && echo "no recorded jobs under $IDS_DIR (nothing started yet)"
    ;;

  # ---------------------------------------------------------------- cancel
  cancel)
    cancelled=0
    for f in "${selected_files[@]}"; do
      stem="$(basename "$f" .yaml)"
      idfile="$IDS_DIR/$stem.id"
      [[ -f "$idfile" ]] || continue
      jobid="$(cat "$idfile")"
      echo "CANCEL   $stem (job $jobid)"
      if "$SEATUNNEL_BIN" job cancel --job-id "$jobid" -a "$MASTER" 2>&1; then
        rm -f "$idfile" "$IDS_DIR/$stem.hash"
        cancelled=$((cancelled + 1))
      else
        echo "FAIL     could not cancel $jobid" >&2
      fi
    done
    echo "-------- cancelled=$cancelled"
    ;;

  # --------------------------------------------------------------- restart
  restart)
    # Unconditional bring-to-current-config: restart recorded jobs with
    # their freshly rendered config (same id, checkpoint restore) and
    # submit the ones never submitted. Unlike plain `start` this ignores
    # the change detection entirely.
    synced=0; submitted=0; failed=0
    for f in "${selected_files[@]}"; do
      stem="$(basename "$f" .yaml)"
      out="$(render_file "$f")"
      if [[ -n "$(unfilled "$out")" ]]; then
        echo "SKIP     $stem — unfilled placeholders:"
        unfilled "$out" | sed 's/^/          /'
        continue
      fi
      idfile="$IDS_DIR/$stem.id"
      if [[ -f "$idfile" ]]; then
        if do_update "$stem" "$out" "$(cat "$idfile")"; then
          synced=$((synced + 1))
        else
          failed=$((failed + 1))
        fi
      else
        if do_submit "$stem" "$out"; then
          submitted=$((submitted + 1))
        else
          failed=$((failed + 1))
        fi
      fi
    done
    echo "-------- restarted=$synced submitted=$submitted failed=$failed (master $MASTER)"
    [[ "$failed" == "0" ]] || exit 1
    ;;
esac
