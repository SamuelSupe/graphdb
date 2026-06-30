#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_DIR="${1:-latest}"
if [[ "$RUN_DIR" == "latest" ]]; then
  RUN_DIR="$(ls -td soak-runs/* 2>/dev/null | head -n 1 || true)"
fi
if [[ -z "$RUN_DIR" || ! -d "$RUN_DIR" ]]; then
  echo "run directory not found" >&2
  exit 1
fi

PID_FILE="$RUN_DIR/pid"
EVENTS="$RUN_DIR/events.ndjson"
LOG="$RUN_DIR/soak.log"
CONTAINER=""
if [[ -f "$RUN_DIR/meta.env" ]]; then
  CONTAINER="$(grep '^container=' "$RUN_DIR/meta.env" | cut -d= -f2- || true)"
fi

status="unknown"
if [[ -n "$CONTAINER" ]] && docker inspect "$CONTAINER" >/dev/null 2>&1; then
  pid="$CONTAINER"
  status="$(docker inspect -f '{{.State.Status}}' "$CONTAINER")"
elif [[ -s "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE")"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    status="running"
  elif [[ -n "$CONTAINER" ]]; then
    status="docker_unavailable"
  else
    status="exited"
  fi
else
  pid=""
fi

echo "run_dir=$RUN_DIR"
echo "process=$pid"
echo "status=$status"
[[ -f "$RUN_DIR/meta.env" ]] && cat "$RUN_DIR/meta.env"

if [[ -f "$EVENTS" ]]; then
  echo "event_lines=$(wc -l <"$EVENTS")"
  echo "event_counts:"
  awk '
    match($0, /"event":"[^"]+"/) {
      event=substr($0, RSTART+9, RLENGTH-10)
      counts[event]++
    }
    END {
      keys[1]="reader_warmup_ok"
      keys[2]="reader_warmup_error"
      keys[3]="reader_checks_skipped"
      keys[4]="query_error"
      keys[5]="write_error"
      keys[6]="compact_ok"
      keys[7]="gc_ok"
      keys[8]="index_rebuild_ok"
      keys[9]="reader_restart_ok"
      keys[10]="soak_done"
      for (i=1; i<=10; i++) {
        key=keys[i]
        printf "  %s=%d\n", key, counts[key]+0
      }
    }
  ' "$EVENTS"
  echo "partial_report:"
  GOCACHE="${GOCACHE:-$ROOT/.gocache}" go run ./tools/soakreport \
    -in "$EVENTS" \
    -warmup "${SOAK_STATUS_WARMUP:-45m}" \
    -require-compact=false \
    -require-gc=false \
    -require-index-rebuild=false \
    -fail-on-error-events=false \
    -max-reader-unready-ratio 1 \
    -max-index-unhealthy-samples 100000 \
    || true
fi

if [[ -f "$LOG" ]]; then
  echo "log_tail:"
  tail -20 "$LOG"
elif [[ -n "$CONTAINER" ]]; then
  echo "container_log_tail:"
  docker logs --tail 20 "$CONTAINER" || true
fi
