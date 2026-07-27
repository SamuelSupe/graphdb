#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${SOAK_RUN_ID:-$(date +%Y%m%d-%H%M%S)-${SOAK_DURATION:-24h}}"
RUN_DIR="${SOAK_RUN_DIR:-$ROOT/soak-runs/$RUN_ID}"
TENANT="${SOAK_TENANT:-soak-rustfs-$RUN_ID}"
EVENTS="$RUN_DIR/events.ndjson"
LOG="$RUN_DIR/soak.log"
PID_FILE="$RUN_DIR/pid"
CONTAINER="${SOAK_CONTAINER:-graphdb-soak-$RUN_ID}"
META_FILE="$RUN_DIR/meta.env"
COMPOSE_PROJECT="${SOAK_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME:-graphdb}}"
READER_RESTART_COMMAND="${SOAK_READER_RESTART_COMMAND:-docker restart ${COMPOSE_PROJECT}-graphdb-reader-1}"

mkdir -p "$RUN_DIR"
if [[ -s "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE")"
  if kill -0 "$old_pid" 2>/dev/null; then
    echo "soak already running pid=$old_pid run_dir=$RUN_DIR" >&2
    exit 1
  fi
fi

cat >"$META_FILE" <<EOF
run_id=$RUN_ID
run_dir=$RUN_DIR
tenant=$TENANT
container=$CONTAINER
events=$EVENTS
log=$LOG
duration=${SOAK_DURATION:-24h}
min_duration=${SOAK_MIN_DURATION:-0}
require_reader_restart=${SOAK_REQUIRE_READER_RESTART:-false}
compose_project=$COMPOSE_PROJECT
query_start_delay=${SOAK_QUERY_START_DELAY:-35m}
report_warmup=${SOAK_REPORT_WARMUP:-45m}
reader_restart_interval=${SOAK_READER_RESTART_INTERVAL:-1h}
fault_object_read_delay=${GRAPHDB_FAULT_OBJECT_READ_DELAY:-}
writer_url=${WRITER_URL:-http://127.0.0.1:${GRAPHDB_PORT:-38080}}
reader_url=${READER_URL:-http://127.0.0.1:${GRAPHDB_READER_PORT:-38081}}
required_operations=${SOAK_REQUIRED_OPERATIONS:-}
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

wait_health() {
  local url="$1"
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 2 "$url/v1/health" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "health check timed out for $url" >&2
  return 1
}

docker compose -p "$COMPOSE_PROJECT" -f docker-compose.rustfs.yml up -d --build
wait_health "${WRITER_URL:-http://127.0.0.1:${GRAPHDB_PORT:-38080}}"
wait_health "${READER_URL:-http://127.0.0.1:${GRAPHDB_READER_PORT:-38081}}"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

container_id="$(docker run -d \
  --name "$CONTAINER" \
  --network "${COMPOSE_PROJECT}_default" \
  -v "$ROOT:/src" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -w /src \
  -e SOAK_DURATION="${SOAK_DURATION:-24h}" \
  -e SOAK_TENANT="$TENANT" \
  -e SOAK_OUT="/src/soak-runs/$RUN_ID/events.ndjson" \
  -e SOAK_LOG="/src/soak-runs/$RUN_ID/soak.log" \
  -e SOAK_MIN_DURATION="${SOAK_MIN_DURATION:-0}" \
  -e SOAK_REQUIRE_READER_RESTART="${SOAK_REQUIRE_READER_RESTART:-false}" \
  -e SOAK_REPORT_WARMUP="${SOAK_REPORT_WARMUP:-45m}" \
  -e SOAK_WRITERS="${SOAK_WRITERS:-1}" \
  -e SOAK_READERS="${SOAK_READERS:-4}" \
  -e SOAK_BATCH_SIZE="${SOAK_BATCH_SIZE:-10}" \
  -e SOAK_WRITE_INTERVAL="${SOAK_WRITE_INTERVAL:-100ms}" \
  -e SOAK_QUERY_INTERVAL="${SOAK_QUERY_INTERVAL:-200ms}" \
  -e SOAK_QUERY_START_DELAY="${SOAK_QUERY_START_DELAY:-35m}" \
  -e SOAK_READER_MAX_STALENESS="${SOAK_READER_MAX_STALENESS:-10m}" \
  -e SOAK_READER_WARMUP_INTERVAL="${SOAK_READER_WARMUP_INTERVAL:-2s}" \
  -e SOAK_READER_WARMUP_TIMEOUT="${SOAK_READER_WARMUP_TIMEOUT:-2m}" \
  -e SOAK_HTTP_TIMEOUT="${SOAK_HTTP_TIMEOUT:-2m}" \
  -e SOAK_SAMPLE_INTERVAL="${SOAK_SAMPLE_INTERVAL:-1m}" \
  -e SOAK_SNAPSHOT_EXPORT_INTERVAL="${SOAK_SNAPSHOT_EXPORT_INTERVAL:-5m}" \
  -e SOAK_COMPACT_INTERVAL="${SOAK_COMPACT_INTERVAL:-5m}" \
  -e SOAK_GC_INTERVAL="${SOAK_GC_INTERVAL:-30m}" \
  -e SOAK_INDEX_REBUILD_INTERVAL="${SOAK_INDEX_REBUILD_INTERVAL:-10m}" \
  -e SOAK_MAINTENANCE_TIMEOUT="${SOAK_MAINTENANCE_TIMEOUT:-20m}" \
  -e SOAK_READER_RESTART_INTERVAL="${SOAK_READER_RESTART_INTERVAL:-1h}" \
  -e SOAK_READER_RESTART_GRACE="${SOAK_READER_RESTART_GRACE:-45s}" \
  -e SOAK_READER_RESTART_COMMAND="$READER_RESTART_COMMAND" \
  -e SOAK_REQUIRED_OPERATIONS="${SOAK_REQUIRED_OPERATIONS:-}" \
  golang:1.25-alpine sh -c '
    set -eu
    export PATH=/usr/local/go/bin:$PATH
    apk add --no-cache docker-cli >/tmp/soak-apk.log 2>&1
    export GOCACHE=/src/.gocache
    {
      go run -mod=readonly ./tools/soaktest \
        -writer http://graphdb:8080 \
        -reader http://graphdb-reader:8080 \
        -tenant "$SOAK_TENANT" \
        -duration "$SOAK_DURATION" \
        -http-timeout "$SOAK_HTTP_TIMEOUT" \
        -writers "$SOAK_WRITERS" \
        -readers "$SOAK_READERS" \
        -batch-size "$SOAK_BATCH_SIZE" \
        -write-interval "$SOAK_WRITE_INTERVAL" \
        -query-interval "$SOAK_QUERY_INTERVAL" \
        -query-start-delay "$SOAK_QUERY_START_DELAY" \
        -reader-max-staleness "$SOAK_READER_MAX_STALENESS" \
        -reader-warmup-interval "$SOAK_READER_WARMUP_INTERVAL" \
        -reader-warmup-timeout "$SOAK_READER_WARMUP_TIMEOUT" \
        -sample-interval "$SOAK_SAMPLE_INTERVAL" \
        -snapshot-export-interval "$SOAK_SNAPSHOT_EXPORT_INTERVAL" \
        -compact-interval "$SOAK_COMPACT_INTERVAL" \
        -gc-interval "$SOAK_GC_INTERVAL" \
        -index-rebuild-interval "$SOAK_INDEX_REBUILD_INTERVAL" \
        -maintenance-timeout "$SOAK_MAINTENANCE_TIMEOUT" \
        -reader-restart-interval "$SOAK_READER_RESTART_INTERVAL" \
        -reader-restart-grace "$SOAK_READER_RESTART_GRACE" \
        -reader-restart-command "$SOAK_READER_RESTART_COMMAND" \
        -out "$SOAK_OUT"
      go run -mod=readonly ./tools/soakreport \
        -in "$SOAK_OUT" \
        -min-duration "$SOAK_MIN_DURATION" \
        -warmup "$SOAK_REPORT_WARMUP" \
        -max-reader-unready-ratio 0.20 \
        -max-index-unhealthy-samples 3 \
        -max-final-commit-tail 300 \
        -require-reader-restart "$SOAK_REQUIRE_READER_RESTART" \
        -require-operation "$SOAK_REQUIRED_OPERATIONS"
    } >"$SOAK_LOG" 2>&1
  ')"

echo "$container_id" >"$PID_FILE"
echo "started soak container=$CONTAINER id=$container_id run_dir=$RUN_DIR tenant=$TENANT"
echo "events=$EVENTS"
echo "log=$LOG"
