#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WRITER_URL="${WRITER_URL:-http://127.0.0.1:${GRAPHDB_PORT:-38080}}"
READER_URL="${READER_URL:-http://127.0.0.1:${GRAPHDB_READER_PORT:-38081}}"
TENANT="${SOAK_TENANT:-soak-rustfs-$(date +%s)}"
OUT="${SOAK_OUT:-soak-${TENANT}.ndjson}"
COMPOSE_PROJECT="${SOAK_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME:-graphdb}}"
READER_RESTART_COMMAND="${SOAK_READER_RESTART_COMMAND:-docker compose -p $COMPOSE_PROJECT -f docker-compose.rustfs.yml restart graphdb-reader}"

docker compose -p "$COMPOSE_PROJECT" -f docker-compose.rustfs.yml up -d --build

go run -mod=readonly ./tools/soaktest \
  -writer "$WRITER_URL" \
  -reader "$READER_URL" \
  -tenant "$TENANT" \
  -duration "${SOAK_DURATION:-24h}" \
  -http-timeout "${SOAK_HTTP_TIMEOUT:-2m}" \
  -writers "${SOAK_WRITERS:-1}" \
  -readers "${SOAK_READERS:-4}" \
  -batch-size "${SOAK_BATCH_SIZE:-10}" \
  -write-interval "${SOAK_WRITE_INTERVAL:-100ms}" \
  -query-interval "${SOAK_QUERY_INTERVAL:-200ms}" \
  -query-start-delay "${SOAK_QUERY_START_DELAY:-35m}" \
  -reader-max-staleness "${SOAK_READER_MAX_STALENESS:-10m}" \
  -reader-warmup-interval "${SOAK_READER_WARMUP_INTERVAL:-2s}" \
  -reader-warmup-timeout "${SOAK_READER_WARMUP_TIMEOUT:-2m}" \
  -sample-interval "${SOAK_SAMPLE_INTERVAL:-1m}" \
  -snapshot-export-interval "${SOAK_SNAPSHOT_EXPORT_INTERVAL:-5m}" \
  -compact-interval "${SOAK_COMPACT_INTERVAL:-5m}" \
  -gc-interval "${SOAK_GC_INTERVAL:-30m}" \
  -index-rebuild-interval "${SOAK_INDEX_REBUILD_INTERVAL:-10m}" \
  -maintenance-timeout "${SOAK_MAINTENANCE_TIMEOUT:-20m}" \
  -reader-restart-interval "${SOAK_READER_RESTART_INTERVAL:-1h}" \
  -reader-restart-grace "${SOAK_READER_RESTART_GRACE:-45s}" \
  -reader-restart-command "$READER_RESTART_COMMAND" \
  -out "$OUT"

go run -mod=readonly ./tools/soakreport \
  -in "$OUT" \
  -min-duration "${SOAK_MIN_DURATION:-0}" \
  -warmup "${SOAK_REPORT_WARMUP:-45m}" \
  -max-reader-unready-ratio "${SOAK_MAX_READER_UNREADY_RATIO:-0.20}" \
  -max-index-unhealthy-samples "${SOAK_MAX_INDEX_UNHEALTHY_SAMPLES:-3}" \
  -max-final-commit-tail "${SOAK_MAX_FINAL_COMMIT_TAIL:-300}" \
  -require-reader-restart "${SOAK_REQUIRE_READER_RESTART:-false}" \
  -require-operation "${SOAK_REQUIRED_OPERATIONS:-}"
