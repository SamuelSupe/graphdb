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

EVENTS="$RUN_DIR/events.ndjson"
if [[ ! -f "$EVENTS" ]]; then
  echo "events file not found: $EVENTS" >&2
  exit 1
fi

META_DURATION=""
if [[ -f "$RUN_DIR/meta.env" ]]; then
  META_DURATION="$(grep '^duration=' "$RUN_DIR/meta.env" | cut -d= -f2- || true)"
fi

REQUIRED_OPERATIONS="${SOAK_REQUIRED_OPERATIONS:-profile-indexed-match,indexed-match-min-version,indexed-match-allow-stale,range-aggregate-match,fuzzy-service-match,cmdb-service-to-database-path,impact-service,stream-large-indexed-hosts-min-version,scan-entities-min-version,scan-entities-allow-stale,scan-edges-allow-stale,export-snapshot-stream,saved-service-impact}"

GOCACHE="${GOCACHE:-$ROOT/.gocache}" go run -mod=readonly ./tools/soakreport \
  -in "$EVENTS" \
  -min-duration "${SOAK_MIN_DURATION:-${META_DURATION:-24h}}" \
  -warmup "${SOAK_REPORT_WARMUP:-45m}" \
  -max-reader-unready-ratio "${SOAK_MAX_READER_UNREADY_RATIO:-0.20}" \
  -max-index-unhealthy-samples "${SOAK_MAX_INDEX_UNHEALTHY_SAMPLES:-3}" \
  -max-final-commit-tail "${SOAK_MAX_FINAL_COMMIT_TAIL:-300}" \
  -require-reader-restart "${SOAK_REQUIRE_READER_RESTART:-true}" \
  -require-operation "$REQUIRED_OPERATIONS"
