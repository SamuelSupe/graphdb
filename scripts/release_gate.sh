#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ "${RELEASE_GATE_SKIP_STATIC:-0}" != 1 ]]; then
  scripts/check_workspace_hygiene.sh
  scripts/check_release_freeze.sh
  go test -mod=readonly ./...
  go vet -mod=readonly ./...
  go test -mod=readonly -race ./...
  scripts/compatibility_v1_0_v1_1.sh
  python3 -m unittest discover -s sdk/python/tests -p 'test_*.py'
fi
[[ "${RELEASE_GATE_VERIFY_ONLY:-0}" == 1 ]] && exit 0
RUN_DIR="${GRAPHDB_GATE_OUTPUT:-$(mktemp -d "${TMPDIR:-/tmp}/graphdb-local-gate.XXXXXX")}"
mkdir -p "$RUN_DIR"
PORT="${GRAPHDB_PORT:-38080}"
BASE="http://127.0.0.1:$PORT"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

go build -mod=readonly -o "$RUN_DIR/graphdb" ./cmd/graphdb
start_server() {
  GRAPHDB_ADDR="127.0.0.1:$PORT" GRAPHDB_DATA_DIR="$RUN_DIR/data" \
    GRAPHDB_STORAGE=local GRAPHDB_COORDINATION=local GRAPHDB_MODE=all \
    GRAPHDB_INSTANCE_ID=local-gate GRAPHDB_INGEST_MODE="$1" \
    GRAPHDB_READER_CACHE_MAX_BYTES="${GRAPHDB_GATE_READER_CACHE_BYTES:-1}" \
    GRAPHDB_INGEST_FLUSH_INTERVAL=100ms GRAPHDB_INGEST_WAL_DURABILITY=sync \
    "$RUN_DIR/graphdb" serve >>"$RUN_DIR/server.log" 2>&1 &
  SERVER_PID="$!"
  for _ in $(seq 1 120); do
    if curl -fsS --max-time 2 "$BASE/v1/readiness" >"$RUN_DIR/readiness.json" 2>/dev/null; then return; fi
    kill -0 "$SERVER_PID" || { cat "$RUN_DIR/server.log" >&2; exit 1; }
    sleep 0.25
  done
  cat "$RUN_DIR/server.log" >&2
  exit 1
}
for mode in direct wal; do
  start_server "$mode"
  if [[ "$mode" == direct ]]; then
    GRAPHDB_TEST_BASE_URL="$BASE" GRAPHDB_TEST_TENANT=python-local-gate \
      python3 -m unittest discover -s sdk/python/tests -p 'test_*.py' >"$RUN_DIR/python-sdk.log" 2>&1
  fi
  go run -mod=readonly ./tools/e2echeck -writer "$BASE" -reader "$BASE" -timeout 5m >"$RUN_DIR/e2e-$mode.log"
  go run -mod=readonly ./tools/loadtest -base "$BASE" -reader-base "$BASE" \
    -tenant "local-gate-$mode" -writers 4 -readers 16 -batches 10 -batch-size 20 \
    -timeout 5m -report-json "$RUN_DIR/load-$mode.json" >"$RUN_DIR/load-$mode.log"
  curl -fsS "$BASE/v1/export/snapshot" -H "X-Tenant-ID: local-gate-$mode" >"$RUN_DIR/before-$mode.json"
  cleanup
  SERVER_PID=""
  start_server "$mode"
  curl -fsS "$BASE/v1/export/snapshot" -H "X-Tenant-ID: local-gate-$mode" >"$RUN_DIR/after-$mode.json"
  python3 - "$RUN_DIR/before-$mode.json" "$RUN_DIR/after-$mode.json" <<'PY'
import json,sys
before,after=(json.load(open(p)) for p in sys.argv[1:])
assert before == after, "snapshot changed after restart"
PY
  if [[ "$mode" == wal && "${GRAPHDB_GATE_SOAK:-0}" == 1 ]]; then
    cleanup
    SERVER_PID=""
    GRAPHDB_GATE_READER_CACHE_BYTES=2147483648
    export GRAPHDB_WRITE_CACHE_MAX_BYTES=2147483648
    start_server wal
    go run -mod=readonly ./tools/soaktest -writer "$BASE" -reader "$BASE" \
      -tenant local-disk-soak -duration 30m -writers 4 -readers 16 -batch-size 20 -write-interval 5s \
      -http-timeout 120s \
      -compact-interval 5m -gc-interval 10m -index-rebuild-interval 10m \
      -out "$RUN_DIR/soak.ndjson"
    go run -mod=readonly ./tools/soakreport -in "$RUN_DIR/soak.ndjson" \
      -min-duration 30m -warmup 1m >"$RUN_DIR/soak-report.txt"
  fi
  cleanup
  SERVER_PID=""
done
if [[ "${GRAPHDB_GATE_OBJECT_BACKUP:-0}" == 1 ]]; then
  scripts/object_backup_gate.sh
fi
printf 'Local disk gate passed. Evidence: %s\n' "$RUN_DIR"
