#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
: "${GRAPHDB_TEST_BACKUP_S3_ENDPOINT:?Set the test S3 endpoint}"
: "${GRAPHDB_TEST_BACKUP_S3_BUCKET:?Create and set a dedicated test bucket}"
: "${GRAPHDB_TEST_BACKUP_S3_ACCESS_KEY_ID:?Set test credentials}"
: "${GRAPHDB_TEST_BACKUP_S3_SECRET_ACCESS_KEY:?Set test credentials}"
RUN_DIR="${GRAPHDB_BACKUP_GATE_OUTPUT:-$(mktemp -d "${TMPDIR:-/tmp}/graphdb-backup-gate.XXXXXX")}"
mkdir -p "$RUN_DIR"
if ! go test -mod=readonly -race ./internal/backupstore ./internal/storage \
  -run 'TestManifestCannotRedirectRestore|TestObjectBackup' -count=1 -v >"$RUN_DIR/integration.log" 2>&1; then
  cat "$RUN_DIR/integration.log" >&2
  exit 1
fi
go build -mod=readonly -o "$RUN_DIR/graphdb" ./cmd/graphdb
export GRAPHDB_BACKUP_S3_ENDPOINT="$GRAPHDB_TEST_BACKUP_S3_ENDPOINT"
export GRAPHDB_BACKUP_S3_BUCKET="$GRAPHDB_TEST_BACKUP_S3_BUCKET"
export GRAPHDB_BACKUP_S3_ACCESS_KEY_ID="$GRAPHDB_TEST_BACKUP_S3_ACCESS_KEY_ID"
export GRAPHDB_BACKUP_S3_SECRET_ACCESS_KEY="$GRAPHDB_TEST_BACKUP_S3_SECRET_ACCESS_KEY"
export GRAPHDB_BACKUP_S3_PATH_STYLE=true
export GRAPHDB_BACKUP_S3_PREFIX="http-gate-$(date +%s)-$$"
export GRAPHDB_ADDR="127.0.0.1:${GRAPHDB_BACKUP_GATE_PORT:-39180}"
export GRAPHDB_STORAGE=local GRAPHDB_MODE=all GRAPHDB_COORDINATION=local
export GRAPHDB_INGEST_MODE=wal GRAPHDB_INGEST_WAL_DURABILITY=sync
export GRAPHDB_INGEST_FLUSH_INTERVAL=100ms
export GRAPHDB_MAINTENANCE_INTERVAL=0
SERVER_PID=""
stop_server() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}
trap stop_server EXIT
start_server() {
  GRAPHDB_DATA_DIR="$1" "$RUN_DIR/graphdb" serve >>"$RUN_DIR/server.log" 2>&1 &
  SERVER_PID="$!"
  for _ in $(seq 1 120); do
    if curl -fsS --max-time 2 "http://$GRAPHDB_ADDR/v1/readiness" >"$RUN_DIR/readiness.json" 2>/dev/null; then return; fi
    kill -0 "$SERVER_PID" || { cat "$RUN_DIR/server.log" >&2; exit 1; }
    sleep 0.25
  done
  cat "$RUN_DIR/server.log" >&2
  exit 1
}
export PYTHONPATH="$ROOT/sdk/python${PYTHONPATH:+:$PYTHONPATH}"
start_server "$RUN_DIR/source-data"
python3 scripts/object_backup_http.py capture "http://$GRAPHDB_ADDR" "$RUN_DIR" >"$RUN_DIR/http-capture.log"
stop_server
# A new data directory proves recovery needs no original tenant, task history,
# local backup record, indexes, or manifest files.
start_server "$RUN_DIR/restored-data"
python3 scripts/object_backup_http.py restore "http://$GRAPHDB_ADDR" "$RUN_DIR" >"$RUN_DIR/http-restore.log"
stop_server
start_server "$RUN_DIR/restored-data"
python3 scripts/object_backup_http.py verify "http://$GRAPHDB_ADDR" "$RUN_DIR" >"$RUN_DIR/http-restart.log"
printf 'Object backup gate passed. Evidence: %s\n' "$RUN_DIR"
