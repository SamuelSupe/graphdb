#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
V1_TAG="${GRAPHDB_V1_COMPAT_TAG:-release_20260722_01}"
PORT="${GRAPHDB_COMPAT_PORT:-38091}"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/graphdb-compat.XXXXXX")"
OLD_SOURCE="$TMP_ROOT/v1-source"
DATA_DIR="$TMP_ROOT/data"
OLD_BIN="$TMP_ROOT/graphdb-v1.0"
NEW_BIN="$TMP_ROOT/graphdb-v1.1"
GOCACHE="${GRAPHDB_COMPAT_GOCACHE:-$TMP_ROOT/go-cache}"
BUILD_PARALLELISM="${GRAPHDB_COMPAT_BUILD_PARALLELISM:-2}"
SERVER_PID=""
export GOCACHE

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

fail() {
  echo "compatibility failure: $*" >&2
  exit 1
}

[[ "$BUILD_PARALLELISM" =~ ^[1-9][0-9]*$ ]] ||
  fail "GRAPHDB_COMPAT_BUILD_PARALLELISM must be a positive integer"

run_with_data() {
  GRAPHDB_STORAGE=local \
  GRAPHDB_DATA_DIR="$DATA_DIR" \
  GRAPHDB_PREFIX=compat \
  GRAPHDB_MODE=all \
  GRAPHDB_INSTANCE_ID=compat-writer \
  "$@"
}

wait_health() {
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 2 "http://127.0.0.1:$PORT/v1/health" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  sed -n '1,160p' "$TMP_ROOT/v1.1-server.log" >&2 || true
  fail "v1.1 compatibility server did not become healthy"
}

cd "$ROOT"
git rev-parse --verify "${V1_TAG}^{commit}" >/dev/null ||
  fail "v1.0 compatibility tag $V1_TAG is unavailable; CI checkout must use fetch-depth: 0"

mkdir -p "$OLD_SOURCE" "$DATA_DIR" "$GOCACHE"
git archive "$V1_TAG" | tar -x -C "$OLD_SOURCE"

echo "building v1.0 binary from $V1_TAG"
(
  cd "$OLD_SOURCE"
  go build -p "$BUILD_PARALLELISM" -mod=mod -trimpath -o "$OLD_BIN" ./cmd/graphdb
)
go build -p "$BUILD_PARALLELISM" -mod=readonly -trimpath -o "$NEW_BIN" ./cmd/graphdb

cat >"$TMP_ROOT/v1.0-commit.json" <<'JSON'
{
  "mutations": {
    "upsert_ci_types": [
      {"name": "host", "fields": {"name": {"type": "string"}}},
      {"name": "service", "fields": {"name": {"type": "string"}}}
    ],
    "upsert_relation_types": [
      {"name": "runs_on", "from_kind": "service", "to_kind": "host", "directed": true}
    ],
    "upsert_entities": [
      {"id": "host:v1", "kind": "host", "fields": {"name": "old-v1-host"}},
      {"id": "service:v1", "kind": "service", "fields": {"name": "old-v1-service"}}
    ],
    "upsert_edges": [
      {"id": "edge:v1", "type": "runs_on", "from": "service:v1", "to": "host:v1"}
    ]
  }
}
JSON
cat >"$TMP_ROOT/host-query.json" <<'JSON'
{"op":"match","kind":"host","limit":10}
JSON

echo "checking v1.0 writer -> v1.1 reader"
run_with_data "$OLD_BIN" init-tenant tenant-from-v1 >/dev/null
run_with_data "$OLD_BIN" commit tenant-from-v1 "$TMP_ROOT/v1.0-commit.json" >/dev/null
run_with_data "$NEW_BIN" query tenant-from-v1 "$TMP_ROOT/host-query.json" >"$TMP_ROOT/v1.1-read.json"
grep -F '"old-v1-host"' "$TMP_ROOT/v1.1-read.json" >/dev/null ||
  fail "v1.1 could not read the v1.0 entity"

cat >"$TMP_ROOT/v1.1-commit.json" <<'JSON'
{
  "mutations": {
    "upsert_entity_types": [
      {"name": "document", "display_name": "Document"}
    ],
    "upsert_relation_types": [
      {"name": "cites", "from_kind": "document", "to_kind": "document", "directed": true}
    ],
    "upsert_entities": [
      {
        "id": "document:one",
        "kind": "document",
        "labels": ["article", "knowledge"],
        "fields": {"title": "v11-document"}
      },
      {"id": "document:two", "kind": "document", "labels": ["article"]}
    ],
    "upsert_edges": [
      {
        "id": "edge:cites",
        "type": "cites",
        "from": "document:one",
        "to": "document:two",
        "fields": {"strength": 0.9}
      }
    ]
  }
}
JSON
cat >"$TMP_ROOT/document-query.json" <<'JSON'
{"op":"match","kind":"document","limit":10}
JSON
cat >"$TMP_ROOT/v1.1-tail.json" <<'JSON'
{
  "mutations": {
    "upsert_entities": [
      {
        "id": "document:tail",
        "kind": "document",
        "labels": ["knowledge"],
        "fields": {"title": "v11-tail-after-snapshot"}
      }
    ]
  }
}
JSON

echo "checking v1.1 writer -> v1.0 reader"
run_with_data "$NEW_BIN" init-tenant tenant-from-v11 >/dev/null
run_with_data "$NEW_BIN" commit tenant-from-v11 "$TMP_ROOT/v1.1-commit.json" >/dev/null
run_with_data "$NEW_BIN" compact tenant-from-v11 >/dev/null
run_with_data "$NEW_BIN" commit tenant-from-v11 "$TMP_ROOT/v1.1-tail.json" >/dev/null

GRAPHDB_STORAGE=local \
GRAPHDB_DATA_DIR="$DATA_DIR" \
GRAPHDB_PREFIX=compat \
GRAPHDB_MODE=all \
GRAPHDB_INSTANCE_ID=compat-writer \
GRAPHDB_ADDR="127.0.0.1:$PORT" \
GRAPHDB_MAINTENANCE_INTERVAL=0 \
"$NEW_BIN" serve >"$TMP_ROOT/v1.1-server.log" 2>&1 &
SERVER_PID="$!"
wait_health
curl -fsS -X PUT "http://127.0.0.1:$PORT/v1/relation-schemas/cites" \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: tenant-from-v11' \
  --data '{"strict":true,"fields":{"strength":{"type":"number","required":true}}}' \
  >"$TMP_ROOT/relation-schema.json"
grep -F '"relation_type":"cites"' "$TMP_ROOT/relation-schema.json" >/dev/null ||
  fail "v1.1 relation schema sidecar was not written"
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

GRAPHDB_STORAGE=local \
GRAPHDB_DATA_DIR="$DATA_DIR" \
GRAPHDB_PREFIX=compat \
GRAPHDB_MODE=reader \
GRAPHDB_INSTANCE_ID=compat-reader-v1 \
"$OLD_BIN" query tenant-from-v11 "$TMP_ROOT/document-query.json" >"$TMP_ROOT/v1.0-read.json"
grep -F '"v11-document"' "$TMP_ROOT/v1.0-read.json" >/dev/null ||
  fail "v1.0 could not read the v1.1 core entity"
grep -F '"v11-tail-after-snapshot"' "$TMP_ROOT/v1.0-read.json" >/dev/null ||
  fail "v1.0 could not replay the v1.1 commit tail after a snapshot"
grep -F '"__graphdb_labels"' "$TMP_ROOT/v1.0-read.json" >/dev/null ||
  fail "v1.0 did not preserve v1.1 labels in the compatible fields map"

echo "v1.0/v1.1 binary compatibility passed: $V1_TAG <-> working tree"
