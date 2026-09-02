#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROJECT="${GRAPHDB_COLD_READER_PROJECT:-graphdb-cold-readers}"
export GRAPHDB_PORT="${GRAPHDB_PORT:-38880}"
export GRAPHDB_READER_PORT="${GRAPHDB_READER_PORT:-38881}"
export GRAPHDB_READER2_PORT="${GRAPHDB_READER2_PORT:-38882}"
export GRAPHDB_READER3_PORT="${GRAPHDB_READER3_PORT:-38883}"
export RUSTFS_API_PORT="${RUSTFS_API_PORT:-39800}"
export GRAPHDB_READER_CATCHUP_TIMEOUT="${GRAPHDB_READER_CATCHUP_TIMEOUT:-10s}"
export GRAPHDB_READ_OBJECT_MAX_CONCURRENT="${GRAPHDB_READ_OBJECT_MAX_CONCURRENT:-8}"
export GRAPHDB_READ_OBJECT_SINGLEFLIGHT="${GRAPHDB_READ_OBJECT_SINGLEFLIGHT:-true}"
export GRAPHDB_FAULT_OBJECT_READ_DELAY="${GRAPHDB_FAULT_OBJECT_READ_DELAY:-25ms}"

WRITER_URL="http://127.0.0.1:$GRAPHDB_PORT"
READERS=(
  "http://127.0.0.1:$GRAPHDB_READER_PORT"
  "http://127.0.0.1:$GRAPHDB_READER2_PORT"
  "http://127.0.0.1:$GRAPHDB_READER3_PORT"
)

cleanup() {
  if [[ "${GRAPHDB_COLD_READER_CLEANUP:-1}" == "1" ]]; then
    docker compose -p "$PROJECT" --profile scale-readers -f docker-compose.rustfs.yml down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_health() {
  local url="$1"
  for _ in $(seq 1 90); do
    if curl -fsS --max-time 2 "$url/v1/health" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "health check timed out for $url" >&2
  return 1
}

json_number() {
  local field="$1"
  sed -n "s/.*\"$field\":\\([0-9][0-9]*\\).*/\\1/p" | head -n 1
}

if [[ "${GRAPHDB_RELEASE_USE_PREBUILT_IMAGE:-0}" == "1" ]]; then
  docker compose -p "$PROJECT" --profile scale-readers -f docker-compose.rustfs.yml up -d --no-build
else
  docker compose -p "$PROJECT" --profile scale-readers -f docker-compose.rustfs.yml up -d --build
fi
wait_health "$WRITER_URL"
for reader in "${READERS[@]}"; do
  wait_health "$reader"
done

tenant="${GRAPHDB_COLD_READER_TENANT:-cold-readers-$(date +%s%N)}"
id="host:cold-$(date +%s%N)"
body=$(printf '{"mutations":{"upsert_entities":[{"id":"%s","kind":"host","fields":{"hostname":"cold-reader-check"}}]}}' "$id")
commit=$(curl -fsS --max-time 20 -X POST "$WRITER_URL/v1/commits" \
  -H "X-Tenant-ID: $tenant" \
  -H 'Content-Type: application/json' \
  -d "$body")
version="$(printf '%s' "$commit" | json_number readable_version)"
if [[ -z "$version" ]]; then
  version="$(printf '%s' "$commit" | json_number version)"
fi
if [[ -z "$version" || "$version" == "0" ]]; then
  echo "commit did not return a readable version: $commit" >&2
  exit 1
fi

check_reader() {
  local reader="$1"
  local entity
  entity=$(curl -fsS --max-time 30 "$reader/v1/entities/$id?min_version=$version" -H "X-Tenant-ID: $tenant")
  if [[ "$entity" != *"\"version\":$version"* || "$entity" != *"$id"* ]]; then
    echo "unexpected entity response from $reader: $entity" >&2
    return 1
  fi

  local query
  query=$(curl -fsS --max-time 30 -X POST "$reader/v1/query" \
    -H "X-Tenant-ID: $tenant" \
    -H "X-GraphDB-Min-Version: $version" \
    -H 'Content-Type: application/json' \
    -d '{"op":"match","kind":"host","where":[{"field":"hostname","op":"eq","value":"cold-reader-check"}],"limit":1}')
  if [[ "$query" != *"\"version\":$version"* || "$query" != *"$id"* ]]; then
    echo "unexpected query response from $reader: $query" >&2
    return 1
  fi

  local metrics
  metrics=$(curl -fsS --max-time 10 "$reader/metrics")
  if [[ "$metrics" != *"graphdb_reader_catchup_total"* || "$metrics" != *"graphdb_object_store_operation_seconds"* ]]; then
    echo "missing reader catchup/object-store metrics from $reader" >&2
    return 1
  fi
}

pids=()
for reader in "${READERS[@]}"; do
  check_reader "$reader" &
  pids+=("$!")
done
for pid in "${pids[@]}"; do
  wait "$pid"
done

echo "PASS cold reader stateless check tenant=$tenant version=$version readers=${READERS[*]} read_delay=$GRAPHDB_FAULT_OBJECT_READ_DELAY read_object_max_concurrent=$GRAPHDB_READ_OBJECT_MAX_CONCURRENT"
