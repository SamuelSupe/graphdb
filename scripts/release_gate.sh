#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WRITER_URL="${WRITER_URL:-http://127.0.0.1:${GRAPHDB_PORT:-38080}}"
READER_URL="${READER_URL:-http://127.0.0.1:${GRAPHDB_READER_PORT:-38081}}"
RUSTFS_URL="${RUSTFS_URL:-http://127.0.0.1:${RUSTFS_API_PORT:-39000}}"
TENANT="release-gate-$(date +%s)"

cleanup() {
  if [[ "${KEEP_RELEASE_STACK:-0}" == "1" ]]; then
    return
  fi
  docker compose -f docker-compose.rustfs.yml down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

log() {
  printf '\n== %s ==\n' "$*"
}

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

wait_rustfs() {
  for _ in $(seq 1 60); do
    local status
    status="$(curl -sS --max-time 2 -o /dev/null -w '%{http_code}' "$RUSTFS_URL" || true)"
    if [[ "$status" != "000" && "$status" != "503" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "RustFS readiness timed out for $RUSTFS_URL" >&2
  return 1
}

expect_status() {
  local want="$1"
  local status
  shift
  status="$(curl -sS -o /tmp/graphdb-release-gate-response.json -w '%{http_code}' "$@")"
  if [[ "$status" != "$want" ]]; then
    echo "unexpected status $status, want $want" >&2
    cat /tmp/graphdb-release-gate-response.json >&2 || true
    return 1
  fi
}

expect_status_retry() {
  local want="$1"
  shift
  for _ in $(seq 1 30); do
    if expect_status "$want" "$@"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_task_success() {
  local tenant="$1"
  local task_id="$2"
  local response="/tmp/graphdb-release-gate-task.json"
  local status
  for _ in $(seq 1 180); do
    curl -fsS --max-time 5 "$WRITER_URL/v1/tasks/$task_id" \
      -H "X-Tenant-ID: $tenant" -o "$response"
    status="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("status",""))' "$response")"
    case "$status" in
      succeeded)
        python3 -c 'import json,sys; task=json.load(open(sys.argv[1])); result=task.get("result",{}); assert result.get("recoverable") is True and result.get("status") == "passed", task' "$response"
        return 0
        ;;
      failed | canceled)
        cat "$response" >&2
        return 1
        ;;
    esac
    sleep 1
  done
  echo "task $task_id did not complete" >&2
  return 1
}

if [[ "${RELEASE_GATE_SKIP_STATIC:-0}" != "1" ]]; then
  log "workspace hygiene"
  scripts/check_workspace_hygiene.sh
  log "1.1 feature-freeze and naming contract"
  scripts/check_release_freeze.sh
  log "unit, vet, race, and v1.0 compatibility tests"
  go test -mod=readonly ./...
  go vet -mod=readonly ./...
  go test -mod=readonly -race ./...
  scripts/compatibility_v1_0_v1_1.sh
fi

if [[ "${RELEASE_GATE_VERIFY_ONLY:-0}" == "1" ]]; then
  log "static release gate passed"
  exit 0
fi

log "start RustFS release stack"
if [[ "${GRAPHDB_RELEASE_USE_PREBUILT_IMAGE:-0}" == "1" ]]; then
  docker compose -f docker-compose.rustfs.yml up -d --no-build
else
  docker compose -f docker-compose.rustfs.yml up -d --build
fi
wait_rustfs
wait_health "$WRITER_URL"
wait_health "$READER_URL"

log "S3 compatibility against RustFS"
GRAPHDB_MINIO_INTEGRATION=1 \
S3_ENDPOINT="$RUSTFS_URL" \
S3_BUCKET="${S3_BUCKET:-graphdb}" \
S3_PATH_STYLE=true \
S3_REGION="${S3_REGION:-us-east-1}" \
S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-graphdbadmin}" \
S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-graphdbsecret}" \
go test -mod=readonly ./internal/storage -run TestS3StoreIntegration -count=1

if [[ "${RUN_EXTERNAL_S3:-0}" == "1" ]]; then
  log "S3 compatibility against external endpoint"
  GRAPHDB_MINIO_INTEGRATION=1 go test -mod=readonly ./internal/storage -run TestS3StoreIntegration -count=1
fi

if [[ "${RUN_POSTGRES_CAS_INTEGRATION:-1}" == "1" ]]; then
  log "PostgreSQL coordinator and RustFS CAS integration"
  GRAPHDB_CAS_USE_EXISTING_RUSTFS=1 scripts/postgres_cas_gate.sh integration
fi

if [[ "${RUN_POSTGRES_CAS_STRESS:-0}" == "1" ]]; then
  log "PostgreSQL/RustFS 2-active-writer 20 QPS soak"
  GRAPHDB_CAS_USE_EXISTING_RUSTFS=1 \
  GRAPHDB_TEST_CAS_STRESS_DURATION="${GRAPHDB_TEST_CAS_STRESS_DURATION:-30m}" \
  GRAPHDB_TEST_CAS_STRESS_QPS="${GRAPHDB_TEST_CAS_STRESS_QPS:-20}" \
  GRAPHDB_TEST_CAS_STRESS_WRITERS="${GRAPHDB_TEST_CAS_STRESS_WRITERS:-2}" \
  scripts/postgres_cas_gate.sh soak
fi

log "end-to-end RustFS checks"
go run -mod=readonly ./tools/e2echeck -writer "$WRITER_URL" -reader "$READER_URL" -timeout "${E2E_TIMEOUT:-3m}"

log "load test"
go run -mod=readonly ./tools/loadtest \
  -base "$WRITER_URL" \
  -reader-base "$READER_URL" \
  -tenant "$TENANT-load" \
  -writers "${LOAD_WRITERS:-4}" \
  -readers "${LOAD_READERS:-8}" \
  -batches "${LOAD_BATCHES:-30}" \
  -batch-size "${LOAD_BATCH_SIZE:-20}" \
  -timeout "${LOAD_TIMEOUT:-3m}"

if [[ "${RUN_COLD_READER_SCALE:-1}" == "1" ]]; then
  log "cold reader scale and slow object-read check"
  scripts/rustfs_reader_stateless_check.sh
fi

log "freshness and repair smoke"
expect_status 200 -X POST "$WRITER_URL/v1/tenants" -H 'Content-Type: application/json' -d "{\"tenant_id\":\"$TENANT\"}"
expect_status 200 -X POST "$WRITER_URL/v1/commits" -H "X-Tenant-ID: $TENANT" -H 'Content-Type: application/json' -d '{"mutations":{"upsert_entities":[{"id":"host:rg","kind":"host","fields":{"name":"release-gate"}}]}}'
expect_status 200 "$READER_URL/v1/control/reader-freshness" -H "X-Tenant-ID: $TENANT"
expect_status 200 -X POST "$WRITER_URL/v1/control/repair" -H "X-Tenant-ID: $TENANT" -H 'Content-Type: application/json' -d '{"apply":true}'
expect_status 200 -X POST "$WRITER_URL/v1/control/recover" -H "X-Tenant-ID: $TENANT"

log "backup and restore drill"
expect_status 202 -X POST "$WRITER_URL/v1/tenants/$TENANT/restore-drill" \
  -H 'Content-Type: application/json' -d '{"cleanup":true}'
restore_drill_task="$(python3 -c 'import json; print(json.load(open("/tmp/graphdb-release-gate-response.json"))["id"])')"
wait_task_success "$TENANT" "$restore_drill_task"

log "reader restart freshness check"
docker compose -f docker-compose.rustfs.yml restart graphdb-reader
wait_health "$READER_URL"
expect_status 200 "$READER_URL/v1/control/reader-freshness" -H "X-Tenant-ID: $TENANT"

log "writer restart check"
docker compose -f docker-compose.rustfs.yml restart graphdb
wait_health "$WRITER_URL"
expect_status 200 "$WRITER_URL/v1/control/reader-freshness" -H "X-Tenant-ID: $TENANT"

if [[ "${RUN_OBJECT_STORE_OUTAGE:-1}" == "1" ]]; then
  log "object store outage check"
  docker compose -f docker-compose.rustfs.yml stop rustfs
  expect_status_retry 503 "$WRITER_URL/v1/readiness"
  expect_status_retry 503 "$READER_URL/v1/readiness"
  if curl -fsS --max-time 3 -X POST "$WRITER_URL/v1/commits" -H "X-Tenant-ID: $TENANT" -H 'Content-Type: application/json' -d '{"mutations":{"upsert_entities":[{"id":"host:outage","kind":"host"}]}}' >/tmp/graphdb-release-gate-outage.json; then
    echo "write unexpectedly succeeded while object store is stopped" >&2
    docker compose -f docker-compose.rustfs.yml start rustfs
    wait_health "$WRITER_URL"
    exit 1
  fi
  docker compose -f docker-compose.rustfs.yml start rustfs
  wait_rustfs
  wait_health "$WRITER_URL"
  wait_health "$READER_URL"
  expect_status_retry 200 "$WRITER_URL/v1/readiness"
  expect_status_retry 200 "$READER_URL/v1/readiness"
fi

log "cleanup release smoke tenant"
expect_status_retry 200 -X POST "$WRITER_URL/v1/tenants/$TENANT/purge?force=true"

log "release gate passed"
