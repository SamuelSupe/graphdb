#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-integration}"
PROJECT="${GRAPHDB_CAS_COMPOSE_PROJECT:-graphdb-cas-gate}"
POSTGRES_PORT="${GRAPHDB_POSTGRES_PORT:-}"
RUSTFS_PORT="${RUSTFS_API_PORT:-39000}"
RUSTFS_URL="${RUSTFS_URL:-http://127.0.0.1:$RUSTFS_PORT}"
USE_EXISTING_RUSTFS="${GRAPHDB_CAS_USE_EXISTING_RUSTFS:-0}"
V1_TAG="${GRAPHDB_V1_COMPAT_TAG:-release_20260722_01}"
V1_BUILD_PARALLELISM="${GRAPHDB_COMPAT_BUILD_PARALLELISM:-2}"
V1_BUILD_ROOT=""

cleanup() {
  if [[ "${KEEP_COORDINATION_STACK:-0}" == "1" ]]; then
    return
  fi
  docker compose -p "$PROJECT" -f docker-compose.postgres.yml down -v >/dev/null 2>&1 || true
  if [[ "$USE_EXISTING_RUSTFS" != "1" ]]; then
    docker compose -p "$PROJECT" -f docker-compose.rustfs.yml down -v >/dev/null 2>&1 || true
  fi
  if [[ -n "$V1_BUILD_ROOT" ]]; then
    rm -rf "$V1_BUILD_ROOT"
  fi
}
trap cleanup EXIT

wait_rustfs() {
  for _ in $(seq 1 90); do
    status="$(curl -sS --max-time 2 -o /dev/null -w '%{http_code}' "$RUSTFS_URL" || true)"
    if [[ "$status" != "000" && "$status" != "503" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "RustFS readiness timed out for $RUSTFS_URL" >&2
  return 1
}

wait_postgres() {
  local ready_streak=0
  local probe=""
  for _ in $(seq 1 90); do
    probe="$(docker compose -p "$PROJECT" -f docker-compose.postgres.yml exec -T postgres \
      psql -U graphdb -d graphdb -Atqc 'SELECT 1' 2>/dev/null || true)"
    if [[ "$probe" == "1" ]]; then
      ready_streak=$((ready_streak + 1))
      if [[ "$ready_streak" -ge 3 ]]; then
        return 0
      fi
    else
      ready_streak=0
    fi
    sleep 1
  done
  echo "PostgreSQL readiness timed out" >&2
  return 1
}

cd "$ROOT"
if [[ -z "$POSTGRES_PORT" ]]; then
  POSTGRES_PORT="$(
    python3 -c 'import socket; sock = socket.socket(); sock.bind(("127.0.0.1", 0)); print(sock.getsockname()[1]); sock.close()'
  )"
fi
export GRAPHDB_POSTGRES_PORT="$POSTGRES_PORT"

if [[ -n "${GRAPHDB_TEST_CAS_STRESS_REPORT:-}" &&
  "$GRAPHDB_TEST_CAS_STRESS_REPORT" != /* ]]; then
  export GRAPHDB_TEST_CAS_STRESS_REPORT="$ROOT/$GRAPHDB_TEST_CAS_STRESS_REPORT"
fi
if [[ -n "${GRAPHDB_TEST_ROLLBACK_REPORT:-}" &&
  "$GRAPHDB_TEST_ROLLBACK_REPORT" != /* ]]; then
  export GRAPHDB_TEST_ROLLBACK_REPORT="$ROOT/$GRAPHDB_TEST_ROLLBACK_REPORT"
fi

case "$MODE" in
  integration | soak | rollback) ;;
  *)
    echo "usage: $0 integration|soak|rollback" >&2
    exit 2
    ;;
esac

if [[ "$MODE" == "soak" && -z "${GRAPHDB_TEST_V1_BINARY:-}" ]]; then
  [[ "$V1_BUILD_PARALLELISM" =~ ^[1-9][0-9]*$ ]]
  git rev-parse --verify "${V1_TAG}^{commit}" >/dev/null
  V1_BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/graphdb-cas-v1.XXXXXX")"
  mkdir -p "$V1_BUILD_ROOT/source"
  git archive "$V1_TAG" | tar -x -C "$V1_BUILD_ROOT/source"
  if [[ -n "${GRAPHDB_COMPAT_BUILD_IMAGE:-}" ]]; then
    docker run --rm \
      -v "$V1_BUILD_ROOT:/compat" \
      -w /compat/source \
      "$GRAPHDB_COMPAT_BUILD_IMAGE" \
      go build -p "$V1_BUILD_PARALLELISM" -mod=mod -trimpath \
        -o /compat/graphdb-v1.0 ./cmd/graphdb
  else
    (
      cd "$V1_BUILD_ROOT/source"
      go build -p "$V1_BUILD_PARALLELISM" -mod=mod -trimpath \
        -o "$V1_BUILD_ROOT/graphdb-v1.0" ./cmd/graphdb
    )
  fi
  export GRAPHDB_TEST_V1_BINARY="$V1_BUILD_ROOT/graphdb-v1.0"
fi
export GRAPHDB_TEST_V1_TAG="${GRAPHDB_TEST_V1_TAG:-$V1_TAG}"

if [[ "$USE_EXISTING_RUSTFS" != "1" ]]; then
  docker compose -p "$PROJECT" -f docker-compose.rustfs.yml up -d rustfs create-bucket
fi
docker compose -p "$PROJECT" -f docker-compose.postgres.yml up -d
wait_rustfs
wait_postgres

export GRAPHDB_TEST_POSTGRES_DSN="${GRAPHDB_TEST_POSTGRES_DSN:-postgres://graphdb:graphdb@127.0.0.1:$POSTGRES_PORT/graphdb?sslmode=disable}"
export GRAPHDB_MINIO_INTEGRATION=1
export S3_ENDPOINT="$RUSTFS_URL"
export S3_BUCKET="${S3_BUCKET:-graphdb}"
export S3_PATH_STYLE=true
export S3_REGION="${S3_REGION:-us-east-1}"
export S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-graphdbadmin}"
export S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-graphdbsecret}"

if [[ "$MODE" == "integration" ]]; then
  if [[ -n "${GRAPHDB_STORAGE_TEST_BIN:-}" ]]; then
    "$GRAPHDB_STORAGE_TEST_BIN" -test.run '^TestPostgresCoordinator' -test.count=1
  else
    go test -mod=readonly ./internal/storage -run '^TestPostgresCoordinator' -count=1
  fi
  exit 0
fi

if [[ "$MODE" == "rollback" ]]; then
  if [[ -n "${GRAPHDB_STORAGE_TEST_BIN:-}" ]]; then
    "$GRAPHDB_STORAGE_TEST_BIN" \
      -test.run '^TestPostgresCoordinatorS3RollbackDrill$' \
      -test.count=1 \
      -test.timeout=5m \
      -test.v
  else
    go test -mod=readonly ./internal/storage \
      -run '^TestPostgresCoordinatorS3RollbackDrill$' \
      -count=1 \
      -timeout=5m \
      -v
  fi
  exit 0
fi

export GRAPHDB_TEST_CAS_STRESS_DURATION="${GRAPHDB_TEST_CAS_STRESS_DURATION:-30m}"
export GRAPHDB_TEST_CAS_STRESS_QPS="${GRAPHDB_TEST_CAS_STRESS_QPS:-20}"
export GRAPHDB_TEST_CAS_STRESS_WRITERS="${GRAPHDB_TEST_CAS_STRESS_WRITERS:-2}"
if [[ -n "${GRAPHDB_STORAGE_TEST_BIN:-}" ]]; then
  "$GRAPHDB_STORAGE_TEST_BIN" \
    -test.run '^TestPostgresCoordinatorS3CASStress$' \
    -test.count=1 \
    -test.timeout=45m \
    -test.v
else
  go test -mod=readonly ./internal/storage -run '^TestPostgresCoordinatorS3CASStress$' -count=1 -timeout 45m -v
fi
