#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_TAG="v1.1.5"
RUNS=5
PORT="${WAL_PERF_PORT:-39880}"
CPUS=8
MEMORY=8g
OUTPUT_DIR="${WAL_PERF_MATRIX_OUTPUT_DIR:-$ROOT/artifacts/wal-performance}"
SCRATCH="$(mktemp -d)"
BASELINE_SOURCE="$SCRATCH/baseline-source"
BASELINE_IMAGE="graphdb-wal-perf-baseline:${BASELINE_TAG//\//-}"
CANDIDATE_IMAGE="graphdb-wal-perf-candidate:$(git -C "$ROOT" rev-parse --short HEAD)"
CONTAINER="graphdb-wal-perf-writer"
DOCKER_CONTEXT="${GRAPHDB_DOCKER_CONTEXT:-orbstack}"
DOCKER=(docker --context "$DOCKER_CONTEXT")

if [[ -n "$(git -C "$ROOT" status --porcelain)" ]]; then
  echo "WAL performance matrix requires a clean, commit-bound worktree" >&2
  exit 2
fi
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "WAL performance output already exists: $OUTPUT_DIR" >&2
  exit 2
fi

cleanup_container() {
  "${DOCKER[@]}" rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
cleanup() {
  cleanup_container
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

HOST_CPUS="$("${DOCKER[@]}" info --format '{{.NCPU}}')"
HOST_MEMORY_BYTES="$("${DOCKER[@]}" info --format '{{.MemTotal}}')"
if (( HOST_CPUS < CPUS || HOST_MEMORY_BYTES < 8 * 1024 * 1024 * 1024 )); then
  echo "OrbStack host must expose at least 8 CPU and 8GiB memory" >&2
  exit 2
fi
mkdir -p "$BASELINE_SOURCE" "$OUTPUT_DIR/baseline" "$OUTPUT_DIR/candidate"
printf 'docker_context=%s\nhost_cpus=%s\nhost_memory_bytes=%s\n' \
  "$DOCKER_CONTEXT" "$HOST_CPUS" "$HOST_MEMORY_BYTES" > "$OUTPUT_DIR/host.txt"
git -C "$ROOT" archive "$BASELINE_TAG" | tar -x -C "$BASELINE_SOURCE"

git -C "$ROOT" archive "$BASELINE_TAG" | "${DOCKER[@]}" build \
  --build-arg VERSION="$BASELINE_TAG" \
  --build-arg COMMIT="$(git -C "$ROOT" rev-list -n 1 "$BASELINE_TAG")" \
  -t "$BASELINE_IMAGE" -
"${DOCKER[@]}" build \
  --build-arg VERSION="v1.2.0-candidate" \
  --build-arg COMMIT="$(git -C "$ROOT" rev-parse HEAD)" \
  -t "$CANDIDATE_IMAGE" "$ROOT"

wait_health() {
  local url="$1"
  for _ in $(seq 1 120); do
    if curl -fsS --max-time 2 "$url/v1/health" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "health check timed out for $url" >&2
  return 1
}

run_profile() {
  local profile="$1"
  local image="$2"
  local run_index="$3"
  local tested_commit="$4"
  local run_dir="$SCRATCH/$profile-$run_index"

  cleanup_container
  "${DOCKER[@]}" run -d --name "$CONTAINER" \
    --platform linux/arm64 \
    --cpus "$CPUS" \
    --memory "$MEMORY" \
    -p "$PORT:8080" \
    -e GRAPHDB_ADDR=:8080 \
    -e GRAPHDB_MODE=writer \
    -e GRAPHDB_STORAGE=local \
    -e GRAPHDB_DATA_DIR=/var/lib/graphdb \
    -e GRAPHDB_PREFIX=graphdb \
    -e GRAPHDB_COORDINATION=local \
    -e GRAPHDB_INGEST_MODE=wal \
    -e GRAPHDB_INGEST_METADATA_MODE=segment \
    -e GRAPHDB_INGEST_WAL_DURABILITY=sync \
    "$image" >/dev/null
  wait_health "http://127.0.0.1:$PORT"
  local run_status=0
  RUN_LABEL="$profile-$run_index" \
  RUN_DURATION=30m \
  WRITER_CONTAINER="$CONTAINER" \
  WRITER_URL="http://host.docker.internal:$PORT" \
  WAL_PERF_TENANTS=8 \
  WAL_PERF_COLLECTORS=16 \
  WAL_PERF_WRITERS_PER_TENANT=32 \
  WAL_PERF_BATCH_SIZE=200 \
  WAL_PERF_WORKING_SET=20000 \
  WAL_PERF_TESTED_COMMIT="$tested_commit" \
  WAL_PERF_OUTPUT_DIR="$run_dir" \
    "$ROOT/scripts/wal_performance_run.sh" || run_status=$?
  mkdir -p "$OUTPUT_DIR/$profile/run-$run_index"
  for evidence in summary.json runtime-config.txt rss-current-bytes.txt; do
    if [[ -f "$run_dir/$evidence" ]]; then
      cp "$run_dir/$evidence" "$OUTPUT_DIR/$profile/run-$run_index/$evidence"
    fi
  done
  if [[ -d "$run_dir/tenants" ]]; then
    cp -R "$run_dir/tenants" "$OUTPUT_DIR/$profile/run-$run_index/tenants"
  fi
  cleanup_container
  return "$run_status"
}

BASELINE_COMMIT="$(git -C "$ROOT" rev-list -n 1 "$BASELINE_TAG")"
CANDIDATE_COMMIT="$(git -C "$ROOT" rev-parse HEAD)"
for ((run_index = 1; run_index <= RUNS; run_index++)); do
  run_profile baseline "$BASELINE_IMAGE" "$run_index" "$BASELINE_COMMIT"
  run_profile candidate "$CANDIDATE_IMAGE" "$run_index" "$CANDIDATE_COMMIT"
done

GRAPHDB_TEST_PERFORMANCE_REGRESSION_REPORT="$OUTPUT_DIR/regression.json" \
GRAPHDB_PERF_BASELINE_COMMIT="$BASELINE_COMMIT" \
  "$ROOT/scripts/performance_regression_gate.sh" "$BASELINE_SOURCE" "$ROOT"
GRAPHDB_TEST_WAL_PERFORMANCE_REPORT="$OUTPUT_DIR/gate.json" \
  "$ROOT/scripts/wal_performance_gate.sh" \
    "$OUTPUT_DIR/baseline" "$OUTPUT_DIR/candidate" "$OUTPUT_DIR/regression.json"

echo "WAL performance matrix evidence: $OUTPUT_DIR/gate.json"
