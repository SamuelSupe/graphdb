#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRITER_URL="${WRITER_URL:-http://127.0.0.1:8080}"
WRITER_CONTAINER="${WRITER_CONTAINER:?set WRITER_CONTAINER to the GraphDB writer container}"
CLIENT_NETWORK="${WAL_PERF_CLIENT_NETWORK:-container}"
IMAGE="${GRAPHDB_GO_IMAGE:-golang:1.25-bookworm}"
DOCKER_CONTEXT="${GRAPHDB_DOCKER_CONTEXT:-orbstack}"
DOCKER=(docker --context "$DOCKER_CONTEXT")
declare -a CLIENT_NETWORK_ARGS=()
case "$CLIENT_NETWORK" in
  container)
    CLIENT_NETWORK_ARGS=(--network "container:$WRITER_CONTAINER")
    ;;
  bridge)
    ;;
  *)
    echo "WAL_PERF_CLIENT_NETWORK must be container or bridge" >&2
    exit 2
    ;;
esac
RUN_LABEL="${RUN_LABEL:-candidate}"
RUN_DURATION="${RUN_DURATION:-30m}"
TENANTS="${WAL_PERF_TENANTS:-8}"
COLLECTORS="${WAL_PERF_COLLECTORS:-16}"
WRITERS_PER_TENANT="${WAL_PERF_WRITERS_PER_TENANT:-32}"
BATCH_SIZE="${WAL_PERF_BATCH_SIZE:-200}"
WORKING_SET="${WAL_PERF_WORKING_SET:-20000}"
HTTP_TIMEOUT="${WAL_PERF_HTTP_TIMEOUT:-2m}"
START_DELAY_SECONDS="${WAL_PERF_START_DELAY_SECONDS:-15}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_SLUG="$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]')"
OUTPUT_DIR="${WAL_PERF_OUTPUT_DIR:-$ROOT/performance-runs/$RUN_LABEL-$RUN_ID}"
TESTED_COMMIT="${WAL_PERF_TESTED_COMMIT:-$(git -C "$ROOT" rev-parse HEAD)}"

if (( TENANTS <= 0 || COLLECTORS <= 0 || COLLECTORS % TENANTS != 0 )); then
  echo "WAL_PERF_COLLECTORS must be a positive multiple of WAL_PERF_TENANTS" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR/tenants"
"${DOCKER[@]}" exec "$WRITER_CONTAINER" sh -c '
  printf "%s\n" \
    "GRAPHDB_INGEST_MODE=$GRAPHDB_INGEST_MODE" \
    "GRAPHDB_INGEST_METADATA_MODE=$GRAPHDB_INGEST_METADATA_MODE" \
    "GRAPHDB_COORDINATION=$GRAPHDB_COORDINATION" \
    "GRAPHDB_INGEST_WAL_DURABILITY=$GRAPHDB_INGEST_WAL_DURABILITY"
' > "$OUTPUT_DIR/runtime-config.txt"
"${DOCKER[@]}" inspect --format 'DOCKER_NANO_CPUS={{.HostConfig.NanoCpus}}' "$WRITER_CONTAINER" >> "$OUTPUT_DIR/runtime-config.txt"
"${DOCKER[@]}" inspect --format 'DOCKER_MEMORY_BYTES={{.HostConfig.Memory}}' "$WRITER_CONTAINER" >> "$OUTPUT_DIR/runtime-config.txt"
grep -Fx 'GRAPHDB_INGEST_MODE=wal' "$OUTPUT_DIR/runtime-config.txt" >/dev/null
grep -Fx 'GRAPHDB_INGEST_METADATA_MODE=segment' "$OUTPUT_DIR/runtime-config.txt" >/dev/null
grep -Fx 'GRAPHDB_COORDINATION=local' "$OUTPUT_DIR/runtime-config.txt" >/dev/null
grep -Fx 'GRAPHDB_INGEST_WAL_DURABILITY=sync' "$OUTPUT_DIR/runtime-config.txt" >/dev/null
grep -Fx 'DOCKER_NANO_CPUS=8000000000' "$OUTPUT_DIR/runtime-config.txt" >/dev/null
grep -Fx 'DOCKER_MEMORY_BYTES=8589934592' "$OUTPUT_DIR/runtime-config.txt" >/dev/null
printf 'CLIENT_NETWORK_MODE=%s\nWRITER_URL=%s\n' "$CLIENT_NETWORK" "$WRITER_URL" >> "$OUTPUT_DIR/runtime-config.txt"

"${DOCKER[@]}" run --rm --platform linux/arm64 \
  --mount type=volume,source=graphdb-wal-perf-go-mod,destination=/go/pkg/mod \
  --mount type=volume,source=graphdb-wal-perf-go-build,destination=/root/.cache/go-build \
  -v "$ROOT:/workspace:ro" \
  -v "$OUTPUT_DIR:/evidence" \
  -e CGO_ENABLED=0 \
  -w /workspace "$IMAGE" \
  go build -mod=readonly -o /evidence/loadtest ./tools/loadtest

read_cpu_usec() {
  "${DOCKER[@]}" exec "$WRITER_CONTAINER" sh -c "awk '\$1 == \"usage_usec\" { print \$2 }' /sys/fs/cgroup/cpu.stat"
}

read_rss_bytes() {
  "${DOCKER[@]}" exec "$WRITER_CONTAINER" sh -c 'awk '\''$1 == "VmRSS:" { printf "%.0f\n", $2 * 1024 }'\'' /proc/1/status'
}

CPU_START="$(read_cpu_usec)"
RSS_SAMPLES="$OUTPUT_DIR/rss-current-bytes.txt"
SAMPLE_STOP="$OUTPUT_DIR/rss-sampler.stop"
rm -f "$SAMPLE_STOP"
(
  while [[ ! -e "$SAMPLE_STOP" ]]; do
    read_rss_bytes >> "$RSS_SAMPLES" || true
    sleep 1
  done
) &
SAMPLER_PID=$!

declare -a PIDS=()
COLLECTORS_PER_TENANT=$((COLLECTORS / TENANTS))
START_AT_UNIX_MS="$(python3 -c 'import sys, time; print(int((time.time() + int(sys.argv[1])) * 1000))' "$START_DELAY_SECONDS")"
for ((tenant_index = 0; tenant_index < TENANTS; tenant_index++)); do
  tenant="wal-perf-${RUN_LABEL}-${RUN_SLUG}-${tenant_index}"
  "${DOCKER[@]}" run --rm --platform linux/arm64 \
    "${CLIENT_NETWORK_ARGS[@]}" \
    -v "$OUTPUT_DIR:/evidence" \
    "$IMAGE" /evidence/loadtest \
    -base "$WRITER_URL" \
    -tenant "$tenant" \
    -writers "$WRITERS_PER_TENANT" \
    -readers 0 \
    -duration "$RUN_DURATION" \
    -collectors "$COLLECTORS_PER_TENANT" \
    -batch-size "$BATCH_SIZE" \
    -working-set "$WORKING_SET" \
    -start-at-unix-ms "$START_AT_UNIX_MS" \
    -timeout "$RUN_DURATION" \
    -http-timeout "$HTTP_TIMEOUT" \
    -allow-write-backpressure=true \
    -post-load-checks=false \
    -report-json "/evidence/tenants/tenant-${tenant_index}.json" \
    > "$OUTPUT_DIR/tenants/tenant-${tenant_index}.log" 2>&1 &
  PIDS+=("$!")
done

RUN_FAILED=0
for pid in "${PIDS[@]}"; do
  if ! wait "$pid"; then
    RUN_FAILED=1
  fi
done
touch "$SAMPLE_STOP"
wait "$SAMPLER_PID" || true
CPU_END="$(read_cpu_usec)"

python3 - "$OUTPUT_DIR" "$RUN_LABEL" "$RUN_DURATION" "$TENANTS" "$COLLECTORS" \
  "$BATCH_SIZE" "$WORKING_SET" "$WRITERS_PER_TENANT" "$CPU_START" "$CPU_END" \
  "$RUN_FAILED" "$TESTED_COMMIT" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
tenant_paths = sorted((root / "tenants").glob("tenant-*.json"))
reports = [json.loads(path.read_text()) for path in tenant_paths]

def duration_ms(value):
    units = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}
    for suffix in ("ms", "s", "m", "h"):
        if value.endswith(suffix):
            return int(float(value[:-len(suffix)]) * units[suffix])
    raise ValueError(f"unsupported duration {value!r}")

def metric(report, name):
    for item in report.get("metrics", []):
        if item.get("name") == name:
            return item
    return {}

committed_mutations = sum(report.get("results", {}).get("committed_mutations", 0) for report in reports)
committed_batches = sum(report.get("results", {}).get("committed_batches", 0) for report in reports)
scheduled_batches = sum(report.get("results", {}).get("scheduled_batches", 0) for report in reports)
backpressured_batches = sum(report.get("results", {}).get("backpressured_batches", 0) for report in reports)
start_at_values = {report.get("workload", {}).get("start_at_unix_ms", 0) for report in reports}
elapsed_ms = max((report.get("elapsed_ms", 0) for report in reports), default=0)
accepted = [metric(report, "ingest") for report in reports]
committed = [metric(report, "ingest-committed") for report in reports]
rss_path = root / "rss-current-bytes.txt"
rss_samples = [int(line) for line in rss_path.read_text().splitlines() if line.strip()] if rss_path.exists() else []
cpu_usec = max(0, int(sys.argv[10]) - int(sys.argv[9]))
cpu_per_1000 = (cpu_usec / committed_mutations * 1000) if committed_mutations else 0
throughput = (committed_mutations / (elapsed_ms / 1000)) if elapsed_ms else 0
expected_duration_ms = duration_ms(sys.argv[3])
expected_committed_mutations = committed_batches * int(sys.argv[6]) * 3

success = (
    len(reports) == int(sys.argv[4])
    and all(report.get("schema_version") == 2 and report.get("success") is True for report in reports)
    and len(start_at_values) == 1
    and next(iter(start_at_values), 0) > 0
    and elapsed_ms >= expected_duration_ms
    and committed_mutations > 0
    and committed_mutations == expected_committed_mutations
    and committed_batches == scheduled_batches
    and int(sys.argv[11]) == 0
)

summary = {
    "schema_version": 1,
    "kind": "local_wal_ingest_performance",
    "success": success,
    "label": sys.argv[2],
    "commit": sys.argv[12],
    "configuration": {
        "duration_ms": expected_duration_ms,
        "tenants": int(sys.argv[4]),
        "collectors": int(sys.argv[5]),
        "batch_size": int(sys.argv[6]),
        "mutations_per_group": 3,
        "working_set": int(sys.argv[7]),
        "writers_per_tenant": int(sys.argv[8]),
        "cpu_limit": 8,
        "memory_limit_bytes": 8 * 1024**3,
        "ingest_mode": "wal",
        "metadata_mode": "segment",
        "coordination": "local",
        "durability": "sync",
        "start_at_unix_ms": next(iter(start_at_values), 0),
    },
    "measurement": {
        "rss": "/proc/1/status:VmRSS",
        "cpu": "/sys/fs/cgroup/cpu.stat:usage_usec",
    },
    "results": {
        "elapsed_ms": elapsed_ms,
        "scheduled_batches": scheduled_batches,
        "committed_batches": committed_batches,
        "committed_mutations": committed_mutations,
        "expected_committed_mutations": expected_committed_mutations,
        "committed_mutations_per_second": throughput,
        "backpressured_batches": backpressured_batches,
        "accepted_p95_ms": max((item.get("p95_ms", 0) for item in accepted), default=0),
        "accepted_p99_ms": max((item.get("p99_ms", 0) for item in accepted), default=0),
        "committed_p95_ms": max((item.get("p95_ms", 0) for item in committed), default=0),
        "committed_p99_ms": max((item.get("p99_ms", 0) for item in committed), default=0),
        "rss_peak_bytes": max(rss_samples, default=0),
        "cpu_usage_usec": cpu_usec,
        "cpu_usec_per_1000_mutations": cpu_per_1000,
    },
    "tenant_reports": [str(path.relative_to(root)) for path in tenant_paths],
}
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["results"], indent=2))
if not success:
    raise SystemExit(1)
PY

echo "WAL performance evidence: $OUTPUT_DIR/summary.json"
