#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <v1.1.5-worktree> [candidate-worktree]" >&2
  exit 2
fi

BASELINE="$(cd "$1" && pwd)"
CANDIDATE="$(cd "${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}" && pwd)"
OUTPUT="${GRAPHDB_TEST_PERFORMANCE_REGRESSION_REPORT:-$CANDIDATE/artifacts/performance-regression.json}"
IMAGE="${GRAPHDB_GO_IMAGE:-golang:1.25-bookworm}"
DOCKER_CONTEXT="${GRAPHDB_DOCKER_CONTEXT:-orbstack}"
DOCKER=(docker --context "$DOCKER_CONTEXT")
RUN_DIR="$(mktemp -d)"
trap 'rm -rf "$RUN_DIR"' EXIT
BASELINE_COMMIT="${GRAPHDB_PERF_BASELINE_COMMIT:-}"
if [[ -z "$BASELINE_COMMIT" ]]; then
  BASELINE_COMMIT="$(git -C "$BASELINE" rev-parse HEAD)"
fi
CANDIDATE_COMMIT="$(git -C "$CANDIDATE" rev-parse HEAD)"

run_benchmarks() {
  local source="$1"
  local output="$2"
  local cache="$RUN_DIR/cache-$(basename "$output")"
  mkdir -p "$cache/go-build" "$cache/go-mod"
  "${DOCKER[@]}" run --rm --platform linux/arm64 \
    -v "$source:/workspace:ro" \
    -v "$cache/go-build:/tmp/go-build" \
    -v "$cache/go-mod:/tmp/go-mod" \
    -e GOCACHE=/tmp/go-build \
    -e GOMODCACHE=/tmp/go-mod \
    -w /workspace "$IMAGE" sh -ceu '
      go test -mod=readonly ./internal/storage -run "^$" -bench "^BenchmarkIngestWriteModes/direct_per_request$" -benchtime=100x -benchmem -count=5
      go test -mod=readonly ./internal/query -run "^$" -bench "^(BenchmarkMaterializedKindPage|BenchmarkMaterializedFieldIndexRangePage)$" -benchtime=100x -benchmem -count=5
    ' > "$output"
}

run_benchmarks "$BASELINE" "$RUN_DIR/baseline.txt"
run_benchmarks "$CANDIDATE" "$RUN_DIR/candidate.txt"

mkdir -p "$(dirname "$OUTPUT")"
python3 - "$RUN_DIR/baseline.txt" "$RUN_DIR/candidate.txt" "$OUTPUT" \
  "$BASELINE_COMMIT" "$CANDIDATE_COMMIT" <<'PY'
import json
import pathlib
import re
import statistics
import sys

line_re = re.compile(r"^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([0-9.]+)\s+ns/op(?:\s|$)")

def parse(path):
    values = {}
    for line in pathlib.Path(path).read_text().splitlines():
        match = line_re.match(line)
        if match:
            values.setdefault(match.group(1), []).append(float(match.group(2)))
    return {name: statistics.median(samples) for name, samples in values.items() if len(samples) == 5}

baseline = parse(sys.argv[1])
candidate = parse(sys.argv[2])
direct_name = "BenchmarkIngestWriteModes/direct_per_request"
query_names = ["BenchmarkMaterializedKindPage", "BenchmarkMaterializedFieldIndexRangePage"]
missing = [name for name in [direct_name, *query_names] if name not in baseline or name not in candidate]
direct_ratio = candidate.get(direct_name, 0) / baseline.get(direct_name, 1)
query_ratios = {name: candidate.get(name, 0) / baseline.get(name, 1) for name in query_names}
query_ratio = max(query_ratios.values(), default=0)
report = {
    "schema_version": 1,
    "kind": "direct_write_query_performance_regression",
    "success": not missing and direct_ratio <= 1.10 and query_ratio <= 1.10,
    "baseline_commit": sys.argv[4],
    "candidate_commit": sys.argv[5],
    "direct_write_ratio": direct_ratio,
    "query_ratio": query_ratio,
    "query_ratios": query_ratios,
    "baseline_ns_per_op": baseline,
    "candidate_ns_per_op": candidate,
    "missing_benchmarks": missing,
}
pathlib.Path(sys.argv[3]).write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
if not report["success"]:
    raise SystemExit(1)
PY
