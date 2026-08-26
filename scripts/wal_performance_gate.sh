#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <v1.1.5-runs-dir> <v1.2.0-runs-dir> <regression-report.json>" >&2
  exit 2
fi

BASELINE_DIR="$1"
CANDIDATE_DIR="$2"
REGRESSION_REPORT="$3"
REPORT_PATH="${GRAPHDB_TEST_WAL_PERFORMANCE_REPORT:-}"

python3 - "$BASELINE_DIR" "$CANDIDATE_DIR" "$REGRESSION_REPORT" "$REPORT_PATH" <<'PY'
import json
import pathlib
import statistics
import sys

baseline_dir = pathlib.Path(sys.argv[1])
candidate_dir = pathlib.Path(sys.argv[2])
regression_path = pathlib.Path(sys.argv[3])
output_path = pathlib.Path(sys.argv[4]) if sys.argv[4] else None

def load_runs(root):
    paths = sorted(root.glob("*/summary.json"))
    if not paths and (root / "summary.json").exists():
        paths = [root / "summary.json"]
    return paths, [json.loads(path.read_text()) for path in paths]

baseline_paths, baseline = load_runs(baseline_dir)
candidate_paths, candidate = load_runs(candidate_dir)
regression = json.loads(regression_path.read_text())
failures = []

def check(condition, message):
    if not condition:
        failures.append(message)

def validate_run(report, label, path, enforce_candidate_limits):
    config = report.get("configuration", {})
    measurement = report.get("measurement", {})
    result = report.get("results", {})
    check(report.get("schema_version") == 1, f"{label} {path}: schema_version must be 1")
    check(report.get("kind") == "local_wal_ingest_performance", f"{label} {path}: wrong kind")
    check(report.get("success") is True, f"{label} {path}: run did not succeed")
    check(config.get("duration_ms") == 1_800_000, f"{label} {path}: configured duration must be exactly 30m")
    check(result.get("elapsed_ms", 0) >= config.get("duration_ms", 0), f"{label} {path}: observed elapsed time is below the configured duration")
    check(config.get("tenants") == 8 and config.get("collectors") == 16, f"{label} {path}: topology must be 8 tenants / 16 collectors")
    check(config.get("writers_per_tenant") == 32, f"{label} {path}: writers_per_tenant must be 32")
    check(config.get("cpu_limit") == 8 and config.get("memory_limit_bytes") == 8 * 1024**3, f"{label} {path}: writer limits must be 8 CPU / 8GiB")
    check(config.get("batch_size") == 200 and config.get("mutations_per_group") == 3 and config.get("working_set") == 20_000, f"{label} {path}: workload must use 200 groups/batch, 3 mutations/group, and a 20000-group working set")
    check(config.get("ingest_mode") == "wal" and config.get("metadata_mode") == "segment", f"{label} {path}: WAL/segment mode required")
    check(config.get("coordination") == "local" and config.get("durability") == "sync", f"{label} {path}: local/sync required")
    check(config.get("start_at_unix_ms", 0) > 0, f"{label} {path}: synchronized workload start is required")
    check(measurement.get("rss") == "/proc/1/status:VmRSS" and measurement.get("cpu") == "/sys/fs/cgroup/cpu.stat:usage_usec", f"{label} {path}: process RSS and cgroup CPU sources are required")
    check(result.get("backpressured_batches") == 0, f"{label} {path}: benchmark run observed 429")
    check(result.get("committed_batches") == result.get("scheduled_batches"), f"{label} {path}: not every scheduled batch committed")
    check(result.get("committed_mutations") == result.get("expected_committed_mutations"), f"{label} {path}: a committed batch lost or failed logical mutations")
    if enforce_candidate_limits:
        check(result.get("committed_mutations_per_second", 0) >= 10_000, f"candidate {path}: throughput below 10000 mutations/s")
        check(result.get("accepted_p95_ms", 10**9) <= 20, f"candidate {path}: accepted p95 above 20ms")
        check(result.get("accepted_p99_ms", 10**9) <= 250, f"candidate {path}: accepted p99 above 250ms")
        check(result.get("committed_p95_ms", 10**9) <= 8_000, f"candidate {path}: committed p95 above 8s")
        check(result.get("committed_p99_ms", 10**9) <= 15_000, f"candidate {path}: committed p99 above 15s")
        check(0 < result.get("rss_peak_bytes", 0) <= 7 * 1024**3, f"candidate {path}: RSS missing or above 7GiB")
        check(result.get("cpu_usec_per_1000_mutations", 0) > 0, f"candidate {path}: CPU evidence missing")

check(len(baseline) == 5, f"expected 5 v1.1.5 runs, found {len(baseline)}")
check(len(candidate) == 5, f"expected 5 v1.2.0 runs, found {len(candidate)}")
for path, report in zip(baseline_paths, baseline):
    validate_run(report, "baseline", path, False)
for path, report in zip(candidate_paths, candidate):
    validate_run(report, "candidate", path, True)

if baseline and candidate:
    check(len({report.get("commit") for report in baseline}) == 1, "baseline runs are not bound to one commit")
    check(len({report.get("commit") for report in candidate}) == 1, "candidate runs are not bound to one commit")
    check(baseline[0].get("commit") != candidate[0].get("commit"), "baseline and candidate commits must differ")
    baseline_throughput = [report["results"]["committed_mutations_per_second"] for report in baseline]
    candidate_throughput = [report["results"]["committed_mutations_per_second"] for report in candidate]
    baseline_rss = [report["results"]["rss_peak_bytes"] for report in baseline]
    candidate_rss = [report["results"]["rss_peak_bytes"] for report in candidate]
    baseline_cpu = [report["results"]["cpu_usec_per_1000_mutations"] for report in baseline]
    candidate_cpu = [report["results"]["cpu_usec_per_1000_mutations"] for report in candidate]
    median_baseline_throughput = statistics.median(baseline_throughput)
    median_candidate_throughput = statistics.median(candidate_throughput)
    throughput_ratio = median_candidate_throughput / median_baseline_throughput if median_baseline_throughput else 0
    variation = (max(candidate_throughput) - min(candidate_throughput)) / median_candidate_throughput if median_candidate_throughput else 1
    rss_ratio = statistics.median(candidate_rss) / statistics.median(baseline_rss) if statistics.median(baseline_rss) else 0
    cpu_ratio = statistics.median(candidate_cpu) / statistics.median(baseline_cpu) if statistics.median(baseline_cpu) else 0
    check(throughput_ratio >= 1.50, f"median throughput ratio {throughput_ratio:.3f} is below 1.50")
    check(variation <= 0.05, f"candidate throughput variation {variation:.3%} exceeds 5%")
    check(0 < rss_ratio <= 1.10, f"median RSS ratio {rss_ratio:.3f} exceeds 1.10 or lacks evidence")
    check(0 < cpu_ratio <= 0.75, f"median CPU/1000 ratio {cpu_ratio:.3f} exceeds 0.75 or lacks evidence")
else:
    median_baseline_throughput = median_candidate_throughput = throughput_ratio = variation = rss_ratio = cpu_ratio = 0

check(regression.get("schema_version") == 1 and regression.get("success") is True, "regression report is missing or unsuccessful")
check(regression.get("direct_write_ratio", 10) <= 1.10, "direct write regression exceeds 10%")
check(regression.get("query_ratio", 10) <= 1.10, "query regression exceeds 10%")
if baseline and candidate:
    check(regression.get("baseline_commit") == baseline[0].get("commit"), "regression baseline commit does not match WAL baseline runs")
    check(regression.get("candidate_commit") == candidate[0].get("commit"), "regression candidate commit does not match WAL candidate runs")

summary = {
    "schema_version": 1,
    "kind": "local_wal_ingest_performance_gate",
    "success": not failures,
    "baseline_commit": baseline[0].get("commit") if baseline else "",
    "candidate_commit": candidate[0].get("commit") if candidate else "",
    "valid_baseline_runs": len(baseline),
    "valid_candidate_runs": len(candidate),
    "median_baseline_mutations_per_second": median_baseline_throughput,
    "median_candidate_mutations_per_second": median_candidate_throughput,
    "throughput_ratio": throughput_ratio,
    "candidate_throughput_variation": variation,
    "rss_ratio": rss_ratio,
    "cpu_per_1000_mutations_ratio": cpu_ratio,
    "direct_write_ratio": regression.get("direct_write_ratio"),
    "query_ratio": regression.get("query_ratio"),
    "failures": failures,
}
encoded = json.dumps(summary, indent=2) + "\n"
if output_path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(encoded)
print(encoded, end="")
if failures:
    raise SystemExit(1)
PY
