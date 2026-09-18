#!/usr/bin/env python3
"""Summarize retained benchmark evidence without treating noisy runs as passes."""
import argparse
import json
import math
from pathlib import Path
import statistics


def read(path):
    return json.loads(path.read_text())


def percentile(values, percent):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


def measurements(folder):
    load = read(folder / "measure.json")
    cold = read(folder / "cold.json")
    values = {"mixed_ops_s": load["mixed_operations_per_second"],
              "published_batches_s": load["published_batches_per_second"],
              "cold_p95_ms": cold["p95_ms"],
              "peak_rss_bytes": max([s["rss_hwm_bytes"] for s in cold.get("resources", [])] or [0])}
    if cold.get("startup_to_ready_ms"):
        values["startup_to_ready_ms"] = statistics.median(cold["startup_to_ready_ms"])
    for metric in load["metrics"]:
        if metric["name"] == "write-backpressure":
            values["write_backpressure_responses"] = metric["count"]
            continue
        for quantile in (50, 95, 99):
            values[f'{metric["name"]}_p{quantile}_ms'] = metric[f"p{quantile}_us"] / 1000
    maintenance = read(folder / "maintenance.json")["samples"]
    for name in ("export", "compact", "backup", "restore"):
        for quantile in (50, 95, 99):
            values[f"{name}_p{quantile}_ms"] = percentile([s[name + "_ms"] for s in maintenance], quantile)
    for stage in ("mixed", "maintenance"):
        resources = read(folder / f"resources-{stage}.json")
        values[stage + "_rss_bytes"] = resources["peak_rss_bytes"]
        values["peak_rss_bytes"] = max(values["peak_rss_bytes"], resources["lifetime_rss_hwm_bytes"])
        values[stage + "_cpu_cores"] = resources["cpu_cores_mean"]
        values[stage + "_fds"] = resources["peak_fds"]
        for name in ("read_bytes", "write_bytes", "rchar", "wchar"):
            values[f"{stage}_{name}"] = resources["io"][name]
        values[stage + "_fsyncs"] = resources["graphdb_sync"].get("graphdb_disk_sync_total", 0)
    return values, len(maintenance)


def summarize(values):
    mean = statistics.mean(values)
    return {"median": statistics.median(values), "min": min(values), "max": max(values),
            "cv": statistics.stdev(values) / mean if len(values) > 1 and mean else 0,
            "runs": values}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    root = args.evidence
    if (root / "reports").is_dir():
        root = root / "reports"
    run = read(root / "run.json")
    grouped, failures, counts = {}, [], {}
    for folder in sorted(root.glob("r*-*")):
        if not (folder / "complete.json").exists():
            failures.append({"case": folder.name, "detail": read(folder / "failure.json") if (folder / "failure.json").exists() else "incomplete"})
            continue
        _, size, mode, case = folder.name.split("-", 3)
        key = f"{size}/{mode}/{case}"
        metrics, sample_count = measurements(folder)
        counts.setdefault(key, []).append(sample_count)
        for name, value in metrics.items():
            grouped.setdefault(key, {}).setdefault(name, []).append(value)
    summary = {key: {name: summarize(values) for name, values in metrics.items()} for key, metrics in grouped.items()}
    complete = (run["warmup_seconds"] >= 60 and run["measure_seconds"] >= 300
                and run["maintenance_seconds"] >= 300 and run["repetitions"] >= 3
                and len(summary) == 12 and not failures
                and all(len(m["mixed_ops_s"]["runs"]) >= 3 for m in summary.values()))
    comparisons = []
    for key, current in summary.items():
        if not key.endswith("/new-local"):
            continue
        baseline = summary.get(key.removesuffix("new-local") + "main-local")
        if not baseline:
            continue
        for name, measured in current.items():
            if name not in baseline or not baseline[name]["median"]:
                continue
            target = None
            if name == "mixed_ops_s":
                target, good = 10, True
            elif name == "cold_p95_ms":
                target, good = -10, False
            elif name == "peak_rss_bytes":
                target, good = 10, False
            elif "_p95_" in name or "_p99_" in name:
                target, good = 5, False
            change = (measured["median"] / baseline[name]["median"] - 1) * 100
            noisy = max(measured["cv"], baseline[name]["cv"]) > .05
            status = "observed"
            if target is not None:
                passes = change >= target if good else change <= target
                status = "inconclusive" if not complete or noisy else "pass" if passes else "fail"
            comparisons.append({"scenario": key.removesuffix("/new-local"), "metric": name,
                                "main_local": baseline[name]["median"], "new_local": measured["median"],
                                "change_percent": change, "target_percent": target, "noisy": noisy, "status": status})
    accepted = complete and all(c["status"] == "pass" for c in comparisons if c["target_percent"] is not None)
    result = {"complete_matrix": complete, "accepted": accepted, "run": run, "summary": summary,
              "comparisons": comparisons, "maintenance_samples": counts, "failures": failures}
    (root / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = ["# Local disk comparison", "", f"Complete matrix: **{complete}**. Acceptance established: **{accepted}**.", "",
             "Each value below is the median across repetitions. A coefficient of variation above 5%",
             "in either compared group marks an acceptance result inconclusive. Short or incomplete",
             "runs cannot establish acceptance. Process-cold reads leave the OS page cache uncontrolled.", "",
             "## Acceptance metrics", "", "| Scenario | Metric | Main local | New local | Change | Result |",
             "| --- | --- | ---: | ---: | ---: | --- |"]
    for c in comparisons:
        if c["target_percent"] is not None:
            lines.append(f'| {c["scenario"]} | {c["metric"]} | {c["main_local"]:.3f} | {c["new_local"]:.3f} | {c["change_percent"]:+.1f}% | {c["status"]} |')
    lines += ["", "## Three-way observations", "", "| Scenario | Mixed ops/s | Published batches/s | Cold p95 ms | Peak mixed RSS MiB |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for key, metrics in summary.items():
        lines.append(f'| {key} | {metrics["mixed_ops_s"]["median"]:.1f} | {metrics["published_batches_s"]["median"]:.2f} | {metrics["cold_p95_ms"]["median"]:.2f} | {metrics["mixed_rss_bytes"]["median"] / 2**20:.1f} |')
    lines += ["", "## Evidence limits", "", "- Full p50/p95/p99, resource measurements, variation and failures are in `summary.json` and each case directory.",
              "- Maintenance quantiles with few completed cycles are order statistics, not precise tail estimates; sample counts are retained.",
              "- CPU, RSS, file descriptors and fsync counts cover the GraphDB process. MinIO internal costs are excluded.",
              "- WAL readable latency includes terminal confirmation, 10 ms polling and a successful min_version read; it bounds first visibility from above.",
              "- Linux page-cache state and unrelated host workloads are uncontrolled. No global cache eviction or unrelated workload shutdown is performed.",
              "- Binary hashes and durations are in `run.json`; source hashes, image metadata and Docker resources are in the parent evidence directory.",
              "- A failed or noisy target prevents an overall performance improvement claim.", ""]
    if failures:
        lines += ["## Failures", "", "```json", json.dumps(failures, indent=2), "```", ""]
    (root / "summary.md").write_text("\n".join(lines))
    print(root / "summary.md")


if __name__ == "__main__":
    main()
