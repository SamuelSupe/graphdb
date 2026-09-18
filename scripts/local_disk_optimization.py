#!/usr/bin/env python3
"""One before/after local-volume run using the existing fixed-graph workload.

Provide graphdb-main (before), graphdb-new (after), and loadtest in --bin.
Run inside Linux with --output on a private local volume. Each case starts
from the same stopped database copy; no existing data directory is modified.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

from local_disk_benchmark import Server, cold_reads, dump, load, maintenance, sampled_run


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    dump(args.output / "run.json", {
        "entities": args.entities, "warmup_seconds": args.warmup,
        "measure_seconds": args.measure, "repetitions": 1,
        "write_interval": args.write_interval,
        "os_page_cache": "uncontrolled; no global eviction",
        "binaries": {name: hashlib.sha256((args.bin / name).read_bytes()).hexdigest()
                     for name in ("graphdb-main", "graphdb-new", "loadtest")},
        "cgroup": {name: Path("/sys/fs/cgroup", name).read_text().strip()
                   for name in ("cpu.max", "memory.max")
                   if Path("/sys/fs/cgroup", name).exists()},
    })
    seed = args.output / "seed"
    seed.mkdir()
    server = Server(args, "main-local", "direct", seed)
    try:
        server.start()
        load(args, seed, "direct", args.entities, "seed", 0, seed=True)
    finally:
        server.stop()
    # Reverse the order for WAL to avoid always favoring the second binary.
    for mode in args.modes:
        cases = ("main-local", "new-local") if mode == "direct" else ("new-local", "main-local")
        for case in cases:
            folder = args.output / (case + "-" + mode)
            folder.mkdir()
            shutil.copytree(seed / "data", folder / "data")
            server = Server(args, case, mode, folder)
            try:
                server.start()
                load(args, folder, mode, args.entities, "warmup", args.warmup)
                sampled_run(server, folder, "mixed", lambda: load(
                    args, folder, mode, args.entities, "measure", args.measure))
                snapshot, timing = sampled_run(server, folder, "maintenance", maintenance)
                dump(folder / "maintenance.json", timing)
                cold_reads(args, server, folder, snapshot)
                print(json.dumps({"case": case, "mode": mode, "status": "passed"}), flush=True)
            finally:
                server.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--entities", type=int, default=10002)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--measure", type=int, default=30)
    parser.add_argument("--cold-samples", type=int, default=3)
    parser.add_argument("--modes", nargs="+", choices=("direct", "wal"), default=("direct", "wal"))
    parser.add_argument("--write-interval", default="", help="optional per-client write interval, e.g. 2s")
    run(parser.parse_args())
