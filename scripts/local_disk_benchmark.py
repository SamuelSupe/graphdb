#!/usr/bin/env python3
"""Run the three-way local-disk comparison inside an isolated Linux container.

The baseline binaries come from the requested revision with only sync counters
added. This script never operates on an existing user's data directory.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.error
import urllib.request


def request(path, body=None, tenant="bench", timeout=120):
    encoded = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request("http://127.0.0.1:39080" + path, data=encoded,
                                 headers={"X-Tenant-ID": tenant, "Content-Type": "application/json"})
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        exc.msg = f"{exc.reason}; {path}: {exc.read(4096).decode(errors='replace')}"
        raise
    return data, (time.monotonic() - start) * 1000


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def proc_sample(pid):
    root = Path("/proc") / str(pid)
    status = dict(line.split(":", 1) for line in (root / "status").read_text().splitlines() if ":" in line)
    ticks = (root / "stat").read_text().rsplit(")", 1)[1].split()
    io = dict((k, int(v.strip())) for k, v in (line.split(":", 1) for line in (root / "io").read_text().splitlines()))
    return {"at": time.time(), "rss_bytes": int(status.get("VmRSS", "0 kB").split()[0])*1024,
            "rss_hwm_bytes": int(status.get("VmHWM", "0 kB").split()[0])*1024,
            "system_load": os.getloadavg(),
            "cpu_seconds": (int(ticks[11])+int(ticks[12]))/os.sysconf("SC_CLK_TCK"),
            "fds": len(list((root / "fd").iterdir())), "io": io}


def sync_metrics():
    raw, _ = request("/metrics", tenant="")
    return {line.split()[0]: float(line.split()[1]) for line in raw.decode().splitlines()
            if line.startswith("graphdb_disk_sync_")}


class Server:
    def __init__(self, args, case, mode, folder, environment=None):
        self.args, self.case, self.mode, self.folder = args, case, mode, folder
        self.environment = environment or {}
        self.process = None
        self.log = None

    def start(self):
        started = time.monotonic()
        env = {k: v for k, v in os.environ.items() if not k.startswith(("GRAPHDB_", "S3_"))}
        env.update(GRAPHDB_ADDR="127.0.0.1:39080", GRAPHDB_MODE="all", GRAPHDB_COORDINATION="local",
                   GRAPHDB_DATA_DIR=str(self.folder / "data"), GRAPHDB_PREFIX="bench",
                   GRAPHDB_INSTANCE_ID="disk-benchmark", GRAPHDB_INGEST_MODE=self.mode,
                   GRAPHDB_INGEST_WAL_DURABILITY="sync", GRAPHDB_INGEST_FLUSH_INTERVAL="100ms",
                   GRAPHDB_WRITE_QUEUE_TIMEOUT="60s",
                   GRAPHDB_READER_CATCHUP_TIMEOUT="120s",
                   GRAPHDB_READER_CACHE_MAX_BYTES="536870912", GRAPHDB_WRITE_CACHE_MAX_BYTES="536870912")
        if self.case == "main-s3":
            env.update(GRAPHDB_STORAGE="s3", S3_ENDPOINT=self.args.s3_endpoint, S3_BUCKET=self.args.output.name + "-" + self.folder.name,
                       S3_REGION="us-east-1", S3_PROVIDER="generic-s3", S3_PATH_STYLE="true",
                       S3_ACCESS_KEY_ID="graphdbbench", S3_SECRET_ACCESS_KEY="graphdbbench-local-only")
        else:
            env["GRAPHDB_STORAGE"] = "local"
        env.update(self.environment)
        binary = self.args.bin / ("graphdb-new" if self.case == "new-local" else "graphdb-main")
        self.log = (self.folder / "server.log").open("ab")
        self.process = subprocess.Popen([str(binary), "serve"], env=env, stdout=self.log, stderr=self.log)
        dump(self.folder / "environment.json", {k: v for k, v in env.items() if k.startswith(("GRAPHDB_", "S3_")) and "KEY" not in k})
        for _ in range(240):
            if self.process.poll() is not None:
                raise RuntimeError("server exited: " + (self.folder / "server.log").read_text()[-3000:])
            try:
                request("/v1/readiness", tenant="", timeout=1)
                return (time.monotonic() - started) * 1000
            except (OSError, urllib.error.URLError):
                time.sleep(.25)
        raise RuntimeError("server readiness timed out")

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise RuntimeError("server failed graceful shutdown")
        if self.log:
            self.log.close()


def load(args, folder, mode, entities, stage, duration, seed=False):
    command = [str(args.bin / "loadtest"), "-base", "http://127.0.0.1:39080", "-tenant", "bench",
               "-entities", str(entities), "-batch-size", "500" if seed else "20",
               "-writers", "4", "-readers", "16", "-timeout", "30m", "-http-timeout", "120s",
               "-warmup", "0", "-run-id", folder.name + "-" + stage,
               "-update-epoch", stage,
               "-report-json", str(folder / (stage + ".json"))]
    if seed:
        command.append("-seed-only")
    else:
        command.extend(["-skip-seed", "-duration", str(duration) + "s"])
        if getattr(args, "write_interval", ""):
            command.extend(["-write-interval", args.write_interval])
        if mode == "wal":
            command.append("-wal")
    with (folder / (stage + ".log")).open("wb") as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(stage + " failed: " + (folder / (stage + ".log")).read_text()[-2000:])


def sampled_run(server, folder, name, work):
    before = sync_metrics()
    samples = [proc_sample(server.process.pid)]
    stopped = threading.Event()
    def sampler():
        while not stopped.wait(.5):
            samples.append(proc_sample(server.process.pid))
    thread = threading.Thread(target=sampler)
    thread.start()
    try:
        return work()
    finally:
        stopped.set()
        thread.join()
        samples.append(proc_sample(server.process.pid))
        after = sync_metrics()
        elapsed = samples[-1]["at"] - samples[0]["at"]
        dump(folder / ("resources-" + name + ".json"), {
            "elapsed_seconds": elapsed,
            "peak_rss_bytes": max(s["rss_bytes"] for s in samples),
            "lifetime_rss_hwm_bytes": max(s["rss_hwm_bytes"] for s in samples),
            "peak_fds": max(s["fds"] for s in samples),
            "cpu_seconds": samples[-1]["cpu_seconds"] - samples[0]["cpu_seconds"],
            "cpu_cores_mean": (samples[-1]["cpu_seconds"] - samples[0]["cpu_seconds"]) / elapsed,
            "io": {k: samples[-1]["io"][k] - samples[0]["io"][k] for k in samples[0]["io"]},
            "graphdb_sync": {k: after[k] - before.get(k, 0) for k in after},
            "samples": samples})


def wait_task(task_id, restoring=False):
    deadline = time.monotonic() + 1200
    while time.monotonic() < deadline:
        try:
            data, _ = request("/v1/tasks/" + task_id)
        except urllib.error.HTTPError as exc:
            if restoring and exc.code in (404, 410):
                time.sleep(.1)
                continue
            raise
        task = json.loads(data)
        if task["status"] == "succeeded":
            return task
        if task["status"] in ("failed", "canceled"):
            raise RuntimeError(str(task))
        time.sleep(.1)
    raise RuntimeError("task timeout")


def maintenance():
    timings = {}
    export, timings["export_ms"] = request("/v1/export/snapshot")
    snapshot = json.loads(export)
    timings["entities"] = len(snapshot["snapshot"]["entities"])
    timings["edges"] = len(snapshot["snapshot"]["edges"])
    timings["snapshot_sha256"] = hashlib.sha256(export).hexdigest()
    _, timings["compact_ms"] = request("/v1/compact", {}, timeout=1200)
    # Backup and overwrite restore validate the exact exported content, including
    # the graph version, after a warm cache and prior maintenance.
    start = time.monotonic()
    task, _ = request("/v1/tenants/bench/backup", {})
    backup = wait_task(json.loads(task)["id"])
    timings["backup_ms"] = (time.monotonic()-start)*1000
    start = time.monotonic()
    task, _ = request("/v1/tenants/bench/restore", {"backup_key":backup["result"]["backup_key"],"overwrite":True})
    wait_task(json.loads(task)["id"], restoring=True)
    timings["restore_ms"] = (time.monotonic()-start)*1000
    restored, _ = request("/v1/export/snapshot")
    if json.loads(restored) != snapshot:
        raise RuntimeError("restore changed the exported graph")
    timings["restore_verified"] = True
    return snapshot, timings


def maintenance_phase(args, folder):
    samples = []
    last = None
    update = 0
    for phase, duration in [("warmup", args.warmup), ("measure", args.maintenance_seconds)]:
        deadline = time.monotonic() + duration
        cycle = 0
        while time.monotonic() < deadline or cycle == 0:
            update += 1
            started = time.monotonic()
            retries = 0
            while True:
                try:
                    request("/v1/commits", {"mutations":{"upsert_entities":[{"id":"host:seed","kind":"host","fields":{"hostname":"seed-host","region":"maintenance-"+str(update)}}]}})
                    break
                except urllib.error.HTTPError as exc:
                    if exc.code != 429 or time.monotonic() - started >= 1200:
                        raise
                    retries += 1
                    time.sleep(float(exc.headers.get("Retry-After", "2")))
            admission_ms = (time.monotonic() - started) * 1000
            last, timing = maintenance()
            timing["update_ms"] = admission_ms
            timing["backpressure_responses"] = retries
            if phase == "measure":
                samples.append(timing)
            cycle += 1
    dump(folder / "maintenance.json", {"samples": samples, "requested_seconds":args.maintenance_seconds})
    return last


def cold_reads(args, server, folder, expected):
    timings = []
    resources = []
    startups = []
    # Process-cold only: intentionally leave the OS page cache alone. Global
    # drop_caches would disturb other OrbStack workloads on the user's machine.
    for _ in range(args.cold_samples):
        server.stop()
        startups.append(server.start())
        data, elapsed = request("/v1/query", {"op":"match","kind":"host","limit":20})
        response = json.loads(data)
        if response.get("version") != expected["version"]:
            raise RuntimeError("cold restart returned an unexpected version")
        timings.append(elapsed)
        resources.append(proc_sample(server.process.pid))
    ordered = sorted(timings)
    dump(folder / "cold.json", {"process_cold":True,"os_page_cache":"uncontrolled; no global cache eviction",
                               "latency_ms":timings,"resources":resources,
                               "startup_to_ready_ms":startups,"readiness_poll_interval_ms":250,
                               "p95_ms":ordered[max(0,(len(ordered)*95+99)//100-1)] if ordered else None})


def run(args):
    args.output.mkdir(parents=True)
    dump(args.output / "run.json", {"baseline":"ffa854149b7d481cd56da58f4a1f2bf61a58af92",
         "warmup_seconds":args.warmup,"measure_seconds":args.measure,"maintenance_seconds":args.maintenance_seconds,"repetitions":args.repeats,
         "cases":args.cases,"modes":args.modes,"entities":args.entities,"cold_samples":args.cold_samples,
         "cgroup":{name:Path("/sys/fs/cgroup",name).read_text().strip() for name in ("cpu.max","memory.max") if Path("/sys/fs/cgroup",name).exists()},
         "binaries":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in args.bin.iterdir() if p.is_file()},
         "kernel":os.uname().release,"clock_ticks":os.sysconf("SC_CLK_TCK"),"started_at":time.time()})
    failures = []
    for repeat in range(args.repeats):
        order = args.cases[repeat % len(args.cases):] + args.cases[:repeat % len(args.cases)]
        for entities in args.entities:
            for mode in args.modes:
                for case in order:
                    name = f"r{repeat+1}-{entities}-{mode}-{case}"
                    folder = args.output / name
                    folder.mkdir(parents=True)
                    server = Server(args, case, mode, folder)
                    print(json.dumps({"event":"case_start","case":name,"at":time.time()}), flush=True)
                    try:
                        if case == "main-s3":
                            # This bucket is unique to this benchmark run and has no user data.
                            command = [str(args.bin / "mc"), "mb", "--ignore-existing", "bench/" + args.output.name + "-" + name]
                            subprocess.run(command, env={**os.environ,"MC_HOST_bench":args.s3_endpoint.replace("://","://graphdbbench:graphdbbench-local-only@")}, check=True, stdout=subprocess.DEVNULL)
                        server.start()
                        load(args, folder, mode, entities, "seed", 0, seed=True)
                        print(json.dumps({"event":"seeded","case":name,"at":time.time()}), flush=True)
                        if args.warmup:
                            load(args, folder, mode, entities, "warmup", args.warmup)
                        sampled_run(server, folder, "mixed", lambda: load(args, folder, mode, entities, "measure", args.measure))
                        print(json.dumps({"event":"mixed_complete","case":name,"at":time.time()}), flush=True)
                        snapshot = sampled_run(server, folder, "maintenance", lambda: maintenance_phase(args, folder))
                        if len(snapshot["snapshot"]["entities"]) != entities:
                            raise RuntimeError("fixed graph cardinality changed")
                        cold_reads(args, server, folder, snapshot)
                        dump(folder / "complete.json", {"success":True,"at":time.time()})
                        print(json.dumps({"event":"case_complete","case":name,"at":time.time()}), flush=True)
                    except Exception as exc:
                        failures.append({"case":name,"error":str(exc)})
                        dump(folder / "failure.json", failures[-1])
                        print(json.dumps(failures[-1]), flush=True)
                    finally:
                        server.stop()
    dump(args.output / "failures.json", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", type=Path, default=Path("/validation/bin"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--s3-endpoint", default="http://graphdb-bench-minio:9000")
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--measure", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--maintenance-seconds", type=int, default=300)
    parser.add_argument("--cold-samples", type=int, default=30)
    parser.add_argument("--entities", type=int, nargs="+", default=[10002,100002])
    parser.add_argument("--cases", nargs="+", choices=["main-s3","main-local","new-local"], default=["main-s3","main-local","new-local"])
    parser.add_argument("--modes", nargs="+", choices=["direct","wal"], default=["direct","wal"])
    args = parser.parse_args()
    if args.warmup < 0 or args.measure <= 0 or args.maintenance_seconds < 0 or args.repeats < 1 or args.cold_samples < 1:
        parser.error("durations must be nonnegative; measurement, repetitions and cold samples must be positive")
    raise SystemExit(run(args))
