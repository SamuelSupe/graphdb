#!/usr/bin/env python3
"""Durable interval backup worker for standalone and Raft HTTP endpoints."""
import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit
import uuid

# The repository SDK uses only the Python standard library.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk/python"))
from graphdb_sdk import GraphDBAPIError, GraphDBClient


class AutomationError(Exception):
    pass


def atomic_write(path, text, mode=0o600):
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as output:
        os.fchmod(output.fileno(), mode)
        output.write(text)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class BackupWorker:
    def __init__(self, client, directory, options, clock=time.time, sleep=time.sleep, monotonic=time.monotonic):
        self.client, self.options = client, options
        self.clock, self.sleep, self.monotonic = clock, sleep, monotonic
        identity = {"url": client.base_url, "tenant": client.tenant_id}
        name = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        self.path = directory / (name + ".json")
        self.metrics_path = (options.metrics_dir or directory) / (name + ".prom")
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {
            "schema_version": 1, "identity": identity, "phase": "idle", "next_run": 0,
            "last_success": 0, "last_drill": 0, "consecutive_failures": 0,
        }
        if not isinstance(self.state, dict) or self.state.get("schema_version") != 1 or self.state.get("identity") != identity:
            raise AutomationError("state schema or endpoint/tenant identity mismatch")
        self.validate_state()

    def validate_state(self):
        state = self.state
        for key in ("next_run", "last_success", "last_drill"):
            value = state.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise AutomationError("invalid state timestamp: " + key)
        failures = state.get("consecutive_failures")
        if type(failures) is not int or not 0 <= failures <= 64:
            raise AutomationError("invalid state failure count")
        phase, pending = state.get("phase"), state.get("pending", {})
        if phase not in ("idle", "backup", "verify", "drill") or not isinstance(pending, dict):
            raise AutomationError("invalid state phase or pending task")
        if phase == "idle":
            if pending:
                raise AutomationError("idle state still contains an unresolved task")
            return
        cycle, params = state.get("cycle"), pending.get("params")
        if not isinstance(cycle, str) or not cycle or not isinstance(params, dict):
            raise AutomationError("active state lacks a cycle or task input")
        task_type = "tenant_backup" if phase == "backup" else "tenant_restore_drill"
        if (pending.get("type") != task_type or params.get("backup_automation_cycle") != cycle
                or params.get("backup_automation_phase") != phase):
            raise AutomationError("state cycle, phase and task input disagree")
        outcome = pending.get("outcome")
        task_id = pending.get("task_id")
        if outcome not in ("prepared", "unknown", "accepted") or (outcome == "accepted") != bool(task_id):
            raise AutomationError("state submission outcome and task ID disagree")
        if task_id is not None and not isinstance(task_id, str):
            raise AutomationError("invalid state task ID")
        if "retry_of" in pending and (not isinstance(pending["retry_of"], str) or not pending["retry_of"]):
            raise AutomationError("invalid state retry identity")
        if phase == "backup":
            if params.get("destination") != "object":
                raise AutomationError("active backup destination changed")
        elif (not isinstance(state.get("backup_key"), str) or not state["backup_key"].startswith("s3://")
                or params.get("backup_key") != state["backup_key"] or params.get("cleanup") is not True
                or params.get("dry_run") is not (phase == "verify") or type(state.get("backup_version")) is not int
                or state["backup_version"] < 0 or type(state.get("drill_due")) is not bool):
            raise AutomationError("active verification or drill input changed")

    def save(self):
        atomic_write(self.path, json.dumps(self.state, indent=2) + "\n")
        labels = '{endpoint=' + json.dumps(self.client.base_url, ensure_ascii=False) + ',tenant=' + json.dumps(self.client.tenant_id, ensure_ascii=False) + '}'
        values = {
            "last_success_timestamp_seconds": self.state.get("last_success", 0),
            "last_drill_timestamp_seconds": self.state.get("last_drill", 0),
            "next_run_timestamp_seconds": self.state.get("next_run", 0),
            "consecutive_failures": self.state.get("consecutive_failures", 0),
            "pending": int(self.state["phase"] != "idle"),
            "last_run_success": self.state.get("last_run_success", 0),
        }
        text = ""
        for suffix, value in values.items():
            name = "graphdb_backup_automation_" + suffix
            text += f"# TYPE {name} gauge\n{name}{labels} {value}\n"
        try:
            atomic_write(self.metrics_path, text, 0o644)
        except OSError as error:
            print(json.dumps({"event": "backup_automation_metrics_error", "tenant": self.client.tenant_id,
                              "error": str(error)}), flush=True)

    def prepare(self, phase):
        state = self.state
        params = {"backup_automation_cycle": state["cycle"], "backup_automation_phase": phase}
        if phase == "backup":
            task_type = "tenant_backup"
            params["destination"] = "object"
        else:
            task_type = "tenant_restore_drill"
            params.update(backup_key=state["backup_key"], cleanup=True, dry_run=phase == "verify")
        state["phase"] = phase
        state["pending"] = {"type": task_type, "params": params, "outcome": "prepared"}
        self.save()

    def matches(self, task):
        pending = self.state["pending"]
        params = task.get("params", {})
        expected = pending["params"]
        return (task.get("tenant_id") == self.client.tenant_id and task.get("type") == pending["type"]
                and all(params.get(key) == value for key, value in expected.items())
                and params.get("retry_of", "") == pending.get("retry_of", ""))

    def get_pending(self):
        pending = self.state["pending"]
        if pending.get("task_id"):
            task = self.client.get_task(pending["task_id"])
            if not self.matches(task):
                raise AutomationError("persisted task identity or input changed")
            return task
        if pending["outcome"] == "unknown":
            tasks = self.client.list_tasks(type=pending["type"], limit=1000)["tasks"]
            matched = [task for task in tasks if self.matches(task)]
            if len(matched) != 1:
                raise AutomationError("submission outcome unknown; no unique matching task in the bounded listing; refusing resubmission")
            task = matched[0]
        else:
            # Persist before the request: even a lost 202 or a crash must not
            # create a second task at a newer captured version.
            pending["outcome"] = "unknown"
            self.save()
            try:
                if pending.get("retry_of"):
                    task = self.client.retry_task(pending["retry_of"])
                else:
                    task = self.client.start_task(pending["type"], pending["params"])
            except GraphDBAPIError as error:
                # Generic storage errors can use 400/409 after persistence;
                # only definite pre-admission rejections allow a fresh POST.
                if error.status_code in (401, 403, 413, 422, 429) or error.code in (
                    "invalid_json", "invalid_tenant", "tenant_required", "request_too_large",
                    "task_conflict", "shard_epoch_changed", "tenant_generation_changed",
                ):
                    pending["outcome"] = "prepared"
                    self.save()
                raise
            if not self.matches(task):
                raise AutomationError("task admission returned a different identity or input")
        pending.update(task_id=task["id"], outcome="accepted")
        self.save()
        return task

    def run(self, deadline):
        if self.clock() < self.state.get("next_run", 0):
            self.save()
            return "deferred"
        if self.state["phase"] == "idle":
            self.state.update(cycle=uuid.uuid4().hex, last_run_success=0, drill_due=self.options.drill_interval > 0
                              and self.clock() >= self.state["last_drill"] + self.options.drill_interval)
            self.prepare("backup")
        while self.monotonic() < deadline:
            task = self.get_pending()
            if task["status"] in ("failed", "canceled"):
                previous = task["id"]
                self.state["pending"] = {"type": task["type"], "params": self.state["pending"]["params"],
                                         "retry_of": previous, "outcome": "prepared"}
                raise AutomationError("task failed: " + str(task.get("error", task["status"]))[:500])
            if task["status"] != "succeeded":
                self.sleep(min(self.options.poll_interval, max(0, deadline - self.monotonic())))
                continue
            result = task.get("result", {})
            phase = self.state["phase"]
            if phase == "backup":
                key = result.get("backup_key", "")
                if not key.startswith("s3://") or result.get("destination") != "object":
                    raise AutomationError("backup succeeded without an object backup reference")
                self.state.update(backup_key=key, backup_version=result["version"])
                self.prepare("verify")
            elif phase == "verify":
                if result.get("dry_run") is not True or result.get("backup_integrity", {}).get("status") != "ok":
                    raise AutomationError("backup readback did not prove manifest and payload integrity")
                if self.state["drill_due"]:
                    self.prepare("drill")
                else:
                    return self.complete()
            else:
                if result.get("recoverable") is not True or result.get("cleanup") is not True:
                    raise AutomationError("restore drill did not establish recoverability and cleanup")
                self.state["last_drill"] = self.clock()
                return self.complete()
        raise AutomationError("task wait budget expired; persisted task will be polled on the next run")

    def complete(self):
        now = self.clock()
        self.state.update(phase="idle", pending={}, last_success=now,
                          last_backup_key=self.state["backup_key"], last_backup_version=self.state["backup_version"],
                          next_run=now + self.options.interval, consecutive_failures=0, last_error="", last_run_success=1)
        self.save()
        return "succeeded"

    def failure(self, error):
        failures = min(self.state.get("consecutive_failures", 0) + 1, 64)
        delay = min(self.options.retry_initial * (2 ** (failures - 1)), self.options.retry_max)
        self.state.update(consecutive_failures=failures, next_run=self.clock() + delay, last_error=str(error)[:1000], last_run_success=0)
        self.save()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="standalone endpoint or Raft/router entry point")
    parser.add_argument("--tenant", action="append", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--metrics-dir", type=Path, help="readable directory for Prometheus textfile collection")
    parser.add_argument("--daemon", action="store_true", help="keep polling every minute; otherwise run once for a timer/cron job")
    parser.add_argument("--interval", type=int, default=86400, help="seconds between completed backup cycles")
    parser.add_argument("--drill-interval", type=int, default=604800, help="0 disables full drills; readback remains mandatory")
    parser.add_argument("--retry-initial", type=int, default=60)
    parser.add_argument("--retry-max", type=int, default=3600)
    parser.add_argument("--http-timeout", type=float, default=30)
    parser.add_argument("--max-run-seconds", type=float, default=600, help="task polling budget per tenant per invocation")
    parser.add_argument("--poll-interval", type=float, default=1)
    options = parser.parse_args()
    parsed = urlsplit(options.url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        parser.error("--url must be an HTTP(S) endpoint without credentials, query or fragment")
    if options.interval < 60 or options.drill_interval < 0 or not (1 <= options.retry_initial <= options.retry_max <= 86400):
        parser.error("interval >= 60, drill-interval >= 0 and 1 <= retry-initial <= retry-max <= 86400 are required")
    if not all(0 < value < float("inf") for value in (options.http_timeout, options.max_run_seconds, options.poll_interval)):
        parser.error("timeouts and poll interval must be positive")
    return options


def main():
    options = arguments()
    options.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if options.metrics_dir:
        options.metrics_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
    with (options.state_dir / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"event": "backup_automation_busy"}))
            return 1
        client = GraphDBClient(options.url, timeout=options.http_timeout,
                               bearer_token=os.environ.get("GRAPHDB_BACKUP_AUTOMATION_TOKEN"))
        while True:
            failed = False
            for tenant in dict.fromkeys(options.tenant):
                worker = None
                try:
                    worker = BackupWorker(client.for_tenant(tenant), options.state_dir, options)
                    status = worker.run(time.monotonic() + options.max_run_seconds)
                    message = worker.state.get("last_error", "")
                except Exception as error:
                    message = str(error)[:1000]
                    if worker is not None:
                        try:
                            worker.failure(error)
                        except Exception as state_error:
                            message += "; state persistence failed: " + str(state_error)[:500]
                    status, failed = "failed", True
                print(json.dumps({"event": "backup_automation_run", "tenant": tenant, "status": status,
                                  "state_file": str(worker.path) if worker else "", "last_error": message}), flush=True)
            if not options.daemon:
                return 1 if failed else 0
            time.sleep(min(60, options.retry_initial))


if __name__ == "__main__":
    raise SystemExit(main())
