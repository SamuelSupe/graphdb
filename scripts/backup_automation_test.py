import copy
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backup_automation import AutomationError, BackupWorker, main
from graphdb_sdk import GraphDBAPIError


class FakeCluster:
    base_url = "http://cluster"
    tenant_id = "tenant-a"

    def __init__(self):
        self.tasks = {}
        self.started = []
        self.retries = []
        self.lose_response = False
        self.reject = False
        self.version = 1
        self.corrupt = False
        self.fail_backup = False
        self.queued = False

    def start_task(self, task_type, params):
        if self.reject:
            self.reject = False
            raise GraphDBAPIError(429, "task_capacity", "not admitted")
        task = self.make_task(task_type, params)
        self.started.append(task["id"])
        if self.lose_response:
            error = self.lose_response
            self.lose_response = False
            if isinstance(error, Exception):
                raise error
            raise OSError("202 response lost")
        return copy.deepcopy(task)

    def make_task(self, task_type, params):
        key = str(len(self.tasks) + 1)
        phase = params["backup_automation_phase"]
        result = {"destination": "object", "backup_key": "s3://backup/tenant/capture/manifest.json", "version": self.version}
        if phase == "verify":
            result = {"dry_run": True, "backup_integrity": {"status": "error" if self.corrupt else "ok"}}
        if phase == "drill":
            result = {"recoverable": True, "cleanup": True}
        task = {"id": key, "tenant_id": self.tenant_id, "type": task_type, "params": copy.deepcopy(params),
                "status": "queued" if self.queued else "failed" if self.fail_backup and phase == "backup" else "succeeded", "result": result}
        self.tasks[key] = task
        return task

    def get_task(self, key):
        return copy.deepcopy(self.tasks[key])

    def list_tasks(self, **options):
        return {"tasks": [copy.deepcopy(t) for t in self.tasks.values() if t["type"] == options["type"]]}

    def retry_task(self, key):
        self.retries.append(key)
        previous = self.tasks[key]
        params = {**previous["params"], "retry_of": key}
        task = self.make_task(previous["type"], params)
        task["result"]["version"] = previous["result"]["version"]
        return copy.deepcopy(task)


class BackupAutomationTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.client = FakeCluster()
        self.now = 1000
        self.options = SimpleNamespace(interval=60, drill_interval=60, retry_initial=1, retry_max=4, poll_interval=1, metrics_dir=None)

    def worker(self):
        return BackupWorker(self.client, self.directory, self.options, clock=lambda: self.now,
                            monotonic=lambda: self.now, sleep=lambda seconds: setattr(self, "now", self.now + seconds))

    def fail_run(self, worker):
        with self.assertRaises((AutomationError, OSError, GraphDBAPIError)) as raised:
            worker.run(self.now + 5)
        worker.failure(raised.exception)

    def test_lost_admission_response_survives_restart_without_duplicate_capture(self):
        for response in (True, GraphDBAPIError(400, "bad_request", "late storage error"),
                         GraphDBAPIError(409, "object_write_conflict", "late fence conflict")):
            with self.subTest(response=response):
                self.client = FakeCluster()
                for path in self.directory.iterdir():
                    path.unlink()
                self.client.lose_response = response
                worker = self.worker()
                self.fail_run(worker)
                self.assertEqual(worker.state["pending"]["outcome"], "unknown")
                self.client.version = 2
                self.now += 1
                resumed = self.worker()
                self.assertEqual(resumed.run(self.now + 5), "succeeded")
                self.assertEqual(len(self.client.started), 3)  # backup, readback, full drill
                self.assertEqual(resumed.state["last_backup_version"], 1)
                self.assertEqual(len({t["params"]["backup_automation_cycle"] for t in self.client.tasks.values()}), 1)
                self.assertEqual(resumed.run(self.now + 5), "deferred")
                self.assertEqual(resumed.state["last_run_success"], 1)

    def test_failed_capture_retries_original_task_after_backoff(self):
        self.client.fail_backup = True
        worker = self.worker()
        self.fail_run(worker)
        self.assertEqual(worker.state["consecutive_failures"], 1)
        self.assertEqual(self.worker().run(self.now + 5), "deferred")
        self.now += 1
        self.client.fail_backup = False
        self.client.version = 2
        resumed = self.worker()
        self.assertEqual(resumed.run(self.now + 5), "succeeded")
        self.assertEqual(self.client.retries, ["1"])
        self.assertEqual(resumed.state["last_backup_version"], 1)

    def test_explicit_admission_rejection_can_retry_but_unknown_missing_task_cannot(self):
        self.client.reject = True
        worker = self.worker()
        self.fail_run(worker)
        self.assertEqual(worker.state["pending"]["outcome"], "prepared")
        self.now += 1
        self.assertEqual(self.worker().run(self.now + 5), "succeeded")
        self.now += 60
        worker = self.worker()
        worker.state.update(cycle="crashed-before-send", phase="idle")
        worker.prepare("backup")
        worker.state["pending"]["outcome"] = "unknown"
        worker.save()
        count = len(self.client.started)
        self.fail_run(self.worker())
        self.assertEqual(len(self.client.started), count)

    def test_readback_error_never_records_success_or_runs_full_drill(self):
        self.client.corrupt = True
        worker = self.worker()
        self.fail_run(worker)
        self.assertEqual(worker.state["last_success"], 0)
        self.assertEqual(worker.state["phase"], "verify")
        self.assertEqual(len(self.client.started), 2)
        self.assertEqual(worker.state["last_run_success"], 0)

    def test_pending_task_wait_budget_resumes_same_task(self):
        self.client.queued = True
        worker = self.worker()
        self.fail_run(worker)
        task_id = worker.state["pending"]["task_id"]
        self.client.tasks[task_id]["status"] = "succeeded"
        self.client.queued = False
        self.now += 1
        self.assertEqual(self.worker().run(self.now + 5), "succeeded")
        self.assertEqual(len(self.client.started), 3)

    def test_changed_input_or_ambiguous_reconciliation_is_rejected(self):
        self.client.lose_response = True
        worker = self.worker()
        self.fail_run(worker)
        duplicate = copy.deepcopy(self.client.tasks["1"])
        duplicate["id"] = "other"
        self.client.tasks["other"] = duplicate
        self.now += 1
        self.fail_run(self.worker())
        self.assertEqual(len(self.client.started), 1)
        del self.client.tasks["other"]
        worker.state["pending"].update(task_id="1", outcome="accepted")
        worker.save()
        self.client.tasks["1"]["params"]["destination"] = "local"
        self.fail_run(self.worker())
        self.assertEqual(len(self.client.started), 1)

    def run_tenants(self, clients):
        self.options.state_dir = self.directory
        self.options.daemon = False
        self.options.url = FakeCluster.base_url
        self.options.tenant = list(clients)
        self.options.http_timeout = 1
        self.options.max_run_seconds = 5
        def make_worker(client, directory, options):
            return BackupWorker(client, directory, options, clock=lambda: self.now,
                                monotonic=lambda: self.now, sleep=lambda seconds: setattr(self, "now", self.now + seconds))
        with patch("backup_automation.arguments", return_value=self.options), \
                patch("backup_automation.GraphDBClient") as client, \
                patch("backup_automation.BackupWorker", side_effect=make_worker), \
                patch("backup_automation.time.monotonic", side_effect=lambda: self.now), \
                patch("sys.stdout", new_callable=io.StringIO):
            client.return_value.for_tenant.side_effect = clients.__getitem__
            return main()

    def test_queued_tenant_does_not_starve_later_tenants(self):
        self.client.queued = True
        healthy = FakeCluster()
        healthy.tenant_id = "healthy"
        self.assertEqual(self.run_tenants({self.client.tenant_id: self.client, healthy.tenant_id: healthy}), 1)
        self.assertEqual(len(healthy.started), 3)
        self.assertEqual(self.worker().state["phase"], "backup")

    def test_corrupt_tenant_state_does_not_stop_other_tenants_or_get_overwritten(self):
        path = self.worker().path
        path.write_text('{"schema_version":')
        healthy = FakeCluster()
        healthy.tenant_id = "healthy"
        self.assertEqual(self.run_tenants({self.client.tenant_id: self.client, healthy.tenant_id: healthy}), 1)
        self.assertEqual(len(healthy.started), 3)
        self.assertEqual(path.read_text(), '{"schema_version":')
        self.assertEqual(self.client.started, [])

    def test_state_write_failure_stops_admission_for_that_tenant_only(self):
        path = self.worker().path
        path.with_name(path.name + ".tmp").mkdir()
        healthy = FakeCluster()
        healthy.tenant_id = "healthy"
        self.assertEqual(self.run_tenants({self.client.tenant_id: self.client, healthy.tenant_id: healthy}), 1)
        self.assertEqual(self.client.started, [])
        self.assertFalse(path.exists())
        self.assertEqual(len(healthy.started), 3)

    def test_invalid_state_cannot_discard_active_cycle_and_recapture(self):
        changes = [lambda s: s.update(phase="idle"), lambda s: s.update(phase="unexpected"),
                   lambda s: s["pending"].update(outcome="accepted"),
                   lambda s: s["pending"]["params"].update(backup_automation_cycle="another-cycle"),
                   lambda s: s.update(next_run=float("inf"))]
        for change in changes:
            with self.subTest(change=change):
                for path in self.directory.iterdir():
                    path.unlink()
                worker = self.worker()
                worker.state["cycle"] = "fault-cycle"
                worker.prepare("backup")
                change(worker.state)
                worker.save()
                with self.assertRaises(AutomationError):
                    self.worker()
        self.assertEqual(self.client.started, [])

    def test_metrics_publication_failure_does_not_block_durable_backup(self):
        worker = self.worker()
        worker.metrics_path.mkdir()
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(worker.run(self.now + 5), "succeeded")
        self.assertEqual(self.worker().state["last_backup_version"], 1)
        self.assertEqual(len(self.client.started), 3)


if __name__ == "__main__":
    unittest.main()
