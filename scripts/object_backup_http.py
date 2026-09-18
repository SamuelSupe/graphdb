#!/usr/bin/env python3
"""HTTP recovery from an empty data directory, driven by object_backup_gate.sh."""
import json
from pathlib import Path
import sys
import time

from graphdb_sdk import GraphDBClient


def wait(client, task):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        current = client.get_task(task["id"])
        if current["status"] == "succeeded":
            return current
        assert current["status"] not in ("failed", "canceled"), current
        time.sleep(0.05)
    raise AssertionError(f"task timed out: {task['id']}")


def main():
    phase, base, directory = sys.argv[1:]
    evidence = Path(directory) / "http-evidence.json"
    admin = GraphDBClient(base, timeout=60)
    source = admin.for_tenant("backup-source")
    target = admin.for_tenant("backup-target")
    if phase == "capture":
        admin.create_tenant("backup-source", name="Remote snapshot source")
        source.ingest({
            "source": "backup-gate", "collector_id": "gate", "batch_id": "initial",
            "items": [{"external_id": "host-a", "entity": {"id": "host:a", "kind": "host", "fields": {"name": "snapshot"}}}],
        })
        snapshot = source.export_snapshot()
        backup = wait(source, admin.backup_tenant("backup-source", destination="object"))
        source.commit({"upsert_entities": [{"id": "host:later", "kind": "host"}]})
        second = wait(source, admin.backup_tenant("backup-source", destination="object"))
        evidence.write_text(json.dumps({"snapshot": snapshot, "backup_key": backup["result"]["backup_key"], "second_key": second["result"]["backup_key"]}, indent=2))
        print("WAL committed snapshot uploaded; a later backup is independently discoverable")
        return
    saved = json.loads(evidence.read_text())
    expected = {**saved["snapshot"], "tenant_id": "backup-target"}
    if phase == "restore":
        first = admin.list_object_backups("backup-source", limit=1)
        assert len(first["backups"]) == 1 and first.get("next_cursor"), first
        second = admin.list_object_backups("backup-source", limit=1, cursor=first["next_cursor"])
        assert {first["backups"][0]["backup_key"], second["backups"][0]["backup_key"]} == {saved["backup_key"], saved["second_key"]}
        dry = wait(target, admin.restore_tenant("backup-target", saved["backup_key"], dry_run=True))
        assert dry["result"]["dry_run"] and not dry["result"].get("target_exists", False), dry
        wait(target, admin.restore_tenant("backup-target", saved["backup_key"]))
        assert target.export_snapshot() == expected
        target.commit({"upsert_entities": [{"id": "host:a", "kind": "host", "fields": {"name": "changed"}}]})
        assert target.get_entity("host:a")["fields"]["name"] == "changed"
        wait(target, admin.restore_tenant("backup-target", saved["backup_key"], overwrite=True))
        drill = wait(target, admin.restore_drill_tenant("backup-target", {
            "backup_key": saved["backup_key"], "target_tenant_id": "backup-drill", "cleanup": True,
        }))
        assert drill["result"]["recoverable"], drill
        print("Fresh-directory discovery, dry-run, restore, and overwrite passed")
    assert target.export_snapshot() == expected
    assert target.get_entity("host:a")["fields"]["name"] == "snapshot"
    assert admin.get_tenant("backup-target")["name"] == "Remote snapshot source"
    print(f"{phase}: snapshot, entity query, and metadata verified")


if __name__ == "__main__":
    main()
