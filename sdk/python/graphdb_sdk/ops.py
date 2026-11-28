from __future__ import annotations


class OpsMixin:
    def start_import(
        self,
        data: bytes | str,
        format: str,
        *,
        source: str | None = None,
        collector_id: str | None = None,
        batch_size: int | None = None,
        on_error: str | None = None,
    ) -> dict:
        format = format.strip().lower()
        if format in ("jsonl", "ndjson"):
            format, content_type = "jsonl", "application/x-ndjson"
        elif format == "csv":
            content_type = "text/csv"
        else:
            raise ValueError("import format must be jsonl or csv")
        payload = data.encode("utf-8") if isinstance(data, str) else data
        query = {
            "format": format,
            "source": source,
            "collector_id": collector_id,
            "batch_size": batch_size,
            "on_error": on_error,
        }
        return self._raw_json("POST", "/v1/imports", payload, content_type, query=query)

    def put_relation_schema(self, relation_type: str, schema: dict) -> dict:
        return self._json("PUT", f"/v1/relation-schemas/{self._escape(relation_type)}", body=schema)

    def delete_relation_schema(self, relation_type: str) -> dict:
        return self._json("DELETE", f"/v1/relation-schemas/{self._escape(relation_type)}")

    def start_task(self, task_type: str, params: dict | None = None) -> dict:
        body = {"type": task_type}
        if params is not None:
            body["params"] = params
        return self._json("POST", "/v1/tasks", body=body)

    def list_tasks(self, **options) -> dict:
        return self._json("GET", "/v1/tasks", query=options)

    def get_task(self, task_id: str) -> dict:
        return self._json("GET", f"/v1/tasks/{self._escape(task_id)}")

    def cancel_task(self, task_id: str) -> dict:
        return self._json("POST", f"/v1/tasks/{self._escape(task_id)}/cancel")

    def retry_task(self, task_id: str) -> dict:
        return self._json("POST", f"/v1/tasks/{self._escape(task_id)}/retry")

    def index_health(self, deep: bool = False) -> dict:
        return self._json("GET", "/v1/indexes/health", query={"deep": deep})

    def index_catalog(self) -> dict:
        return self._json("GET", "/v1/indexes")

    def list_index_definitions(self) -> dict:
        return self._json("GET", "/v1/indexes/definitions")

    def create_index(self, kind: str, field: str, *, name: str = "", unique: bool = False) -> dict:
        body: dict = {"kind": kind, "field": field, "unique": unique}
        if name:
            body["name"] = name
        return self._json("POST", "/v1/indexes", body=body)

    def drop_index(self, name: str) -> dict:
        return self._json("DELETE", f"/v1/indexes/definitions/{self._escape(name)}")

    def rebuild_indexes(self, async_: bool = False) -> dict:
        return self._json("POST", "/v1/indexes/rebuild", query={"async": async_})

    def reader_freshness(self) -> dict:
        return self._json("GET", "/v1/control/reader-freshness")

    def reader_fleet_readiness(
        self,
        *,
        min_ready: int | None = None,
        max_staleness_ms: int | None = None,
        min_version: int | None = None,
        target_version: int | None = None,
    ) -> dict:
        version_floor = min_version if min_version is not None else target_version
        query = {
            "min_ready": min_ready,
            "max_staleness_ms": max_staleness_ms,
            "min_version": version_floor,
        }
        return self._json("GET", "/v1/control/reader-fleet-readiness", query=query)

    def reader_traffic_gate(
        self,
        *,
        max_staleness_ms: int | None = None,
        min_version: int | None = None,
        target_version: int | None = None,
        allow_stale: bool | None = None,
        refresh: bool | None = None,
    ) -> dict:
        version_floor = min_version if min_version is not None else target_version
        query = {
            "max_staleness_ms": max_staleness_ms,
            "min_version": version_floor,
            "allow_stale": allow_stale,
            "refresh": refresh,
        }
        return self._json("GET", "/v1/control/reader-traffic-gate", query=query)

    def writer_lease(self) -> dict:
        return self._json("GET", "/v1/control/writer-lease")

    def integrity_audit(self, deep: bool = True) -> dict:
        return self._json("GET", "/v1/control/integrity-audit", query={"deep": deep})

    def repair(self, apply: bool = False) -> dict:
        return self._json("POST", "/v1/control/repair", body={"apply": apply})

    def recover(self) -> dict:
        return self._json("POST", "/v1/control/recover")

    def cleanup_commits(self) -> dict:
        return self._json("POST", "/v1/control/cleanup-commits")

    def gc(self, *, keep_snapshots: int = 2, dry_run: bool = False, max_deletes: int | None = None, cleanup_index_orphans: bool = True) -> dict:
        body: dict = {
            "keep_snapshots": keep_snapshots,
            "dry_run": dry_run,
            "cleanup_index_orphans": cleanup_index_orphans,
        }
        if max_deletes is not None:
            body["max_deletes"] = max_deletes
        return self._json("POST", "/v1/control/gc", body=body)

    def compact(self) -> dict:
        return self._json("POST", "/v1/compact")
