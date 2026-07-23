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

    def rebuild_indexes(self, async_: bool = False) -> dict:
        return self._json("POST", "/v1/indexes/rebuild", query={"async": async_})

    def reader_freshness(self) -> dict:
        return self._json("GET", "/v1/control/reader-freshness")

    def writer_lease(self) -> dict:
        return self._json("GET", "/v1/control/writer-lease")

    def integrity_audit(self, deep: bool = True) -> dict:
        return self._json("GET", "/v1/control/integrity-audit", query={"deep": deep})

    def repair(self, apply: bool = False) -> dict:
        return self._json("POST", "/v1/control/repair", body={"apply": apply})
