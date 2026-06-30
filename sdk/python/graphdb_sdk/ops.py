from __future__ import annotations


class OpsMixin:
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
