from __future__ import annotations

from typing import Any

ERROR_CODE_BAD_REQUEST = "bad_request"
ERROR_CODE_INVALID_TENANT = "invalid_tenant"
ERROR_CODE_TENANT_REQUIRED = "tenant_required"
ERROR_CODE_TENANT_DISABLED = "tenant_disabled"
ERROR_CODE_TENANT_DELETED = "tenant_deleted"
ERROR_CODE_OPERATION_DISABLED = "operation_disabled"
ERROR_CODE_NOT_FOUND = "not_found"
ERROR_CODE_METHOD_NOT_ALLOWED = "method_not_allowed"
ERROR_CODE_REQUEST_TOO_LARGE = "request_too_large"
ERROR_CODE_TOO_MANY_REQUESTS = "too_many_requests"
ERROR_CODE_INTERNAL = "internal_error"
ERROR_CODE_INVALID_JSON = "invalid_json"
ERROR_CODE_INVALID_QUERY = "invalid_query"
ERROR_CODE_QUERY_LIMIT_EXCEEDED = "query_limit_exceeded"
ERROR_CODE_MUTATION_BUDGET = "mutation_budget_exceeded"
ERROR_CODE_INDEX_STALE = "index_stale"
ERROR_CODE_READER_NOT_FRESH = "reader_not_fresh"
ERROR_CODE_QUOTA_EXCEEDED = "quota_exceeded"
ERROR_CODE_LEASE_HELD = "lease_held"
ERROR_CODE_MANIFEST_CAS_CONFLICT = "manifest_cas_conflict"
ERROR_CODE_OBJECT_WRITE_CONFLICT = "object_write_conflict"
ERROR_CODE_OBJECT_STORE_UNAVAILABLE = "object_store_unavailable"
ERROR_CODE_TASK_CONFLICT = "task_conflict"
ERROR_CODE_REPAIR_REQUIRED = "repair_required"
ERROR_CODE_VERSION_CONFLICT = "version_conflict"
ERROR_CODE_PRECONDITION_FAILED = "precondition_failed"
ERROR_CODE_ATOMIC_VALIDATION_FAILED = "atomic_validation_failed"
ERROR_CODE_ATOMIC_SUPPRESSED = "atomic_suppressed"
ERROR_CODE_IDEMPOTENCY_CONFLICT = "idempotency_conflict"
ERROR_CODE_IDEMPOTENCY_IN_PROGRESS = "idempotency_in_progress"
ERROR_CODE_WRITE_CONFLICT = "write_conflict"
ERROR_CODE_COORDINATOR_UNAVAILABLE = "coordinator_unavailable"
ERROR_CODE_COMMIT_TAIL_TOO_LONG = "commit_tail_too_long"
ERROR_CODE_INDEX_REBUILD_RUNNING = "index_rebuild_running"
ERROR_CODE_MAINTENANCE_TASK_RUNNING = "maintenance_task_running"
ERROR_CODE_WRITE_ADMISSION_TIMEOUT = "write_admission_queue_timeout"
ERROR_CODE_WRITE_BACKPRESSURE = "write_backpressure"
ERROR_CODE_REQUEST_TIMEOUT = "request_timeout"
ERROR_CODE_RAFT_UNAVAILABLE = "raft_unavailable"
ERROR_CODE_WRITE_OUTCOME_UNKNOWN = "write_outcome_unknown"
ERROR_CODE_TENANT_GENERATION_CHANGED = "tenant_generation_changed"
ERROR_CODE_INGEST_QUEUE_FULL = "ingest_queue_full"
ERROR_CODE_REQUEST_CANCELED = "request_canceled"


class GraphDBAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str = "",
        message: str = "",
        retryable: bool = False,
        retry_after_ms: int | None = None,
        detail: Any = None,
        reasons: list[dict] | None = None,
        body: bytes = b"",
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms
        self.detail = {} if detail is None else detail
        self.reasons = reasons or []
        self.body = body
        super().__init__(self._format())

    def _format(self) -> str:
        if self.code and self.message:
            return f"graphdb: status={self.status_code} code={self.code} message={self.message}"
        if self.message:
            return f"graphdb: status={self.status_code} message={self.message}"
        return f"graphdb: status={self.status_code}"
