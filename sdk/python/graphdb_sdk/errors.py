from __future__ import annotations


class GraphDBAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str = "",
        message: str = "",
        retryable: bool = False,
        retry_after_ms: int | None = None,
        detail: dict | None = None,
        reasons: list[dict] | None = None,
        body: bytes = b"",
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms
        self.detail = detail or {}
        self.reasons = reasons or []
        self.body = body
        super().__init__(self._format())

    def _format(self) -> str:
        if self.code and self.message:
            return f"graphdb: status={self.status_code} code={self.code} message={self.message}"
        if self.message:
            return f"graphdb: status={self.status_code} message={self.message}"
        return f"graphdb: status={self.status_code}"
