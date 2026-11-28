from __future__ import annotations

import time
from typing import Any
from urllib import parse

from .errors import GraphDBAPIError


def _ingest_result_api_error(result: dict, last_error: str = "") -> GraphDBAPIError:
    error_code = str(result.get("error_code") or "")
    status_code = 422
    if not error_code:
        error_code = "internal_error"
        status_code = 500
    elif error_code in ("version_conflict", "atomic_suppressed", "idempotency_conflict"):
        status_code = 409
    elif error_code == "precondition_failed":
        status_code = 412
    elif error_code in ("atomic_validation_failed", "mutation_budget_exceeded"):
        status_code = 422
    message = "ingest request failed"
    conflicts = result.get("conflicts")
    failures = result.get("failures")
    if isinstance(conflicts, list) and conflicts and isinstance(conflicts[-1], dict) and conflicts[-1].get("message"):
        message = str(conflicts[-1]["message"])
    elif isinstance(failures, list) and failures and isinstance(failures[0], dict) and failures[0].get("error"):
        message = str(failures[0]["error"])
    elif last_error:
        message = last_error
    return GraphDBAPIError(
        status_code=status_code,
        code=error_code,
        message=message,
        retryable=False,
        detail=result,
    )


class WriteMixin:
    def commit(self, mutations: dict, expected_version: int | None = None, idempotency_key: str = "") -> dict:
        body: dict[str, Any] = {"mutations": mutations}
        if expected_version is not None:
            body["expected_version"] = expected_version
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        return self._json("POST", "/v1/commits", body=body)

    def ingest(self, batch: dict, timeout: float | None = None) -> dict:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        effective_timeout = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + effective_timeout if effective_timeout is not None else None
        submission = self._submit_ingest(batch, wait_committed=True, timeout=effective_timeout)
        if "status_url" not in submission:
            return submission
        remaining: float | None = None
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for ingest to finish")
        status = self.wait_ingest(submission, timeout=remaining)
        result = status.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"terminal ingest status {status.get('state')!r} has no result")
        # Partial-failure batches (applied > 0, error_code == "") return 207
        # Multi-Status synchronously; preserve that contract on async polling.
        if status.get("state") == "failed" and (
            result.get("error_code") or not result.get("applied")
        ):
            raise _ingest_result_api_error(result, str(status.get("last_error") or ""))
        return result

    def submit_ingest(self, batch: dict, timeout: float | None = None) -> dict:
        return self._submit_ingest(batch, wait_committed=False, timeout=timeout)

    def _submit_ingest(self, batch: dict, wait_committed: bool, timeout: float | None = None) -> dict:
        headers = {"Prefer": "wait=committed"} if wait_committed else None
        payload, status, response_headers = self._json_with_meta(
            "POST", "/v1/ingest/batches", body=batch, headers=headers, timeout=timeout
        )
        if status != 202:
            return payload
        if not payload.get("status_url"):
            payload["status_url"] = self._response_header(response_headers, "Location")
        payload.setdefault("source", batch.get("source", ""))
        payload.setdefault("collector_id", batch.get("collector_id", ""))
        self._ingest_status_path(payload.get("status_url", ""))
        return payload

    def get_ingest_status(self, status_url: str, timeout: float | None = None) -> dict:
        return self._json("GET", self._ingest_status_path(status_url), timeout=timeout)

    def wait_ingest(
        self,
        status_or_acceptance: str | dict,
        poll_interval: float = 0.25,
        timeout: float | None = None,
    ) -> dict:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        status_url = (
            status_or_acceptance.get("status_url", "")
            if isinstance(status_or_acceptance, dict)
            else status_or_acceptance
        )
        self._ingest_status_path(status_url)
        deadline = time.monotonic() + timeout if timeout is not None else None
        attempt = 0
        while True:
            call_timeout: float | None = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0 and (attempt > 0 or (timeout is not None and timeout > 0)):
                    raise TimeoutError("timed out waiting for ingest to finish")
                if remaining > 0:
                    call_timeout = remaining
            try:
                status = self.get_ingest_status(status_url, timeout=call_timeout)
            except GraphDBAPIError as exc:
                if exc.status_code in (429, 503):
                    attempt += 1
                    delay = (exc.retry_after_ms / 1000.0) if exc.retry_after_ms else poll_interval
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("timed out waiting for ingest to finish") from None
                        time.sleep(min(delay, remaining))
                    else:
                        time.sleep(delay)
                    continue
                raise
            attempt += 1
            state = status.get("state")
            if state in ("committed", "failed"):
                return status
            if state not in ("accepted", "prepared", "published", "retrying"):
                raise RuntimeError(f"unknown ingest state {state!r}")
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for ingest to finish")
                time.sleep(min(poll_interval, remaining))
            else:
                time.sleep(poll_interval)

    @staticmethod
    def _response_header(headers: dict[str, str], name: str) -> str:
        lowered = name.lower()
        return next((value for key, value in headers.items() if key.lower() == lowered), "")

    @staticmethod
    def _ingest_status_path(status_url: str) -> str:
        if not isinstance(status_url, str):
            raise ValueError(f"invalid ingest status URL {status_url!r}")
        status_url = status_url.strip()
        parsed = parse.urlsplit(status_url)
        if parsed.scheme or parsed.netloc or "#" in status_url:
            raise ValueError(f"invalid ingest status URL {status_url!r}")
        prefix = "/v1/ingest/batches/"
        parts_valid = False
        if parsed.path.startswith("/v1/ingest/writers/"):
            prefix = "/v1/ingest/writers/"
            parts = parsed.path.removeprefix(prefix).split("/")
            decoded = [parse.unquote(part) for part in parts]
            # Support both current 5-segment (/writers/{id}/batches/{src}/{col}/{batch})
            # and legacy 4-segment (/writers/{id}/{src}/{col}/{batch}) URLs.
            parts_valid = (
                (len(parts) == 4 or (len(parts) == 5 and decoded[1] == "batches"))
                and not any(part in ("", ".", "..") for part in decoded)
            )
        elif parsed.path.startswith(prefix):
            parts = parsed.path.removeprefix(prefix).split("/")
            decoded = [parse.unquote(part) for part in parts]
            parts_valid = len(parts) == 3 and not any(part in ("", ".", "..") for part in decoded)
        if not parts_valid:
            raise ValueError(f"invalid ingest status URL {status_url!r}")
        if "?" in status_url:
            pairs = parse.parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
            query = dict(pairs)
            if len(pairs) != len(query) or "accepted_lsn" not in query:
                raise ValueError(f"invalid ingest status URL {status_url!r}")
            for key, value in pairs:
                if (
                    key not in ("accepted_lsn", "accepted_epoch")
                    or not value.isascii()
                    or not value.isdecimal()
                    or len(value) > 20
                    or value.startswith("0")
                    or int(value) >= 2**64
                ):
                    raise ValueError(f"invalid ingest status URL {status_url!r}")
            return f"{parsed.path}?{parsed.query}"
        return parsed.path

    def get_source_policy(self) -> dict:
        return self._json("GET", "/v1/source-policy")

    def put_source_policy(self, policy: dict) -> dict:
        return self._json("PUT", "/v1/source-policy", body=policy)

    def get_tenant_config(self) -> dict:
        return self._json("GET", "/v1/tenant-config")

    def put_tenant_config(self, config: dict) -> dict:
        return self._json("PUT", "/v1/tenant-config", body=config)
