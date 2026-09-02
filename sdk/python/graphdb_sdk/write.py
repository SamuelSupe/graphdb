from __future__ import annotations

import time
from typing import Any
from urllib import parse


class WriteMixin:
    def commit(self, mutations: dict, expected_version: int | None = None, idempotency_key: str = "") -> dict:
        body: dict[str, Any] = {"mutations": mutations}
        if expected_version is not None:
            body["expected_version"] = expected_version
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        return self._json("POST", "/v1/commits", body=body)

    def ingest(self, batch: dict) -> dict:
        submission = self._submit_ingest(batch, wait_committed=True)
        if "status_url" not in submission:
            return submission
        status = self.wait_ingest(submission)
        result = status.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"terminal ingest status {status.get('state')!r} has no result")
        return result

    def submit_ingest(self, batch: dict) -> dict:
        return self._submit_ingest(batch, wait_committed=False)

    def _submit_ingest(self, batch: dict, wait_committed: bool) -> dict:
        headers = {"Prefer": "wait=committed"} if wait_committed else None
        payload, status, response_headers = self._json_with_meta(
            "POST", "/v1/ingest/batches", body=batch, headers=headers
        )
        if status != 202:
            return payload
        if not payload.get("status_url"):
            payload["status_url"] = self._response_header(response_headers, "Location")
        payload.setdefault("source", batch.get("source", ""))
        payload.setdefault("collector_id", batch.get("collector_id", ""))
        self._ingest_status_path(payload.get("status_url", ""))
        return payload

    def get_ingest_status(self, status_url: str) -> dict:
        return self._json("GET", self._ingest_status_path(status_url))

    def wait_ingest(
        self,
        status_or_acceptance: str | dict,
        poll_interval: float = 0.25,
        timeout: float | None = None,
    ) -> dict:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        status_url = (
            status_or_acceptance.get("status_url", "")
            if isinstance(status_or_acceptance, dict)
            else status_or_acceptance
        )
        self._ingest_status_path(status_url)
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            status = self.get_ingest_status(status_url)
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
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError(f"invalid ingest status URL {status_url!r}")
        prefix = "/v1/ingest/batches/"
        part_count = 3
        if parsed.path.startswith("/v1/ingest/writers/"):
            prefix = "/v1/ingest/writers/"
            part_count = 4
        elif not parsed.path.startswith(prefix):
            raise ValueError(f"invalid ingest status URL {status_url!r}")
        parts = parsed.path.removeprefix(prefix).split("/")
        decoded = [parse.unquote(part) for part in parts]
        if len(parts) != part_count or any(part in ("", ".", "..") for part in decoded):
            raise ValueError(f"invalid ingest status URL {status_url!r}")
        return parsed.path

    def get_source_policy(self) -> dict:
        return self._json("GET", "/v1/source-policy")

    def put_source_policy(self, policy: dict) -> dict:
        return self._json("PUT", "/v1/source-policy", body=policy)

    def get_tenant_config(self) -> dict:
        return self._json("GET", "/v1/tenant-config")

    def put_tenant_config(self, config: dict) -> dict:
        return self._json("PUT", "/v1/tenant-config", body=config)
