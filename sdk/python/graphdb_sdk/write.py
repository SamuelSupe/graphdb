from __future__ import annotations

from typing import Any


class WriteMixin:
    def commit(self, mutations: dict, expected_version: int | None = None, idempotency_key: str = "") -> dict:
        body: dict[str, Any] = {"mutations": mutations}
        if expected_version is not None:
            body["expected_version"] = expected_version
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        return self._json("POST", "/v1/commits", body=body)

    def ingest(self, batch: dict) -> dict:
        return self._json("POST", "/v1/ingest/batches", body=batch)

    def get_source_policy(self) -> dict:
        return self._json("GET", "/v1/source-policy")

    def put_source_policy(self, policy: dict) -> dict:
        return self._json("PUT", "/v1/source-policy", body=policy)

    def get_tenant_config(self) -> dict:
        return self._json("GET", "/v1/tenant-config")

    def put_tenant_config(self, config: dict) -> dict:
        return self._json("PUT", "/v1/tenant-config", body=config)
