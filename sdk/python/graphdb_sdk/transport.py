from __future__ import annotations

import json
from typing import Any
from urllib import error, parse, request

from .errors import GraphDBAPIError
from .stream import NDJSONStream


class TransportMixin:
    def health(self) -> dict:
        return self._json("GET", "/v1/health", tenant_id=None)

    def _json(self, method: str, path: str, tenant_id: str | None = "", query: dict | None = None, body: Any = None, headers: dict[str, str] | None = None) -> dict:
        data = None
        request_headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request_headers.update(headers or {})
        with self._open(method, path, tenant_id, query, data, request_headers) as response:
            payload = response.read()
        return json.loads(payload.decode("utf-8")) if payload else {}

    def _text_json(self, method: str, path: str, text: str) -> dict:
        headers = {"Accept": "application/json", "Content-Type": "text/plain"}
        with self._open(method, path, "", None, text.encode("utf-8"), headers) as response:
            return json.loads(response.read().decode("utf-8"))

    def _raw_json(self, method: str, path: str, data: bytes, content_type: str, query: dict | None = None) -> dict:
        headers = {"Accept": "application/json", "Content-Type": content_type}
        with self._open(method, path, "", query, data, headers) as response:
            payload = response.read()
        return json.loads(payload.decode("utf-8")) if payload else {}

    def _stream(self, method: str, path: str, body: Any = None, text: str | None = None) -> NDJSONStream:
        headers = {"Accept": "application/x-ndjson"}
        data = None
        if text is not None:
            data = text.encode("utf-8")
            headers["Content-Type"] = "text/plain"
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        return NDJSONStream(self._open(method, path, "", None, data, headers))

    def _open(self, method: str, path: str, tenant_id: str | None, query: dict | None, data: bytes | None, headers: dict):
        req_headers = dict(self.headers)
        req_headers.update(headers)
        effective_tenant = self.tenant_id if tenant_id == "" else tenant_id
        if effective_tenant:
            req_headers["X-Tenant-ID"] = effective_tenant
        req = request.Request(self._url(path, query), data=data, headers=req_headers, method=method)
        try:
            return request.urlopen(req, timeout=self.timeout)
        except error.HTTPError as exc:
            raise self._api_error(exc) from exc

    def _url(self, path: str, query: dict | None = None) -> str:
        encoded = self._encode_query(query or {})
        return f"{self.base_url}{path}" + (f"?{encoded}" if encoded else "")

    def _encode_query(self, query: dict) -> str:
        clean_query = {k: v for k, v in query.items() if v not in (None, "", False)}
        return parse.urlencode(clean_query, doseq=True)

    def _api_error(self, exc: error.HTTPError) -> GraphDBAPIError:
        body = exc.read()
        envelope = self._decode_error(body)
        retry_after_ms = envelope.get("retry_after_ms")
        if retry_after_ms is None and exc.headers.get("Retry-After"):
            try:
                retry_after_ms = int(exc.headers["Retry-After"]) * 1000
            except ValueError:
                retry_after_ms = None
        return GraphDBAPIError(
            status_code=exc.code,
            code=envelope.get("code", ""),
            message=envelope.get("message") or envelope.get("error", ""),
            retryable=bool(envelope.get("retryable", False)),
            retry_after_ms=retry_after_ms,
            detail=envelope.get("detail"),
            reasons=envelope.get("reasons"),
            body=body,
        )

    def _decode_error(self, body: bytes) -> dict:
        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            return {"message": body.decode("utf-8", errors="replace")}
