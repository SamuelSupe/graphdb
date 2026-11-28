from __future__ import annotations

import ssl
from urllib import parse

from .ops import OpsMixin
from .query import QueryMixin
from .read import ReadMixin
from .tenant import TenantMixin
from .transport import TransportMixin, _ConnectionPool
from .write import WriteMixin

SDK_VERSION = "2.7.2"
DEFAULT_TIMEOUT_SECONDS = 60.0
_MAX_POOLED_CONNECTIONS = 16


class GraphDBClient(TenantMixin, WriteMixin, ReadMixin, QueryMixin, OpsMixin, TransportMixin):
    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        timeout: float | None = DEFAULT_TIMEOUT_SECONDS,
        bearer_token: str | None = None,
        headers: dict[str, str] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        _pool: _ConnectionPool | None = None,
    ) -> None:
        cleaned_url = base_url.strip().rstrip("/")
        parsed = parse.urlsplit(cleaned_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc or not parsed.hostname:
            raise ValueError("base_url must use http or https with a host")
        # Eagerly validate port syntax (raises ValueError on non-numeric/out-of-range port).
        _ = parsed.port
        self.base_url = cleaned_url
        self._parsed_base = parsed
        self.tenant_id = tenant_id
        self.timeout = timeout
        self._ssl_context = ssl_context
        self.headers = {"User-Agent": f"graphdb-python-sdk/{SDK_VERSION}"}
        if bearer_token and bearer_token.strip():
            self.headers["Authorization"] = f"Bearer {bearer_token.strip()}"
        self.headers.update(headers or {})
        self._owns_pool = _pool is None
        self._pool: _ConnectionPool = (
            _pool if _pool is not None else _ConnectionPool(maxsize=_MAX_POOLED_CONNECTIONS)
        )

    def for_tenant(self, tenant_id: str) -> "GraphDBClient":
        return GraphDBClient(
            self.base_url,
            tenant_id=tenant_id,
            timeout=self.timeout,
            headers=self.headers,
            ssl_context=self._ssl_context,
            _pool=self._pool,
        )

    def close(self) -> None:
        if self._owns_pool:
            self._pool.close()

    def __enter__(self) -> "GraphDBClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
