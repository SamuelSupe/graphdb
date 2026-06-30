from __future__ import annotations

from .ops import OpsMixin
from .query import QueryMixin
from .read import ReadMixin
from .tenant import TenantMixin
from .transport import TransportMixin
from .write import WriteMixin


class GraphDBClient(TenantMixin, WriteMixin, ReadMixin, QueryMixin, OpsMixin, TransportMixin):
    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.tenant_id = tenant_id
        self.timeout = timeout
        self.headers = dict(headers or {})

    def for_tenant(self, tenant_id: str) -> "GraphDBClient":
        return GraphDBClient(self.base_url, tenant_id=tenant_id, timeout=self.timeout, headers=self.headers)
