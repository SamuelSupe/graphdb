from __future__ import annotations

from typing import Any
from urllib import parse


class TenantMixin:
    def create_tenant(self, tenant_id: str, **metadata: Any) -> dict:
        return self._json("POST", "/v1/tenants", tenant_id=None, body={"tenant_id": tenant_id, **metadata})

    def list_tenants(self, include_legacy: bool = False) -> dict:
        return self._json("GET", "/v1/tenants", tenant_id=None, query={"include_legacy": include_legacy})

    def get_tenant(self, tenant_id: str) -> dict:
        return self._json("GET", f"/v1/tenants/{self._escape(tenant_id)}", tenant_id=None)

    def disable_tenant(self, tenant_id: str) -> dict:
        return self._tenant_action(tenant_id, "disable")

    def enable_tenant(self, tenant_id: str) -> dict:
        return self._tenant_action(tenant_id, "enable")

    def delete_tenant(self, tenant_id: str) -> dict:
        return self._json("DELETE", f"/v1/tenants/{self._escape(tenant_id)}", tenant_id=None)

    def purge_tenant(self, tenant_id: str, force: bool = False) -> dict:
        return self._json("POST", f"/v1/tenants/{self._escape(tenant_id)}/purge", tenant_id=None, query={"force": force})

    def clone_tenant(self, source_tenant_id: str, target_tenant_id: str, **metadata: Any) -> dict:
        body = {"target_tenant_id": target_tenant_id, **metadata}
        return self._json("POST", f"/v1/tenants/{self._escape(source_tenant_id)}/clone", tenant_id=None, body=body)

    def backup_tenant(self, tenant_id: str) -> dict:
        return self._json("POST", f"/v1/tenants/{self._escape(tenant_id)}/backup", tenant_id=None)

    def restore_tenant(self, tenant_id: str, backup_key: str, overwrite: bool = False, dry_run: bool = False) -> dict:
        body = {"backup_key": backup_key, "overwrite": overwrite, "dry_run": dry_run}
        return self._json("POST", f"/v1/tenants/{self._escape(tenant_id)}/restore", tenant_id=None, body=body)

    def restore_drill_tenant(self, tenant_id: str, params: dict | None = None) -> dict:
        return self._json("POST", f"/v1/tenants/{self._escape(tenant_id)}/restore-drill", tenant_id=None, body=params or {})

    def _tenant_action(self, tenant_id: str, action: str) -> dict:
        return self._json("POST", f"/v1/tenants/{self._escape(tenant_id)}/{action}", tenant_id=None)

    def _escape(self, value: str) -> str:
        return parse.quote(value, safe="")
