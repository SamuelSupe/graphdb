from __future__ import annotations


class ReadMixin:
    def get_entity(self, entity_id: str, min_version: int | None = None, allow_stale: bool = False) -> dict:
        path = f"/v1/entities/{self._escape(entity_id)}"
        payload = self._json("GET", path, query=self._read_query(min_version, allow_stale))
        return payload.get("entity", payload)

    def list_entities(self, **options) -> dict:
        return self._json("GET", "/v1/entities", query=options)

    def stream_entities(self, **options):
        return self._stream("GET", self._url_path("/v1/entities/stream", options))

    def list_edges(self, **options) -> dict:
        return self._json("GET", "/v1/edges", query=options)

    def stream_edges(self, **options):
        return self._stream("GET", self._url_path("/v1/edges/stream", options))

    def export_snapshot(self, min_version: int | None = None, allow_stale: bool = False) -> dict:
        return self._json("GET", "/v1/export/snapshot", query=self._read_query(min_version, allow_stale))

    def stream_snapshot(self, **options):
        return self._stream("GET", self._url_path("/v1/export/snapshot/stream", options))

    def list_entity_types(self, **options) -> dict:
        return self._json("GET", "/v1/entity-types", query=options)

    def list_relation_schemas(self) -> dict:
        return self._json("GET", "/v1/relation-schemas")

    def _read_query(self, min_version: int | None, allow_stale: bool) -> dict:
        query = {}
        if min_version is not None:
            query["min_version"] = min_version
        if allow_stale:
            query["allow_stale"] = True
        return query

    def _url_path(self, path: str, query: dict) -> str:
        return path + ("?" + self._encode_query(query) if query else "")
