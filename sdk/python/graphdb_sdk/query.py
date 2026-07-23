from __future__ import annotations

from .stream import NDJSONStream


class QueryMixin:
    def query(self, body: dict) -> dict:
        return self._json("POST", "/v1/query", body=body)

    def graphql(self, document: str, variables: dict | None = None, operation_name: str = "") -> dict:
        body = {"query": document}
        if variables is not None:
            body["variables"] = variables
        if operation_name:
            body["operationName"] = operation_name
        return self._json("POST", "/v1/query/graphql", body=body)

    def gql(self, text: str) -> dict:
        """Execute the legacy FIND/MATCH text DSL. This is not GraphQL."""
        return self._text_json("POST", "/v1/query/gql", text)

    def stream_query(self, body: dict) -> NDJSONStream:
        return self._stream("POST", "/v1/query/stream", body=body)

    def stream_gql(self, text: str) -> NDJSONStream:
        """Stream the legacy FIND/MATCH text DSL."""
        return self._stream("POST", "/v1/query/gql/stream", text=text)

    def save_query(self, name: str, request: dict, description: str = "") -> dict:
        return self._json("POST", "/v1/query/templates", body={"name": name, "description": description, "request": request})

    def list_queries(self) -> dict:
        return self._json("GET", "/v1/query/templates")

    def run_saved_query(self, name: str) -> dict:
        return self._json("POST", f"/v1/query/templates/{self._escape(name)}/run")

    def list_running_queries(self) -> dict:
        return self._json("GET", "/v1/queries/running")

    def kill_running_query(self, query_id: str) -> dict:
        return self._json("DELETE", f"/v1/queries/running/{self._escape(query_id)}")
