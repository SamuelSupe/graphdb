import io
import json
import unittest
from unittest.mock import patch
from urllib import error

from graphdb_sdk import GraphDBAPIError, GraphDBClient
from graphdb_sdk.stream import NDJSONStream


class _FakeResponse:
    def __init__(self, body: bytes, status: int = 200, headers=None):
        self._buf = io.BytesIO(body)
        self.status = status
        self.headers = headers or {}
        self.closed = False

    def read(self):
        return self._buf.read()

    def __iter__(self):
        return iter(self._buf)

    def getcode(self):
        return self.status

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class Round5SDKRegressionTests(unittest.TestCase):
    def test_header_configured_tenant_preserved_on_tenant_calls_and_stripped_on_platform(self):
        captured = []

        def fake_urlopen(req, timeout=None):
            captured.append(dict(req.header_items()))
            return _FakeResponse(b'{"results": []}')

        client = GraphDBClient("http://localhost:8080", headers={"X-Tenant-ID": "t-hdr"})
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=fake_urlopen):
            client.query({"op": "match", "kind": "host"})
            client.health()

        self.assertEqual(captured[0].get("X-tenant-id"), "t-hdr")
        self.assertNotIn("X-tenant-id", captured[1])

    def test_http_error_response_closed(self):
        closed = {"flag": False}

        class _ClosableFP(io.BytesIO):
            def close(self):
                closed["flag"] = True
                super().close()

        fp = _ClosableFP(b'{"code":"conflict","message":"boom"}')
        http_err = error.HTTPError("http://localhost:8080/v1/query", 409, "Conflict", None, fp)
        client = GraphDBClient("http://localhost:8080", tenant_id="t1")
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=http_err):
            with self.assertRaises(GraphDBAPIError) as ctx:
                client.query({"op": "match"})
        self.assertTrue(closed["flag"])
        self.assertEqual(ctx.exception.status_code, 409)

    def test_ndjson_non_object_raises_value_error(self):
        resp = _FakeResponse(b"null\n")
        stream = NDJSONStream(resp, require_done=False)
        with self.assertRaises(ValueError):
            list(stream)
        self.assertTrue(resp.closed)

    def test_async_ingest_terminal_error_code_raises_api_error(self):
        responses = [
            _FakeResponse(json.dumps({"status_url": "/v1/ingest/batches/s/c/b1", "state": "accepted"}).encode(), status=202),
            _FakeResponse(json.dumps({
                "state": "failed",
                "result": {
                    "batch_id": "b1",
                    "failed": 1,
                    "error_code": "version_conflict",
                    "failures": [{"error": "stale version"}],
                },
            }).encode(), status=200),
        ]

        client = GraphDBClient("http://localhost:8080", tenant_id="t1")
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=responses):
            with self.assertRaises(GraphDBAPIError) as ctx:
                client.ingest({"source": "s", "collector_id": "c", "batch_id": "b1"})
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.code, "version_conflict")


if __name__ == "__main__":
    unittest.main()
