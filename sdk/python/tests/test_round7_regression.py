import io
import json
import unittest
from unittest.mock import patch
from urllib import error

from graphdb_sdk import GraphDBAPIError, GraphDBClient


class _FakeResponse:
    def __init__(self, body: bytes, status: int = 200, headers=None):
        self._buf = io.BytesIO(body)
        self.status = status
        self.headers = headers or {}

    def read(self):
        return self._buf.read()

    def getcode(self):
        return self.status

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class Round7SDKRegressionTests(unittest.TestCase):
    def test_ingest_and_wait_ingest_propagate_timeout_to_urlopen(self):
        seen_timeouts = []

        def fake_urlopen(req, timeout=None):
            seen_timeouts.append((req.get_method(), timeout))
            if req.get_method() == "POST":
                return _FakeResponse(
                    json.dumps({"status_url": "/v1/ingest/batches/s/c/b1", "state": "accepted"}).encode(),
                    status=202,
                )
            return _FakeResponse(
                json.dumps({"state": "committed", "result": {"batch_id": "b1", "applied": 1}}).encode(),
                status=200,
            )

        client = GraphDBClient("http://localhost:8080", tenant_id="t1", timeout=None)
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=fake_urlopen):
            result = client.ingest({"source": "s", "collector_id": "c", "batch_id": "b1"}, timeout=3.5)

        self.assertEqual(result["applied"], 1)
        self.assertEqual(len(seen_timeouts), 2)
        self.assertEqual(seen_timeouts[0][0], "POST")
        self.assertEqual(seen_timeouts[0][1], 3.5)
        self.assertEqual(seen_timeouts[1][0], "GET")
        self.assertIsNotNone(seen_timeouts[1][1])
        self.assertGreater(seen_timeouts[1][1], 0)
        self.assertLessEqual(seen_timeouts[1][1], 3.5)

    def test_error_decoding_handles_non_utf8_and_empty_redirect_reason(self):
        client = GraphDBClient("http://localhost:8080", tenant_id="t1")

        # Non-UTF-8 error body must decode cleanly to GraphDBAPIError instead of UnicodeDecodeError.
        bad_utf8_err = error.HTTPError(
            "http://localhost:8080/v1/query",
            502,
            "Bad Gateway",
            None,
            io.BytesIO(b"\xff\xfeproxy error"),
        )
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=bad_utf8_err):
            with self.assertRaises(GraphDBAPIError) as ctx:
                client.query({"op": "match"})
        self.assertEqual(ctx.exception.status_code, 502)
        self.assertIn("proxy error", ctx.exception.message)

        # Empty 3xx redirect body must preserve exc.reason.
        redirect_err = error.HTTPError(
            "http://localhost:8080/v1/query",
            302,
            "HTTP redirects are disabled (302 -> http://evil.example)",
            None,
            io.BytesIO(b""),
        )
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=redirect_err):
            with self.assertRaises(GraphDBAPIError) as ctx:
                client.query({"op": "match"})
        self.assertEqual(ctx.exception.status_code, 302)
        self.assertIn("HTTP redirects are disabled", ctx.exception.message)


if __name__ == "__main__":
    unittest.main()
