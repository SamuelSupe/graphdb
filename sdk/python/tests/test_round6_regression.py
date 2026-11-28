import io
import json
import unittest
from unittest.mock import patch

from graphdb_sdk import GraphDBClient


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


class Round6SDKRegressionTests(unittest.TestCase):
    def test_ingest_enforces_client_timeout_and_explicit_timeout(self):
        def fake_urlopen(req, timeout=None):
            if req.get_method() == "POST":
                return _FakeResponse(
                    json.dumps({"status_url": "/v1/ingest/batches/s/c/b1", "state": "accepted"}).encode(),
                    status=202,
                )
            return _FakeResponse(
                json.dumps({"state": "accepted"}).encode(),
                status=200,
            )

        client = GraphDBClient("http://localhost:8080", tenant_id="t1", timeout=0.05)
        with patch("graphdb_sdk.transport.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(TimeoutError):
                client.ingest({"source": "s", "collector_id": "c", "batch_id": "b1"})

        with patch("graphdb_sdk.transport.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(TimeoutError):
                client.ingest({"source": "s", "collector_id": "c", "batch_id": "b1"}, timeout=0.02)

        with self.assertRaises(ValueError):
            client.ingest({"source": "s", "collector_id": "c", "batch_id": "b1"}, timeout=-1.0)


if __name__ == "__main__":
    unittest.main()
