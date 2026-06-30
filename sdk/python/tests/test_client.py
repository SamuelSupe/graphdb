import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from graphdb_sdk import GraphDBAPIError, GraphDBClient


class Handler(BaseHTTPRequestHandler):
    requests = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.__class__.requests.append((self.command, self.path, dict(self.headers), body))
        if self.path == "/v1/commits":
            self._json({"version": 3, "readable_version": 3})
        elif self.path == "/v1/query/gql":
            self._json({"version": 9, "results": [], "stats": {"returned": 0}})
        elif self.path == "/v1/query/gql/stream":
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            self.wfile.write(b'{"stream":true}\n{"done":true}\n')
        elif self.path == "/v1/query":
            self.send_response(429)
            self.send_header("Retry-After", "2")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"code":"write_backpressure","message":"slow down","retryable":true}')
        else:
            self.send_error(404)

    def do_GET(self):
        self.__class__.requests.append((self.command, self.path, dict(self.headers), b""))
        if self.path == "/v1/entities/host%3A1":
            self._json({"id": "host:1", "kind": "host", "fields": {"hostname": "app-01"}})
        else:
            self.send_error(404)

    def log_message(self, *_):
        pass

    def _json(self, payload):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(timeout=5)

    def setUp(self):
        Handler.requests = []

    def test_commit_sets_tenant_header(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        result = client.commit({"upsert_entities": [{"id": "host:1", "kind": "host"}]}, idempotency_key="idem-1")
        self.assertEqual(result["version"], 3)
        method, path, headers, body = Handler.requests[-1]
        headers_lower = {key.lower(): value for key, value in headers.items()}
        self.assertEqual((method, path), ("POST", "/v1/commits"))
        self.assertEqual(headers_lower["x-tenant-id"], "tenant-a")
        self.assertEqual(json.loads(body)["idempotency_key"], "idem-1")

    def test_get_entity_escapes_id(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        entity = client.get_entity("host:1")
        self.assertEqual(entity["fields"]["hostname"], "app-01")

    def test_api_error(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        with self.assertRaises(GraphDBAPIError) as caught:
            client.query({"op": "match", "kind": "host"})
        self.assertEqual(caught.exception.code, "write_backpressure")
        self.assertEqual(caught.exception.retry_after_ms, 2000)
        self.assertTrue(caught.exception.retryable)

    def test_gql_and_stream(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        self.assertEqual(client.gql("FIND host LIMIT 1")["version"], 9)
        method, path, headers, body = Handler.requests[-1]
        self.assertEqual((method, path), ("POST", "/v1/query/gql"))
        self.assertEqual(headers["Content-Type"], "text/plain")
        self.assertEqual(body, b"FIND host LIMIT 1")
        with client.stream_gql("FIND host LIMIT 1") as stream:
            self.assertEqual(len(list(stream)), 2)


if __name__ == "__main__":
    unittest.main()
