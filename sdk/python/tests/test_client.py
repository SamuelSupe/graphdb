import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import parse

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
        elif self.path == "/v1/query/graphql":
            self._json({"data": {"graph": {"version": 1}}})
        elif self.path == "/v1/query":
            self.send_response(429)
            self.send_header("Retry-After", "2")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"code":"write_backpressure","message":"slow down","retryable":true}')
        elif self.path.startswith("/v1/imports?"):
            self._json({"id": "task-import", "type": "bulk_import", "status": "queued"})
        else:
            self.send_error(404)

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.__class__.requests.append((self.command, self.path, dict(self.headers), body))
        if self.path == "/v1/source-policy":
            self._json({"configured": True, "policy": json.loads(body)})
        elif self.path == "/v1/relation-schemas/cites":
            self._json({"revision": 2, "relation_schemas": [json.loads(body)]})
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


class IngestHandler(BaseHTTPRequestHandler):
    scenario = ""
    post_count = 0
    status_count = 0
    bodies = []
    prefer_headers = []
    status_path = "/v1/ingest/writers/writer-a/agent/collector-a/wal-batch"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.__class__.post_count += 1
        self.__class__.bodies.append(json.loads(body))
        self.__class__.prefer_headers.append(self.headers.get("Prefer"))
        if self.path != "/v1/ingest/batches":
            self.send_error(404)
            return
        if self.scenario == "submit_wait":
            self.send_response(202)
            self.send_header("Location", self.status_path)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                b'{"writer_id":"writer-a","batch_id":"batch-1","state":"accepted",'
                b'"durability":"durable","accepted_at":"2026-09-02T10:00:00Z",'
                b'"estimated_flush_at":"2026-09-02T10:00:01Z"}'
            )
            return
        if self.scenario == "direct_and_wal":
            if self.post_count == 1:
                self._json(200, {"batch_id": "direct-200", "version": 3, "applied": 1, "failed": 0})
            elif self.post_count == 2:
                self._json(
                    207,
                    {
                        "batch_id": "direct-207",
                        "version": 4,
                        "applied": 1,
                        "failed": 1,
                        "error_code": "precondition_failed",
                        "failures": [{"index": 1, "error": "state changed"}],
                    },
                )
            elif self.post_count == 3:
                self.send_response(202)
                self.send_header("Location", self.status_path)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    b'{"writer_id":"writer-a","batch_id":"wal-batch","state":"accepted",'
                    b'"durability":"durable","status_url":"/v1/ingest/writers/writer-a/'
                    b'agent/collector-a/wal-batch","accepted_at":"2026-09-02T10:00:00Z",'
                    b'"estimated_flush_at":"2026-09-02T10:00:01Z"}'
                )
            else:
                self.send_error(500)
            return
        if self.scenario == "error":
            self._json(
                412,
                {
                    "error": "state changed",
                    "code": "precondition_failed",
                    "message": "state changed",
                    "retryable": False,
                },
            )
            return
        self.send_error(500)

    def do_GET(self):
        if self.path != self.status_path:
            self.send_error(404)
            return
        self.__class__.status_count += 1
        if self.scenario == "submit_wait" and self.status_count == 1:
            self._json(
                200,
                {
                    "tenant_id": "tenant-a",
                    "writer_id": "writer-a",
                    "source": "agent",
                    "collector_id": "collector-a",
                    "batch_id": "batch-1",
                    "state": "accepted",
                    "durability": "durable",
                    "recovery_pending": True,
                },
            )
            return
        self._json(
            200,
            {
                "tenant_id": "tenant-a",
                "writer_id": "writer-a",
                "source": "agent",
                "collector_id": "collector-a",
                "batch_id": "batch-1" if self.scenario == "submit_wait" else "wal-batch",
                "state": "committed",
                "durability": "durable",
                "result": {
                    "batch_id": "batch-1" if self.scenario == "submit_wait" else "wal-batch",
                    "version": 12 if self.scenario == "submit_wait" else 5,
                    "applied": 1,
                    "failed": 0,
                },
            },
        )

    def log_message(self, *_):
        pass

    def _json(self, status, payload):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))


def start_ingest_server(test_case, scenario):
    class ScenarioHandler(IngestHandler):
        pass

    ScenarioHandler.scenario = scenario
    ScenarioHandler.post_count = 0
    ScenarioHandler.status_count = 0
    ScenarioHandler.bodies = []
    ScenarioHandler.prefer_headers = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), ScenarioHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    test_case.addCleanup(server.server_close)
    test_case.addCleanup(thread.join, 5)
    test_case.addCleanup(server.shutdown)
    return server, ScenarioHandler


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.saved_no_proxy = {
            "NO_PROXY": os.environ.get("NO_PROXY"),
            "no_proxy": os.environ.get("no_proxy"),
        }
        os.environ["NO_PROXY"] = "127.0.0.1,localhost"
        os.environ["no_proxy"] = "127.0.0.1,localhost"
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(timeout=5)
        for name, value in cls.saved_no_proxy.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def setUp(self):
        Handler.requests = []

    def test_commit_sets_tenant_header(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a", bearer_token="test-token")
        result = client.commit({"upsert_entities": [{"id": "host:1", "kind": "host"}]}, idempotency_key="idem-1")
        self.assertEqual(result["version"], 3)
        method, path, headers, body = Handler.requests[-1]
        headers_lower = {key.lower(): value for key, value in headers.items()}
        self.assertEqual((method, path), ("POST", "/v1/commits"))
        self.assertEqual(headers_lower["x-tenant-id"], "tenant-a")
        self.assertEqual(headers_lower["user-agent"], "graphdb-python-sdk/1.3.1")
        self.assertEqual(headers_lower["authorization"], "Bearer test-token")
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

    def test_graphql(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        response = client.graphql(
            "query Version($request: QueryRequest!) { graph(request: $request) { version } }",
            {"request": {"op": "match"}},
            "Version",
        )
        self.assertEqual(response["data"]["graph"]["version"], 1)
        method, path, _, body = Handler.requests[-1]
        self.assertEqual((method, path), ("POST", "/v1/query/graphql"))
        self.assertEqual(json.loads(body)["operationName"], "Version")

    def test_source_policy_field_aliases_are_sent(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        policy = {
            "default_priority": 0,
            "sources": [{"name": "agent", "priority": 100}],
            "field_aliases": [{"source": "agent", "aliases": {"host_name": "hostname"}}],
            "field_priorities": [{"source": "agent", "fields": {"hostname": 150}}],
        }
        result = client.put_source_policy(policy)
        self.assertEqual(result["policy"]["field_aliases"][0]["aliases"]["host_name"], "hostname")
        self.assertEqual(result["policy"]["field_priorities"][0]["fields"]["hostname"], 150)
        method, path, headers, body = Handler.requests[-1]
        self.assertEqual((method, path), ("PUT", "/v1/source-policy"))
        sent = json.loads(body)
        self.assertEqual(sent["field_aliases"][0]["aliases"]["host_name"], "hostname")
        self.assertEqual(sent["field_priorities"][0]["fields"]["hostname"], 150)

    def test_version_11_import_and_relation_schema(self):
        client = GraphDBClient(self.base_url, tenant_id="tenant-a")
        task = client.start_import('{"entity":{}}\n', "jsonl", batch_size=25)
        self.assertEqual(task["id"], "task-import")
        method, path, headers, body = Handler.requests[-1]
        parsed = parse.urlparse(path)
        self.assertEqual((method, parsed.path), ("POST", "/v1/imports"))
        self.assertEqual(parse.parse_qs(parsed.query)["batch_size"], ["25"])
        self.assertEqual(headers["Content-Type"], "application/x-ndjson")
        self.assertEqual(body, b'{"entity":{}}\n')

        catalog = client.put_relation_schema("cites", {"relation_type": "cites", "strict": True})
        self.assertEqual(catalog["revision"], 2)

    def test_submit_ingest_acceptance_location_owner_status_and_wait(self):
        server, handler = start_ingest_server(self, "submit_wait")
        client = GraphDBClient(f"http://127.0.0.1:{server.server_port}", tenant_id="tenant-a")
        batch = {
            "source": "agent",
            "collector_id": "collector-a",
            "batch_id": "batch-1",
            "expected_version": 7,
            "failure_mode": "atomic",
            "preconditions": [
                {"resource_type": "entity", "id": "host:1", "field": "state", "op": "eq", "value": "ready"}
            ],
            "items": [{"external_id": "host-1", "entity": {"id": "host:1", "kind": "host"}}],
        }
        acceptance = client.submit_ingest(batch)
        self.assertEqual(acceptance["writer_id"], "writer-a")
        self.assertEqual(acceptance["state"], "accepted")
        self.assertEqual(acceptance["status_url"], handler.status_path)
        self.assertEqual(handler.prefer_headers, [None])
        self.assertEqual(handler.bodies[0]["expected_version"], 7)
        self.assertEqual(handler.bodies[0]["failure_mode"], "atomic")
        self.assertEqual(handler.bodies[0]["preconditions"][0]["op"], "eq")

        active = client.get_ingest_status(acceptance["status_url"])
        self.assertEqual(active["state"], "accepted")
        self.assertTrue(active["recovery_pending"])
        terminal = client.wait_ingest(acceptance, poll_interval=0.001, timeout=1)
        self.assertEqual(terminal["state"], "committed")
        self.assertEqual(terminal["result"]["version"], 12)
        self.assertEqual(handler.status_count, 2)

    def test_ingest_waits_for_wal_and_accepts_direct_200_and_207(self):
        server, handler = start_ingest_server(self, "direct_and_wal")
        client = GraphDBClient(f"http://127.0.0.1:{server.server_port}", tenant_id="tenant-a")
        batch = {
            "source": "agent",
            "collector_id": "collector-a",
            "items": [{"external_id": "host-1", "entity": {"id": "host:1", "kind": "host"}}],
        }
        direct = client.ingest(batch)
        self.assertEqual(direct["version"], 3)
        self.assertEqual(direct["failed"], 0)
        partial = client.ingest(batch)
        self.assertEqual(partial["version"], 4)
        self.assertEqual(partial["failed"], 1)
        self.assertEqual(partial["error_code"], "precondition_failed")
        wal = client.ingest(batch)
        self.assertEqual(wal["version"], 5)
        self.assertEqual(wal["failed"], 0)
        self.assertEqual(handler.prefer_headers, ["wait=committed"] * 3)
        self.assertEqual(handler.status_count, 1)

    def test_ingest_raises_structured_api_error(self):
        server, handler = start_ingest_server(self, "error")
        client = GraphDBClient(f"http://127.0.0.1:{server.server_port}", tenant_id="tenant-a")
        with self.assertRaises(GraphDBAPIError) as caught:
            client.ingest({"source": "agent", "collector_id": "collector-a", "items": []})
        self.assertEqual(caught.exception.status_code, 412)
        self.assertEqual(caught.exception.code, "precondition_failed")
        self.assertEqual(caught.exception.message, "state changed")
        self.assertEqual(handler.prefer_headers, ["wait=committed"])

    def test_wait_ingest_rejects_untrusted_urls(self):
        client = GraphDBClient("http://127.0.0.1:1", tenant_id="tenant-a")
        for status_url in (
            "https://other.example/v1/ingest/batches/agent/collector/batch",
            "/v1/query/graphql",
            "/v1/ingest/batches/agent/collector/batch?tenant=other",
        ):
            with self.assertRaises(ValueError):
                client.get_ingest_status(status_url)


if __name__ == "__main__":
    unittest.main()
