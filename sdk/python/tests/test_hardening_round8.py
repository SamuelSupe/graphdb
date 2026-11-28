from __future__ import annotations

import http.server
import json
import ssl
import threading
import time
import unittest

from graphdb_sdk import GraphDBClient
from graphdb_sdk.errors import ERROR_CODE_WRITE_ADMISSION_TIMEOUT


class _HardeningHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    methods_seen: list[tuple[str, str]] = []
    poll_count = 0

    def log_message(self, format, *args):
        return

    def _send_json(self, status: int, body: dict, headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.__class__.methods_seen.append(("GET", self.path))
        if self.path == "/v1/health":
            self._send_json(200, {"status": "ok"})
            return
        if self.path.startswith("/v1/control/reader-fleet-readiness"):
            self._send_json(200, {"ready": True})
            return
        if self.path.startswith("/v1/control/reader-traffic-gate"):
            self._send_json(200, {"serve_traffic": True})
            return
        if self.path.startswith("/v1/ingest/batches/"):
            self.__class__.poll_count += 1
            if self.__class__.poll_count == 1:
                self._send_json(
                    429,
                    {"code": "too_many_requests", "message": "busy", "retryable": True, "retry_after_ms": 10},
                    headers={"Retry-After": "1"},
                )
                return
            self._send_json(
                200,
                {
                    "state": "committed",
                    "result": {"batch_id": "b1", "version": 2, "applied": 1, "failed": 0},
                },
            )
            return
        self._send_json(404, {"code": "not_found", "message": "not found"})

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        self.__class__.methods_seen.append(("PUT", self.path))
        self._send_json(200, {"tenant_id": "t1", **payload})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        _ = self.rfile.read(length) if length else b""
        self.__class__.methods_seen.append(("POST", self.path))
        self._send_json(200, {"version": 1})


class TestHardeningRound8(unittest.TestCase):
    def setUp(self):
        _HardeningHandler.methods_seen = []
        _HardeningHandler.poll_count = 0
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _HardeningHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_update_tenant_uses_put(self):
        with GraphDBClient(self.base_url) as client:
            res = client.update_tenant("t1", name="Updated")
            self.assertEqual(res["name"], "Updated")
        self.assertIn(("PUT", "/v1/tenants/t1"), _HardeningHandler.methods_seen)

    def test_for_tenant_close_does_not_close_parent_pool(self):
        with GraphDBClient(self.base_url) as parent:
            self.assertEqual(parent.health()["status"], "ok")
            with parent.for_tenant("t1") as child:
                self.assertEqual(child.health()["status"], "ok")
            self.assertFalse(parent._pool._closed)
            self.assertEqual(parent.health()["status"], "ok")

    def test_stale_idle_connection_evicted_before_post(self):
        with GraphDBClient(self.base_url, tenant_id="t1") as client:
            self.assertEqual(client.health()["status"], "ok")
            # Force pooled connection to appear expired by idle TTL.
            with client._pool._lock:
                self.assertGreaterEqual(len(client._pool._idle), 1)
                conn, _ = client._pool._idle[-1]
                client._pool._idle[-1] = (conn, time.monotonic() - 120.0)
            res = client.commit({"upsert_entities": []})
            self.assertEqual(res["version"], 1)

    def test_write_admission_error_code_matches_server_contract(self):
        self.assertEqual(ERROR_CODE_WRITE_ADMISSION_TIMEOUT, "write_admission_queue_timeout")

    def test_invalid_port_rejected_eagerly(self):
        with self.assertRaises(ValueError):
            GraphDBClient("http://127.0.0.1:notaport")

    def test_ops_reader_fleet_and_traffic_gate_and_wait_ingest_retry(self):
        with GraphDBClient(self.base_url, tenant_id="t1") as client:
            self.assertTrue(client.reader_fleet_readiness(min_ready=1, min_version=5)["ready"])
            self.assertTrue(client.reader_traffic_gate(allow_stale=False, target_version=5)["serve_traffic"])
            status = client.wait_ingest("/v1/ingest/batches/src/col/b1", poll_interval=0.01, timeout=2.0)
            self.assertEqual(status["state"], "committed")
            self.assertEqual(_HardeningHandler.poll_count, 2)
            # 5-segment writer URL validation must succeed.
            self.assertEqual(
                client._ingest_status_path("/v1/ingest/writers/w1/batches/src/col/b1"),
                "/v1/ingest/writers/w1/batches/src/col/b1",
            )
            # Non-positive timeout must be rejected explicitly instead of switching to non-blocking mode.
            with self.assertRaises(ValueError):
                client.wait_ingest("/v1/ingest/batches/src/col/b1", timeout=0)
            with self.assertRaises(ValueError):
                client.ingest({"source": "s", "collector_id": "c", "batch_id": "b1"}, timeout=0)

    def test_https_ssl_eof_retry_uses_fresh_connection_and_retries_readonly_query(self):
        class _DeadSSLConn:
            def __init__(self):
                self.closed = False

            def request(self, *args, **kwargs):
                raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol")

            def close(self):
                self.closed = True

        with GraphDBClient(self.base_url, tenant_id="t1") as client:
            # Place TWO dead connections in the pool; attempt 1 must bypass the
            # pool and open a fresh connection instead of grabbing the second dead socket.
            dead1, dead2 = _DeadSSLConn(), _DeadSSLConn()
            with client._pool._lock:
                client._pool._idle.append((dead1, time.monotonic()))
                client._pool._idle.append((dead2, time.monotonic()))
            self.assertEqual(client.health()["status"], "ok")
            self.assertTrue(dead2.closed)
            # Now put a dead socket at the top again and verify read-only POST /v1/query retries cleanly.
            dead3 = _DeadSSLConn()
            with client._pool._lock:
                client._pool._idle.append((dead3, time.monotonic()))
            res = client.query({"op": "match", "kind": "host"})
            self.assertEqual(res["version"], 1)
            self.assertTrue(dead3.closed)


if __name__ == "__main__":
    unittest.main()
