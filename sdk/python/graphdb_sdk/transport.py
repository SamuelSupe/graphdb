from __future__ import annotations

import datetime
import email.utils
import http.client
import json
import selectors
import socket
import ssl
import threading
import time
from typing import Any
from urllib import error, parse, request

from .errors import GraphDBAPIError
from .stream import NDJSONStream

_MAX_ERROR_BODY_BYTES = 1 << 20
_DEFAULT_IDLE_TTL_SECONDS = 60.0


class _NoRedirectHandler(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise error.HTTPError(req.full_url, code, f"HTTP redirects are disabled ({msg})", headers, fp)


_NO_REDIRECT_OPENER = request.build_opener(_NoRedirectHandler())
_DEFAULT_URLOPEN = request.urlopen


def _socket_looks_closed(conn: http.client.HTTPConnection) -> bool:
    sock = getattr(conn, "sock", None)
    if sock is None:
        return True
    try:
        if sock.fileno() < 0:
            return True
    except Exception:
        return True
    # TLS 1.3 servers send post-handshake NewSessionTicket records that make
    # healthy idle SSLSockets appear readable; rely on idle_ttl + EOF retry.
    if isinstance(sock, ssl.SSLSocket):
        return False
    try:
        with selectors.DefaultSelector() as sel:
            sel.register(sock, selectors.EVENT_READ)
            return bool(sel.select(timeout=0))
    except Exception:
        return True


class _ConnectionPool:
    def __init__(self, maxsize: int = 16, idle_ttl: float = _DEFAULT_IDLE_TTL_SECONDS) -> None:
        self._lock = threading.Lock()
        self._idle: list[tuple[http.client.HTTPConnection, float]] = []
        self._closed = False
        self._maxsize = maxsize
        self._idle_ttl = idle_ttl

    def acquire(self, timeout: float | None) -> http.client.HTTPConnection | None:
        if timeout is not None and timeout <= 0:
            raise TimeoutError("request timed out")
        now = time.monotonic()
        stale: list[http.client.HTTPConnection] = []
        chosen: http.client.HTTPConnection | None = None
        while True:
            with self._lock:
                if self._closed or not self._idle:
                    break
                conn, last_used = self._idle.pop()
            if (now - last_used) > self._idle_ttl or _socket_looks_closed(conn):
                stale.append(conn)
                continue
            chosen = conn
            break
        for conn in stale:
            try:
                conn.close()
            except Exception:
                pass
        if chosen is not None:
            try:
                if chosen.sock is not None:
                    chosen.sock.settimeout(timeout)
                chosen.timeout = timeout
            except Exception:
                try:
                    chosen.close()
                except Exception:
                    pass
                return None
        return chosen

    def release(self, conn: http.client.HTTPConnection) -> None:
        keep = False
        with self._lock:
            if not self._closed and len(self._idle) < self._maxsize:
                self._idle.append((conn, time.monotonic()))
                keep = True
        if not keep:
            try:
                conn.close()
            except Exception:
                pass

    def close(self) -> None:
        with self._lock:
            self._closed = True
            conns = [conn for conn, _ in self._idle]
            self._idle.clear()
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass


class _PooledResponse:
    def __init__(
        self,
        pool: _ConnectionPool,
        conn: http.client.HTTPConnection,
        resp: http.client.HTTPResponse,
    ) -> None:
        self._pool = pool
        self._conn = conn
        self._resp = resp
        self._closed = False
        self.headers = resp.headers

    def getcode(self) -> int:
        return self._resp.status

    def read(self, amt: int | None = None) -> bytes:
        return self._resp.read(amt)

    def readline(self, limit: int = -1) -> bytes:
        return self._resp.readline(limit)

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        line = self._resp.readline()
        if not line:
            raise StopIteration
        return line

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        reusable = False
        try:
            conn_hdr = (self._resp.getheader("Connection") or "").lower()
            reusable = (
                not self._resp.will_close
                and conn_hdr != "close"
                and self._resp.isclosed()
                and self._conn.sock is not None
            )
        except Exception:
            reusable = False
        if reusable:
            self._pool.release(self._conn)
            return
        try:
            self._resp.close()
        finally:
            try:
                self._conn.close()
            except Exception:
                pass

    def __enter__(self) -> "_PooledResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class TransportMixin:
    def health(self) -> dict:
        return self._json("GET", "/v1/health", tenant_id=None)

    def _json(
        self,
        method: str,
        path: str,
        tenant_id: str | None = "",
        query: dict | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> dict:
        payload, _, _ = self._json_with_meta(method, path, tenant_id, query, body, headers, timeout=timeout)
        return payload

    def _json_with_meta(
        self,
        method: str,
        path: str,
        tenant_id: str | None = "",
        query: dict | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> tuple[dict, int, dict[str, str]]:
        data = None
        request_headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request_headers.update(headers or {})
        with self._open(method, path, tenant_id, query, data, request_headers, timeout=timeout) as response:
            payload = response.read()
            status = response.getcode()
            response_headers = dict(response.headers.items())
        decoded = json.loads(payload.decode("utf-8")) if payload else {}
        return decoded, status, response_headers

    def _text_json(self, method: str, path: str, text: str) -> dict:
        headers = {"Accept": "application/json", "Content-Type": "text/plain"}
        with self._open(method, path, "", None, text.encode("utf-8"), headers) as response:
            return json.loads(response.read().decode("utf-8"))

    def _raw_json(self, method: str, path: str, data: bytes, content_type: str, query: dict | None = None) -> dict:
        headers = {"Accept": "application/json", "Content-Type": content_type}
        with self._open(method, path, "", query, data, headers) as response:
            payload = response.read()
        return json.loads(payload.decode("utf-8")) if payload else {}

    def _stream(self, method: str, path: str, body: Any = None, text: str | None = None) -> NDJSONStream:
        headers = {"Accept": "application/x-ndjson"}
        data = None
        if text is not None:
            data = text.encode("utf-8")
            headers["Content-Type"] = "text/plain"
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        return NDJSONStream(self._open(method, path, "", None, data, headers), require_done=True)

    def _open(
        self,
        method: str,
        path: str,
        tenant_id: str | None,
        query: dict | None,
        data: bytes | None,
        headers: dict,
        timeout: float | None = None,
    ):
        req_headers = dict(self.headers)
        req_headers.update(headers)
        effective_tenant = self.tenant_id if tenant_id == "" else tenant_id
        if tenant_id is None or effective_tenant:
            for existing_key in [k for k in req_headers if k.lower() == "x-tenant-id"]:
                del req_headers[existing_key]
        if effective_tenant:
            req_headers["X-Tenant-ID"] = effective_tenant
        effective_timeout = self.timeout if timeout is None else timeout
        if effective_timeout is not None and effective_timeout <= 0:
            raise TimeoutError("request timed out")
        if request.urlopen is not _DEFAULT_URLOPEN:
            req = request.Request(self._url(path, query), data=data, headers=req_headers, method=method)
            try:
                return request.urlopen(req, timeout=effective_timeout)
            except error.HTTPError as exc:
                raise self._api_error(exc) from None
            except error.URLError as exc:
                if isinstance(exc.reason, TimeoutError):
                    raise TimeoutError(str(exc.reason)) from None
                raise
        if self._uses_env_proxy():
            req = request.Request(self._url(path, query), data=data, headers=req_headers, method=method)
            opener = (
                request.build_opener(_NoRedirectHandler(), request.HTTPSHandler(context=self._ssl_context))
                if self._ssl_context is not None
                else _NO_REDIRECT_OPENER
            )
            try:
                return opener.open(req, timeout=effective_timeout)
            except error.HTTPError as exc:
                raise self._api_error(exc) from None
            except error.URLError as exc:
                if isinstance(exc.reason, TimeoutError):
                    raise TimeoutError(str(exc.reason)) from None
                raise
        return self._pooled_open(method, path, query, data, req_headers, effective_timeout)

    def _uses_env_proxy(self) -> bool:
        proxies = request.getproxies()
        scheme = self._parsed_base.scheme
        if not proxies or scheme not in proxies:
            return False
        host = self._parsed_base.hostname or ""
        if host and request.proxy_bypass(host):
            return False
        return True

    def _new_connection(self, timeout: float | None) -> http.client.HTTPConnection:
        parsed = self._parsed_base
        conn_timeout = socket._GLOBAL_DEFAULT_TIMEOUT if timeout is None else timeout
        if parsed.scheme == "https":
            return http.client.HTTPSConnection(
                parsed.hostname,
                parsed.port,
                timeout=conn_timeout,
                context=self._ssl_context,
            )
        return http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=conn_timeout)

    _STALE_RETRYABLE_POST_PATHS = frozenset(
        {
            "/v1/query",
            "/v1/query/graphql",
            "/v1/query/gql",
            "/v1/query/stream",
            "/v1/query/gql/stream",
        }
    )

    def _pooled_open(
        self,
        method: str,
        path: str,
        query: dict | None,
        data: bytes | None,
        headers: dict[str, str],
        timeout: float | None,
    ) -> _PooledResponse:
        base_path = self._parsed_base.path.rstrip("/")
        encoded = self._encode_query(query or {})
        target = f"{base_path}{path}" + (f"?{encoded}" if encoded else "")
        idempotent_retry = method in ("GET", "HEAD") or (
            method == "POST" and path in self._STALE_RETRYABLE_POST_PATHS
        )

        for attempt in range(2):
            conn = self._pool.acquire(timeout) if attempt == 0 else None
            reused = conn is not None
            if conn is None:
                conn = self._new_connection(timeout)

            try:
                conn.request(method, target, body=data, headers=headers)
                resp = conn.getresponse()
            except (
                http.client.RemoteDisconnected,
                http.client.BadStatusLine,
                BrokenPipeError,
                ConnectionResetError,
                ssl.SSLEOFError,
                ssl.SSLZeroReturnError,
            ):
                try:
                    conn.close()
                except Exception:
                    pass
                if reused and idempotent_retry and attempt == 0:
                    continue
                raise
            except (TimeoutError, socket.timeout) as exc:
                try:
                    conn.close()
                except Exception:
                    pass
                raise TimeoutError(str(exc)) from None
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                raise

            if resp.status >= 300:
                reusable = False
                try:
                    body = resp.read(_MAX_ERROR_BODY_BYTES)
                    conn_hdr = (resp.headers.get("Connection") or "").lower()
                    reusable = (
                        resp.status >= 400
                        and not resp.will_close
                        and "close" not in conn_hdr
                        and resp.isclosed()
                    )
                finally:
                    resp.close()
                    if reusable:
                        self._pool.release(conn)
                    else:
                        conn.close()
                if 300 <= resp.status < 400:
                    raise GraphDBAPIError(
                        status_code=resp.status,
                        message=f"HTTP redirects are disabled ({resp.reason})",
                        body=body,
                    )
                raise self._api_error_from_parts(resp.status, resp.reason, resp.headers, body)
            return _PooledResponse(self._pool, conn, resp)
        raise RuntimeError("unreachable")

    def _url(self, path: str, query: dict | None = None) -> str:
        encoded = self._encode_query(query or {})
        return f"{self.base_url}{path}" + (f"?{encoded}" if encoded else "")

    def _encode_query(self, query: dict) -> str:
        clean_query = {
            key: str(value).lower() if isinstance(value, bool) else value
            for key, value in query.items()
            if value is not None and value != ""
        }
        return parse.urlencode(clean_query, doseq=True)

    def _api_error(self, exc: error.HTTPError) -> GraphDBAPIError:
        try:
            body = exc.read(_MAX_ERROR_BODY_BYTES)
        finally:
            exc.close()
        return self._api_error_from_parts(exc.code, str(exc.reason or ""), exc.headers, body)

    def _api_error_from_parts(self, status: int, reason: str, headers: Any, body: bytes) -> GraphDBAPIError:
        envelope = self._decode_error(body)
        retry_after_ms = envelope.get("retry_after_ms")
        if retry_after_ms is None and headers and headers.get("Retry-After"):
            retry_after_ms = self._parse_retry_after_ms(headers["Retry-After"])
        return GraphDBAPIError(
            status_code=status,
            code=envelope.get("code", ""),
            message=envelope.get("message") or envelope.get("error") or reason,
            retryable=bool(envelope.get("retryable", False)),
            retry_after_ms=retry_after_ms,
            detail=envelope.get("detail"),
            reasons=envelope.get("reasons"),
            body=body,
        )

    def _parse_retry_after_ms(self, raw: str) -> int | None:
        raw = str(raw).strip()
        if not raw:
            return None
        try:
            seconds = int(raw)
            return max(seconds, 0) * 1000
        except ValueError:
            pass
        try:
            when = email.utils.parsedate_to_datetime(raw)
            if when.tzinfo is None:
                when = when.replace(tzinfo=datetime.timezone.utc)
            delta = (when - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            return int(delta * 1000) if delta > 0 else None
        except (TypeError, ValueError):
            return None

    def _decode_error(self, body: bytes) -> dict:
        if not body:
            return {}
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {"message": body.decode("utf-8", errors="replace")}
        if isinstance(decoded, dict):
            return decoded
        return {"message": body.decode("utf-8", errors="replace")}
