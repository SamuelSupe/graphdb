"""Disposable identity-provider fixture; never deploy as a real identity service."""
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        identity = {f'Bearer {role}-{method.lower()}-test': (role, method)
                    for role in ('reader', 'writer', 'operator') for method in ('GET', 'POST')}
        role, method = identity.get(self.headers.get('Authorization'), (None, None))
        allowed = self.headers.get('X-GraphDB-Required-Roles', '').split(',')
        status = 401 if role is None else (204 if role in allowed else 403)
        if role is not None and (self.headers.get('X-Original-Method') != method
                                 or self.headers.get('X-Original-URI') in (None, '/forged')):
            status = 403
        self.send_response(status)
        if status == 204:
            self.send_header('X-GraphDB-Tenant-ID', 'tenant-a')
        self.send_header('Content-Length', '0')
        self.end_headers()


HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
