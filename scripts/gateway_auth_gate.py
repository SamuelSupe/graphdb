#!/usr/bin/env python3
"""Check the production gateway against the isolated role fixture in gateway_gate.sh."""
import json
import os
from pathlib import Path
import ssl
import urllib.error
import urllib.request

base = os.environ['GRAPHDB_GATEWAY_TEST_URL']
output = Path(os.environ['GRAPHDB_GATEWAY_OUTPUT'])
context = ssl.create_default_context(cafile=os.environ['GRAPHDB_GATEWAY_TEST_CA'])
results = []


def request(method, path, role, body=None, expected=200):
    headers = {'Content-Type': 'application/json', 'X-Tenant-ID': 'spoofed-tenant',
               'X-Original-Method': 'DELETE', 'X-Original-URI': '/forged'}
    if role:
        headers['Authorization'] = f'Bearer {role}-{method.lower()}-test'
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, context=context, timeout=30) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    row = {'method': method, 'path': path, 'role': role, 'expected': expected, 'actual': status,
           'result': 'PASS' if status == expected else 'FAIL'}
    results.append(row)
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    assert status == expected, (row, raw[:500])
    return json.loads(raw) if raw.startswith(b'{') else None


request('POST', '/admin/v1/tenants', 'operator', {'tenant_id': 'tenant-a'})
mutation = {'mutations': {'upsert_entities': [{'id': 'host:gateway-auth', 'kind': 'host'}]}}
for path in (
    '/v1/commits', '/v1/%63ommits', '/%76%31/commits', '/v1/commits?test=1',
    '/v1/ingest/batches', '/v1/%69ngest/batches', '/v1/ingest/%62atches',
    '/v1/imports', '/v1/%69mports', '/%76%31/imports',
):
    request('POST', path, 'reader', mutation, expected=403)
request('POST', '/v1/%63ommits', None, mutation, expected=401)
request('GET', '/admin/v1/tenants', 'reader', expected=403)
request('GET', '/v1/tenants', 'reader', expected=404)
for path in ('/v1/commits', '/v1/%63ommits'):
    result = request('POST', path, 'writer', mutation)
    assert result['tenant_id'] == 'tenant-a', result
result = request('GET', '/v1/entities/host:gateway-auth', 'reader')
assert result['entity']['id'] == 'host:gateway-auth', result
print(f'Gateway role, encoded-route and tenant-isolation gate passed ({len(results)} requests)')
