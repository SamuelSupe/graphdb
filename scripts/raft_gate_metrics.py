"""Capture and check actual local diagnostic endpoints during deployment gates."""
import json
import math
import urllib.request


def collect(base, output, name, expected, headers=None):
    headers = headers or {}
    with urllib.request.urlopen(urllib.request.Request(base+'/metrics', headers=headers), timeout=3) as response:
        raw = response.read()
        assert response.status == 200
    samples = {}
    for line in raw.decode().splitlines():
        if not line or line.startswith('#'):
            continue
        key, value = line.rsplit(' ', 1)
        assert key not in samples, ('duplicate metric sample', key)
        samples[key] = float(value)
        assert math.isfinite(samples[key]), ('nonfinite metric sample', key)
    for key, value in expected.items():
        assert key in samples and (value is None or samples[key] == value), (name, key, samples.get(key), value)
    assert samples['graphdb_go_goroutines'] > 0
    assert samples['graphdb_go_heap_objects_bytes'] > 0
    (output/(name+'.prom')).write_bytes(raw)
    with urllib.request.urlopen(urllib.request.Request(base+'/v1/diagnostics', headers=headers), timeout=3) as response:
        diagnostic = json.load(response)
        assert response.status == 200
    (output/(name+'.json')).write_text(json.dumps(diagnostic, indent=2)+'\n')
    return {'name': name, 'samples': len(samples), 'deployment': diagnostic['deployment']}
