#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-python3}"
export GRAPHDB_GATE_IMAGE="${GRAPHDB_GATE_IMAGE:?set the candidate Docker image}"
export GRAPHDB_GATE_OUTPUT="${GRAPHDB_GATE_OUTPUT:-$(mktemp -d "${TMPDIR:-/tmp}/graphdb-raft-gate.XXXXXX")}"
export GRAPHDB_GATE_PROJECT="${GRAPHDB_GATE_PROJECT:-graphdb-gate-$(date +%s)-$$}"
export GRAPHDB_RAFT_TOKEN="${GRAPHDB_RAFT_TOKEN:-$("$PYTHON" -c 'import secrets; print(secrets.token_hex(32))')}"
mkdir -p "$GRAPHDB_GATE_OUTPUT"
"$PYTHON" - <<'PY'
import datetime, hashlib, json, os, pathlib, subprocess
output = pathlib.Path(os.environ['GRAPHDB_GATE_OUTPUT'])
image = os.environ['GRAPHDB_GATE_IMAGE']
info = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image]))[0]
container = subprocess.check_output(['docker', 'create', image], text=True).strip()
try:
    binary = output / 'graphdb'
    subprocess.run(['docker', 'cp', container+':/usr/local/bin/graphdb', str(binary)], check=True)
    digest = hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest()
    binary.unlink()
finally:
    subprocess.run(['docker', 'rm', container], check=True, stdout=subprocess.DEVNULL)
metadata = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'image': image, 'image_id': info['Id'], 'binary_sha256': digest,
    'binary_version': subprocess.check_output(['docker', 'run', '--rm', image, 'version'], text=True).strip(),
    'image_labels': info['Config'].get('Labels'),
    'port_offset': int(os.environ.get('GRAPHDB_GATE_PORT_OFFSET', '0')),
    'cross_host_qualification': 'NOT RUN', 'result': 'RUNNING'}
(output / 'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
PY
finish() {
  local code=$?
  "$PYTHON" - "$code" <<'PY'
import datetime, json, os, pathlib, sys
path = pathlib.Path(os.environ['GRAPHDB_GATE_OUTPUT']) / 'metadata.json'
value = json.loads(path.read_text())
value.update(result='PASS' if sys.argv[1] == '0' else 'FAIL',
    finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
path.write_text(json.dumps(value, indent=2)+'\n')
PY
}
trap finish EXIT
"$PYTHON" scripts/raft_gate_dual.py
"$PYTHON" scripts/raft_gate_sharded.py
printf 'Raft and sharding gate passed. Evidence: %s\n' "$GRAPHDB_GATE_OUTPUT"
