#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
: "${GRAPHDB_GATE_IMAGE:?set the candidate Docker image}"
RUN_DIR="${GRAPHDB_GATEWAY_OUTPUT:-$(mktemp -d "${TMPDIR:-/tmp}/graphdb-gateway-gate.XXXXXX")}"
mkdir -p "$RUN_DIR/tls"
RUN_DIR="$(cd "$RUN_DIR" && pwd)"
export GRAPHDB_GATEWAY_OUTPUT="$RUN_DIR"
PROJECT="${GRAPHDB_GATEWAY_PROJECT:-graphdb-gateway-gate-$(date +%s)-$$}"
PORT="${GRAPHDB_GATEWAY_PORT:-38084}"
PYTHON="${PYTHON:-python3}"
cleanup() {
  local status=$?
  for service in data identity nginx; do
    docker logs "$PROJECT-$service" >"$RUN_DIR/$service.log" 2>&1 || true
    docker rm -f "$PROJECT-$service" >/dev/null 2>&1 || true
  done
  docker volume rm "$PROJECT-data" >/dev/null 2>&1 || true
  docker network rm "$PROJECT" >/dev/null 2>&1 || true
  rm -f "$RUN_DIR/tls/tls.key"
  return "$status"
}
trap cleanup EXIT
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=graphdb-gateway-test \
  -addext subjectAltName=IP:127.0.0.1 -keyout "$RUN_DIR/tls/tls.key" \
  -out "$RUN_DIR/tls/tls.crt" >"$RUN_DIR/tls-generation.log" 2>&1
chmod 600 "$RUN_DIR/tls/tls.key"
docker network create "$PROJECT" >/dev/null
docker volume create "$PROJECT-data" >/dev/null
docker run -d --name "$PROJECT-data" --network "$PROJECT" --network-alias graphdb \
  --cpus 2 --cpu-shares 4096 --memory 1g --memory-swap 1g \
  -e GOMAXPROCS=2 -e GRAPHDB_ADDR=:8080 -e GRAPHDB_ADMIN_ADDR=:8081 -e GRAPHDB_INGEST_MODE=direct \
  -v "$PROJECT-data:/var/lib/graphdb" "$GRAPHDB_GATE_IMAGE" >/dev/null
docker run -d --name "$PROJECT-identity" --network "$PROJECT" --network-alias identity-provider \
  --cpus 1 --cpu-shares 4096 --memory 256m --memory-swap 256m \
  -v "$ROOT/scripts/gateway-gate/identity.py:/identity.py:ro" \
  --entrypoint python3 golang:1.26.7-bookworm /identity.py >/dev/null
docker run -d --name "$PROJECT-nginx" --network "$PROJECT" -p "127.0.0.1:$PORT:443" \
  --cpus 1 --cpu-shares 4096 --memory 256m --memory-swap 256m \
  -e NGINX_ENTRYPOINT_WORKER_PROCESSES_AUTOTUNE=1 \
  -v "$ROOT/deploy/nginx/graphdb.conf.example:/etc/nginx/conf.d/default.conf:ro" \
  -v "$RUN_DIR/tls:/etc/nginx/tls:ro" nginx:1.28.0-bookworm >/dev/null
export GRAPHDB_GATEWAY_TEST_URL="https://127.0.0.1:$PORT"
export GRAPHDB_GATEWAY_TEST_CA="$RUN_DIR/tls/tls.crt"
curl --fail --cacert "$GRAPHDB_GATEWAY_TEST_CA" --retry 30 --retry-all-errors \
  --retry-delay 1 --max-time 5 -H 'Authorization: Bearer reader-get-test' \
  "$GRAPHDB_GATEWAY_TEST_URL/v1/health" >"$RUN_DIR/health.json"
"$PYTHON" scripts/gateway_auth_gate.py
"$PYTHON" - <<'PY'
import hashlib, json, os, pathlib, subprocess
image = os.environ['GRAPHDB_GATE_IMAGE']
metadata = {'result': 'PASS', 'image': image,
    'image_id': subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Id}}', image], text=True).strip(),
    'binary_sha256': subprocess.check_output(['docker', 'run', '--rm', '--entrypoint', 'sha256sum', image, '/usr/local/bin/graphdb'], text=True).split()[0],
    'gateway_config_sha256': hashlib.sha256(pathlib.Path('deploy/nginx/graphdb.conf.example').read_bytes()).hexdigest(),
    'identity_fixture_sha256': hashlib.sha256(pathlib.Path('scripts/gateway-gate/identity.py').read_bytes()).hexdigest(),
    'gate_client_sha256': hashlib.sha256(pathlib.Path('scripts/gateway_auth_gate.py').read_bytes()).hexdigest(),
    'identity_provider': 'disposable role fixture; no external identity integration qualified'}
pathlib.Path(os.environ['GRAPHDB_GATEWAY_OUTPUT'], 'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
PY
