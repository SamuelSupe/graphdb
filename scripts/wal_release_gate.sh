#!/usr/bin/env bash
set -Eeuo pipefail

# Real-process WAL/metadata-segment release gate. The base RustFS compose file
# intentionally keeps the product defaults (direct/legacy); this gate overlays
# the explicit 1.1 WAL mode so a release cannot accidentally pass with a direct
# writer. The generated evidence is the contract consumed by release.yml.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROJECT="${WAL_GATE_PROJECT:-graphdb-wal-gate-${RANDOM}-${RANDOM}}"
WRITER_PORT="${WAL_GATE_GRAPHDB_PORT:-38180}"
READER_PORT="${WAL_GATE_READER_PORT:-38181}"
RUSTFS_PORT="${WAL_GATE_RUSTFS_PORT:-39100}"
WRITER_URL="${WAL_GATE_WRITER_URL:-http://127.0.0.1:${WRITER_PORT}}"
READER_URL="${WAL_GATE_READER_URL:-http://127.0.0.1:${READER_PORT}}"
RUSTFS_URL="${WAL_GATE_RUSTFS_URL:-http://127.0.0.1:${RUSTFS_PORT}}"
REPORT="${GRAPHDB_TEST_WAL_RELEASE_REPORT:-${WAL_GATE_REPORT:-artifacts/wal-recovery.json}}"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_ROOT="${TMP_ROOT%/}"
RUN_DIR="${WAL_GATE_RUN_DIR:-$TMP_ROOT/graphdb-wal-release-gate-${PROJECT}}"
OVERRIDE="$RUN_DIR/compose.override.yml"
WINDOWS="$RUN_DIR/windows.ndjson"
FAIL_REASON=""
REPORT_WRITTEN=0
RUN_DIR_CREATED=0

fail() {
  FAIL_REASON="$*"
  echo "wal release gate: $FAIL_REASON" >&2
  exit 1
}

if [[ -e "$RUN_DIR" ]]; then
  if [[ ! -f "$RUN_DIR/.wal-release-gate-owned" ]]; then
    fail "WAL_GATE_RUN_DIR exists without the release-gate ownership marker: $RUN_DIR"
  fi
else
  mkdir -p "$RUN_DIR"
  RUN_DIR_CREATED=1
  printf 'project=%s\n' "$PROJECT" > "$RUN_DIR/.wal-release-gate-owned"
fi
mkdir -p "$(dirname "$REPORT")"
: > "$WINDOWS"

compose() {
  docker compose --project-name "$PROJECT" -f docker-compose.rustfs.yml -f "$OVERRIDE" "$@"
}

log() {
  printf '\n== %s ==\n' "$*"
}

record_window() {
  python3 - "$WINDOWS" "$1" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
value = json.loads(sys.argv[2])
with path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
PY
}

status_code() {
  local method="$1"
  local url="$2"
  local output="$3"
  shift 3
  curl -sS --max-time 5 -o "$output" -w '%{http_code}' -X "$method" "$url" "$@" || true
}

wait_health() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 "${WAL_GATE_HEALTH_RETRIES:-120}"); do
    if curl -fsS --max-time 3 "$url/v1/health" >"$RUN_DIR/${name}-health.json" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  fail "$name health did not become available at $url"
}

wait_readiness() {
  local url="$1"
  local want="$2"
  local name="$3"
  local output="$RUN_DIR/${name}-readiness.json"
  for _ in $(seq 1 "${WAL_GATE_READINESS_RETRIES:-90}"); do
    local code
    code="$(curl -sS --max-time 3 -o "$output" -w '%{http_code}' "$url/v1/readiness" || true)"
    if [[ "$code" == "$want" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "last $name readiness response:" >&2
  cat "$output" >&2 || true
  fail "$name readiness did not become HTTP $want"
}

assert_wal_readiness() {
  local path="$1"
  local expected="$2"
  python3 - "$path" "$expected" <<'PY'
import json
import sys

body = json.load(open(sys.argv[1], encoding="utf-8"))
wal = body.get("ingest_wal")
if not isinstance(wal, dict):
    raise SystemExit(f"readiness response has no ingest_wal report: {body!r}")
if sys.argv[2] == "outage":
    if wal.get("ready") is not False or wal.get("writable") is not True:
        raise SystemExit(f"unexpected outage WAL readiness: {wal!r}")
    if int(wal.get("pending", 0)) <= 0 or not str(wal.get("last_error", "")).strip():
        raise SystemExit(f"outage WAL did not expose pending/error evidence: {wal!r}")
elif sys.argv[2] == "recovered":
    if wal.get("ready") is not True or wal.get("writable") is not True:
        raise SystemExit(f"unexpected recovered WAL readiness: {wal!r}")
    if str(wal.get("last_error", "")).strip():
        raise SystemExit(f"recovered WAL retained an error: {wal!r}")
else:
    raise SystemExit(f"unknown WAL readiness expectation {sys.argv[2]!r}")
PY
}

wait_wal_outage_readiness() {
  local url="$1"
  local name="$2"
  local output="$RUN_DIR/${name}-readiness.json"
  for _ in $(seq 1 "${WAL_GATE_READINESS_RETRIES:-90}"); do
    local code
    code="$(curl -sS --max-time 3 -o "$output" -w '%{http_code}' "$url/v1/readiness" || true)"
    if [[ "$code" == "503" ]] && assert_wal_readiness "$output" outage 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "last $name readiness response:" >&2
  cat "$output" >&2 || true
  fail "$name did not expose HTTP 503 with WAL pending/error evidence"
}

json_value() {
  local path="$1"
  local expression="$2"
  python3 - "$path" "$expression" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    if not part:
        continue
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
if value is None:
    raise SystemExit(1)
if isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

assert_json() {
  local path="$1"
  local code="$2"
  shift 2
  python3 - "$path" "$code" "$@" <<'PY'
import json
import sys

path, check = sys.argv[1], sys.argv[2]
body = json.load(open(path, encoding="utf-8"))
if check == "accepted":
    if body.get("state") != "accepted" or body.get("durability") != "durable":
        raise SystemExit(f"not durable accepted: {body!r}")
elif check == "committed":
    if body.get("state") != "committed" or not isinstance(body.get("result"), dict):
        raise SystemExit(f"not committed: {body!r}")
elif check == "result":
    if body.get("version") != int(sys.argv[3]):
        raise SystemExit(f"unexpected result version: {body!r}")
elif check == "idempotent":
    if body.get("skipped") is not True or body.get("skip_reason") != "idempotent_replay":
        raise SystemExit(f"not idempotent replay: {body!r}")
else:
    raise SystemExit(f"unknown assertion {check!r}")
PY
}

batch_path() {
  printf '/v1/ingest/batches/wal-gate/collector-main/%s' "$1"
}

post_batch() {
  local batch="$1"
  local body="$2"
  local response="$RUN_DIR/post-${batch}.json"
  local code
  code="$(status_code POST "$WRITER_URL/v1/ingest/batches" "$response" \
    -H "X-Tenant-ID: $TENANT" -H 'Content-Type: application/json' --data-binary "$body")"
  printf '%s\n' "$code" > "$RUN_DIR/post-${batch}.status"
  printf '%s\n' "$response"
}

post_batch_wait_committed() {
  local batch="$1"
  local body="$2"
  local response="$RUN_DIR/post-${batch}-committed.json"
  local code
  code="$(status_code POST "$WRITER_URL/v1/ingest/batches" "$response" \
    -H "X-Tenant-ID: $TENANT" -H 'Content-Type: application/json' \
    -H 'Prefer: wait=committed' --data-binary "$body")"
  if [[ "$code" != "200" && "$code" != "207" ]]; then
    echo "wait=committed response ($code):" >&2
    cat "$response" >&2 || true
    fail "$batch did not return a committed result"
  fi
  printf '%s\n' "$response"
}

poll_batch_state() {
  local batch="$1"
  local wanted="$2"
  local timeout_seconds="$3"
  local path
  path="$(batch_path "$batch")"
  local latest="$RUN_DIR/status-${batch}.json"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    local code
    code="$(status_code GET "$WRITER_URL$path" "$latest" -H "X-Tenant-ID: $TENANT")"
    if [[ "$code" == "200" ]]; then
      local state
      state="$(json_value "$latest" state 2>/dev/null || true)"
      if [[ "$state" == "$wanted" ]]; then
        return 0
      fi
      if [[ "$state" == "failed" ]]; then
        cat "$latest" >&2
        fail "$batch entered failed state"
      fi
    fi
    sleep "${WAL_GATE_POLL_INTERVAL:-0.2}"
  done
  echo "last status for $batch:" >&2
  cat "$latest" >&2 || true
  return 1
}

restart_writer() {
  log "SIGKILL writer and restart"
  compose kill -s SIGKILL graphdb >>"$RUN_DIR/compose.log" 2>&1
  compose up -d graphdb >>"$RUN_DIR/compose.log" 2>&1
  wait_health "$WRITER_URL" writer
  wait_readiness "$WRITER_URL" 200 writer-recovered
}

kill_and_restart_writer() {
  local batch="$1"
  local pre_kill="$RUN_DIR/pre-kill-${batch}.json"
  local code
  code="$(status_code GET "$WRITER_URL$(batch_path "$batch")" "$pre_kill" -H "X-Tenant-ID: $TENANT")"
  [[ "$code" == "200" ]] || fail "$batch pre-kill status returned HTTP $code"
  local state
  state="$(json_value "$pre_kill" state 2>/dev/null || true)"
  [[ -n "$state" ]] || fail "$batch pre-kill status did not include a state"
  if [[ "$state" != "accepted" ]]; then
    cat "$pre_kill" >&2
    fail "$batch pre-kill state was $state, want accepted"
  fi
  restart_writer
}

write_report() {
  local exit_code="$1"
  [[ "$REPORT_WRITTEN" == "1" ]] && return 0
  REPORT_WRITTEN=1
  WAL_GATE_REPORT_PATH="$REPORT" \
  WAL_GATE_WINDOWS_PATH="$WINDOWS" \
  WAL_GATE_EXIT_CODE="$exit_code" \
  WAL_GATE_FAILURE="$FAIL_REASON" \
  WAL_GATE_COMMIT="$(git rev-parse HEAD 2>/dev/null || printf unknown)" \
  WAL_GATE_PROJECT="$PROJECT" \
  WAL_GATE_TENANT="${TENANT:-}" \
  WAL_GATE_WRITER_PORT="$WRITER_PORT" \
  WAL_GATE_READER_PORT="$READER_PORT" \
  WAL_GATE_RUSTFS_PORT="$RUSTFS_PORT" \
  WAL_GATE_EXPECTED_APPLIED="${EXPECTED_APPLIED:-0}" \
  WAL_GATE_FINAL_VERSION="${FINAL_VERSION:-0}" \
  WAL_GATE_FINAL_TOTAL="${FINAL_TOTAL:-0}" \
  WAL_GATE_PUBLISHED_OBSERVED="${PUBLISHED_OBSERVED:-false}" \
  python3 - <<'PY'
import datetime
import json
import os
import pathlib

windows = []
windows_path = pathlib.Path(os.environ["WAL_GATE_WINDOWS_PATH"])
if windows_path.exists():
    for line in windows_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            windows.append(json.loads(line))

exit_code = int(os.environ["WAL_GATE_EXIT_CODE"])
published = os.environ.get("WAL_GATE_PUBLISHED_OBSERVED", "false").lower() == "true"
report = {
    "schema_version": 1,
    "kind": "wal_metadata_segment_process_recovery",
    "success": exit_code == 0,
    "commit": os.environ["WAL_GATE_COMMIT"],
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "configuration": {
        "ingest_mode": "wal",
        "metadata_mode": "segment",
        "coordination": "local",
        "object_store": "RustFS",
        "compose_project": os.environ["WAL_GATE_PROJECT"],
        "writer_port": int(os.environ["WAL_GATE_WRITER_PORT"]),
        "reader_port": int(os.environ["WAL_GATE_READER_PORT"]),
        "rustfs_port": int(os.environ["WAL_GATE_RUSTFS_PORT"]),
    },
    "tenant": os.environ.get("WAL_GATE_TENANT", ""),
    "counts": {
        "expected_applied": int(os.environ.get("WAL_GATE_EXPECTED_APPLIED", "0")),
        "final_graph_version": int(os.environ.get("WAL_GATE_FINAL_VERSION", "0")),
        "collector_applied_total": int(os.environ.get("WAL_GATE_FINAL_TOTAL", "0")),
        "published_windows_observed": 1 if published else 0,
    },
    "recovery": {
        "no_loss": exit_code == 0,
        "no_duplicate_graph_versions": exit_code == 0,
        "fifo_preserved": exit_code == 0,
        "metadata_outage_readiness_recovered": any(
            item.get("name") == "object_store_outage" and item.get("readiness_recovered") is True
            for item in windows
        ),
    },
    "windows": windows,
    "error": os.environ.get("WAL_GATE_FAILURE", "") if exit_code else "",
}
path = pathlib.Path(os.environ["WAL_GATE_REPORT_PATH"])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

cleanup() {
  local exit_code="$?"
  write_report "$exit_code" || true
  if [[ "${KEEP_WAL_GATE_STACK:-0}" != "1" ]]; then
    compose down -v >>"$RUN_DIR/compose.log" 2>&1 || true
    case "$RUN_DIR" in
      "$TMP_ROOT"/graphdb-wal-release-gate-*)
        if [[ "$RUN_DIR_CREATED" == "1" && -f "$RUN_DIR/.wal-release-gate-owned" ]]; then
          rm -rf "$RUN_DIR"
        fi
        ;;
      *)
        echo "WAL gate run directory retained (outside safe temporary prefix): $RUN_DIR" >&2
        ;;
    esac
  else
    echo "WAL gate stack kept: project=$PROJECT run_dir=$RUN_DIR" >&2
  fi
  exit "$exit_code"
}
trap cleanup EXIT

cat > "$OVERRIDE" <<YAML
services:
  graphdb:
    environment:
      GRAPHDB_DATA_DIR: /tmp/graphdb-wal
      GRAPHDB_INGEST_WAL_DIR: /tmp/graphdb-wal/wal/ingest
      GRAPHDB_INGEST_MODE: wal
      GRAPHDB_INGEST_METADATA_MODE: segment
      GRAPHDB_COORDINATION: local
      GRAPHDB_INGEST_FLUSH_INTERVAL: ${WAL_GATE_GRAPH_FLUSH_INTERVAL:-5s}
      GRAPHDB_INGEST_FLUSH_MAX_REQUESTS: ${WAL_GATE_GRAPH_FLUSH_MAX_REQUESTS:-256}
      GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL: ${WAL_GATE_METADATA_FLUSH_INTERVAL:-45s}
      GRAPHDB_INGEST_METADATA_MAX_REQUESTS: ${WAL_GATE_METADATA_MAX_REQUESTS:-256}
      GRAPHDB_INGEST_METADATA_FLUSH_WORKERS: "1"
      GRAPHDB_INGEST_FLUSH_WORKERS: "1"
      GRAPHDB_INGEST_SHUTDOWN_TIMEOUT: 10s
    ports:
      - "${WRITER_PORT}:8080"
  graphdb-reader:
    environment:
      GRAPHDB_DATA_DIR: /tmp/graphdb-reader
    ports:
      - "${READER_PORT}:8080"
  rustfs:
    ports:
      - "${RUSTFS_PORT}:9000"
YAML

TENANT="wal-release-gate-$(date +%s)-$$"
SOURCE="wal-gate"
COLLECTOR="collector-main"
EXPECTED_APPLIED=0
FINAL_VERSION=0
FINAL_TOTAL=0
PUBLISHED_OBSERVED=false

log "validate compose topology and start RustFS WAL stack"
compose config >/dev/null
compose up -d --build >"$RUN_DIR/compose.log" 2>&1
wait_health "$WRITER_URL" writer
wait_health "$READER_URL" reader
wait_readiness "$WRITER_URL" 200 writer

health_mode="$(json_value "$RUN_DIR/writer-health.json" mode)"
coordination_backend="$(json_value "$RUN_DIR/writer-health.json" coordination.backend)"
if [[ "$health_mode" != "writer" || "$coordination_backend" != "local" ]]; then
  cat "$RUN_DIR/writer-health.json" >&2
  fail "writer topology is not writer/local (mode=$health_mode backend=$coordination_backend)"
fi
ingest_mode="$(json_value "$RUN_DIR/writer-health.json" ingest_wal.ready 2>/dev/null || true)"
if [[ "$ingest_mode" != "true" ]]; then
  cat "$RUN_DIR/writer-health.json" >&2
  fail "WAL readiness was not reported by the real writer"
fi
compose exec -T graphdb sh -c 'printf "%s\n" "GRAPHDB_INGEST_MODE=$GRAPHDB_INGEST_MODE" "GRAPHDB_INGEST_METADATA_MODE=$GRAPHDB_INGEST_METADATA_MODE" "GRAPHDB_COORDINATION=$GRAPHDB_COORDINATION"' > "$RUN_DIR/runtime-config.txt"
grep -Fx 'GRAPHDB_INGEST_MODE=wal' "$RUN_DIR/runtime-config.txt" >/dev/null || fail "runtime writer did not expose GRAPHDB_INGEST_MODE=wal"
grep -Fx 'GRAPHDB_INGEST_METADATA_MODE=segment' "$RUN_DIR/runtime-config.txt" >/dev/null || fail "runtime writer did not expose GRAPHDB_INGEST_METADATA_MODE=segment"
grep -Fx 'GRAPHDB_COORDINATION=local' "$RUN_DIR/runtime-config.txt" >/dev/null || fail "runtime writer did not expose GRAPHDB_COORDINATION=local"

log "create isolated release tenant"
tenant_body="$RUN_DIR/tenant.json"
tenant_code="$(status_code POST "$WRITER_URL/v1/tenants" "$tenant_body" -H 'Content-Type: application/json' --data-binary "{\"tenant_id\":\"$TENANT\"}")"
[[ "$tenant_code" == "200" ]] || { cat "$tenant_body" >&2 || true; fail "tenant creation returned HTTP $tenant_code"; }

make_batch() {
  local batch="$1"
  local ordinal="$2"
  local full_sync="${3:-false}"
  python3 - "$TENANT" "$batch" "$ordinal" "$full_sync" <<'PY'
import json
import sys

tenant, batch, ordinal, full_sync = sys.argv[1:]
value = int(ordinal)
entity_id = f"host:{tenant}:{value:03d}"
body = {
    "source": "wal-gate",
    "collector_id": "collector-main",
    "batch_id": batch,
    "idempotency_key": batch,
    "cursor": f"cursor-{value:03d}",
    "items": [{
        "external_id": entity_id,
        "entity": {"id": entity_id, "kind": "host", "fields": {"ordinal": value, "gate": "wal-1.1.5"}},
    }],
}
if full_sync == "true":
    body["full_sync"] = True
print(json.dumps(body, separators=(",", ":")))
PY
}

crash_batch="durable-crash-1"
crash_payload="$(make_batch "$crash_batch" 1)"
crash_response="$(post_batch "$crash_batch" "$crash_payload")"
crash_code="$(cat "$RUN_DIR/post-${crash_batch}.status")"
[[ "$crash_code" == "202" ]] || { cat "$crash_response" >&2; fail "durable crash request returned HTTP $crash_code"; }
assert_json "$crash_response" accepted accepted
EXPECTED_APPLIED=$((EXPECTED_APPLIED + 1))
kill_and_restart_writer "$crash_batch"
if ! poll_batch_state "$crash_batch" committed "${WAL_GATE_RECOVERY_TIMEOUT:-90}"; then
  fail "durable 202 batch did not recover to committed"
fi
crash_status="$RUN_DIR/status-${crash_batch}.json"
crash_version="$(json_value "$crash_status" result.version)"
crash_applied="$(json_value "$crash_status" result.applied)"
[[ "$crash_applied" == "1" ]] || fail "recovered crash result applied=$crash_applied"
crash_pre_kill_state="$(json_value "$RUN_DIR/pre-kill-${crash_batch}.json" state)"
record_window "{\"name\":\"durable_202_sigkill_restart\",\"accepted\":true,\"pre_kill_state\":\"$crash_pre_kill_state\",\"recovered_state\":\"committed\",\"graph_version\":$crash_version,\"applied\":$crash_applied,\"passed\":true}"

log "idempotency replay does not advance graph version or collector totals"
duplicate_response="$(post_batch_wait_committed "$crash_batch" "$crash_payload")"
assert_json "$duplicate_response" idempotent
duplicate_version="$(json_value "$duplicate_response" version)"
[[ "$duplicate_version" == "$crash_version" ]] || fail "idempotency replay changed graph version"
record_window "{\"name\":\"idempotency_replay\",\"original_version\":$crash_version,\"replay_version\":$duplicate_version,\"passed\":true}"

log "graph publish to metadata FINALIZED crash window"
published_batch="published-crash-1"
published_payload="$(make_batch "$published_batch" 2)"
published_version=0
published_response="$(post_batch "$published_batch" "$published_payload")"
published_code="$(cat "$RUN_DIR/post-${published_batch}.status")"
[[ "$published_code" == "202" ]] || { cat "$published_response" >&2; fail "published-window request returned HTTP $published_code"; }
assert_json "$published_response" accepted accepted
EXPECTED_APPLIED=$((EXPECTED_APPLIED + 1))
if poll_batch_state "$published_batch" published "${WAL_GATE_PUBLISHED_TIMEOUT:-60}"; then
  PUBLISHED_OBSERVED=true
  published_status="$RUN_DIR/status-${published_batch}.json"
  accepted_lsn_at_published="$(json_value "$published_status" accepted_lsn)"
  compose kill -s SIGKILL graphdb >>"$RUN_DIR/compose.log" 2>&1
  compose up -d graphdb >>"$RUN_DIR/compose.log" 2>&1
  wait_health "$WRITER_URL" writer-published-recovered
  wait_readiness "$WRITER_URL" 200 writer-published-recovered
  poll_batch_state "$published_batch" committed "${WAL_GATE_RECOVERY_TIMEOUT:-90}" || fail "published batch did not finalize after restart"
  published_status="$RUN_DIR/status-${published_batch}.json"
  published_version="$(json_value "$published_status" result.version)"
  record_window "{\"name\":\"published_to_finalized_sigkill_restart\",\"published_observed\":true,\"accepted_lsn_at_published\":$accepted_lsn_at_published,\"graph_version\":$published_version,\"recovered_state\":\"committed\",\"passed\":true}"
else
  record_window "{\"name\":\"published_to_finalized_sigkill_restart\",\"published_observed\":false,\"covered\":false,\"passed\":false,\"risk\":\"published state was not externally observed before metadata finalized\"}"
  fail "published state was not externally observed before metadata finalized"
fi

log "same-tenant FIFO versions and collector totals"
fifo_versions=()
for ordinal in 3 4; do
  batch="fifo-${ordinal}"
  payload="$(make_batch "$batch" "$ordinal")"
  response="$(post_batch_wait_committed "$batch" "$payload")"
  version="$(json_value "$response" version)"
  applied="$(json_value "$response" applied)"
  [[ "$applied" == "1" ]] || fail "$batch applied=$applied"
  fifo_versions+=("$version")
  EXPECTED_APPLIED=$((EXPECTED_APPLIED + 1))
done
if [[ "${#fifo_versions[@]}" -ne 2 || "${fifo_versions[0]}" -ge "${fifo_versions[1]}" ]]; then
  fail "same-tenant FIFO versions were not strictly increasing: ${fifo_versions[*]}"
fi
record_window "{\"name\":\"same_tenant_fifo\",\"versions\":[${fifo_versions[0]},${fifo_versions[1]}],\"passed\":true}"

log "RustFS outage after a durable append, retry, and readiness recovery"
outage_batch="metadata-outage-1"
outage_payload="$(make_batch "$outage_batch" 5)"
outage_response="$(post_batch "$outage_batch" "$outage_payload")"
outage_code="$(cat "$RUN_DIR/post-${outage_batch}.status")"
[[ "$outage_code" == "202" ]] || { cat "$outage_response" >&2; fail "pre-outage durable append returned HTTP $outage_code"; }
assert_json "$outage_response" accepted accepted
EXPECTED_APPLIED=$((EXPECTED_APPLIED + 1))
compose stop rustfs >>"$RUN_DIR/compose.log" 2>&1
sleep "${WAL_GATE_OUTAGE_FLUSH_WAIT:-5}"
wait_wal_outage_readiness "$WRITER_URL" writer-outage
compose start rustfs >>"$RUN_DIR/compose.log" 2>&1
wait_health "$WRITER_URL" writer-after-outage
wait_readiness "$WRITER_URL" 200 writer-after-outage
assert_wal_readiness "$RUN_DIR/writer-after-outage-readiness.json" recovered
poll_batch_state "$outage_batch" committed "${WAL_GATE_RECOVERY_TIMEOUT:-90}" || fail "outage batch did not commit after RustFS recovery"
outage_status="$RUN_DIR/status-${outage_batch}.json"
outage_version="$(json_value "$outage_status" result.version)"
outage_pending="$(json_value "$RUN_DIR/writer-outage-readiness.json" ingest_wal.pending)"
record_window "{\"name\":\"object_store_outage\",\"durable_202_before_outage\":true,\"recovered_across_outage\":true,\"readiness_during_outage\":503,\"ingest_wal_ready_during_outage\":false,\"ingest_wal_writable_during_outage\":true,\"ingest_wal_pending_during_outage\":$outage_pending,\"readiness_recovered\":true,\"recovered_state\":\"committed\",\"passed\":true}"

log "collector totals and final graph version"
collector="$RUN_DIR/collector.json"
collector_code="$(status_code GET "$WRITER_URL/v1/ingest/collectors/$SOURCE/$COLLECTOR" "$collector" -H "X-Tenant-ID: $TENANT")"
[[ "$collector_code" == "200" ]] || { cat "$collector" >&2 || true; fail "collector status returned HTTP $collector_code"; }
FINAL_VERSION="$(json_value "$collector" last_version)"
FINAL_TOTAL="$(json_value "$collector" applied_total)"
[[ "$FINAL_TOTAL" == "$EXPECTED_APPLIED" ]] || fail "collector applied_total=$FINAL_TOTAL expected=$EXPECTED_APPLIED"
if [[ "$published_version" -le "$crash_version" || "${fifo_versions[0]}" -le "$published_version" || "${fifo_versions[1]}" -le "${fifo_versions[0]}" || "$outage_version" -le "${fifo_versions[1]}" ]]; then
  fail "graph versions are not monotonic: crash=$crash_version published=$published_version fifo=${fifo_versions[*]} outage=$outage_version"
fi
record_window "{\"name\":\"collector_totals_and_graph_version\",\"version_chain\":[$crash_version,$published_version,${fifo_versions[0]},${fifo_versions[1]},$outage_version],\"final_graph_version\":$FINAL_VERSION,\"collector_applied_total\":$FINAL_TOTAL,\"expected_applied\":$EXPECTED_APPLIED,\"passed\":true}"

log "WAL metadata segment process gate passed"
echo "WAL recovery evidence: $REPORT"
