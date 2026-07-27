#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRITER_URL="${WRITER_URL:-http://127.0.0.1:${GRAPHDB_PORT:-38080}}"
READER_URL="${READER_URL:-http://127.0.0.1:${GRAPHDB_READER_PORT:-38081}}"
PROFILE="${CAPACITY_PROFILE:-smoke}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID_SLUG="$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]')"
OUTPUT_DIR="${CAPACITY_OUTPUT_DIR:-$ROOT/capacity-runs/$RUN_ID}"

run_case() {
  local name="$1"
  local writers="$2"
  local readers="$3"
  local batches="$4"
  local batch_size="$5"
  local tenant="capacity-${name}-${RUN_ID_SLUG}"

  echo "capacity case=$name writers=$writers readers=$readers batches=$batches batch_size=$batch_size"
  go run -mod=readonly ./tools/loadtest \
    -base "$WRITER_URL" \
    -reader-base "$READER_URL" \
    -tenant "$tenant" \
    -writers "$writers" \
    -readers "$readers" \
    -batches "$batches" \
    -batch-size "$batch_size" \
    -timeout "${CAPACITY_TIMEOUT:-20m}" \
    -maintenance-timeout "${CAPACITY_MAINTENANCE_TIMEOUT:-20m}" \
    -report-json "$OUTPUT_DIR/$name.json"
}

cd "$ROOT"
mkdir -p "$OUTPUT_DIR"

case "$PROFILE" in
  smoke)
    run_case mixed-smoke 2 2 5 20
    ;;
  baseline)
    run_case mixed-10k 4 8 50 200
    run_case write-25k 8 0 50 500
    ;;
  *)
    echo "CAPACITY_PROFILE must be smoke or baseline" >&2
    exit 2
    ;;
esac

python3 - "$OUTPUT_DIR" "$PROFILE" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
reports = [json.loads(path.read_text()) for path in sorted(root.glob("*.json"))]
summary = {
    "schema_version": 1,
    "profile": sys.argv[2],
    "success": bool(reports) and all(report.get("success") for report in reports),
    "reports": [
        {
            "file": path.name,
            "elapsed_ms": report.get("elapsed_ms"),
            "planned_graph": report.get("planned_graph"),
        }
        for path, report in zip(sorted(root.glob("*.json")), reports)
    ],
}
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
if not summary["success"]:
    raise SystemExit(1)
PY

echo "capacity reports: $OUTPUT_DIR"
