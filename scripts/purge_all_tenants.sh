#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/purge_all_tenants.sh [--dry-run] [--yes] [--managed-only]

Permanently purge every GraphDB tenant visible to the configured object store.

Options:
  --dry-run       List tenants and commands without deleting data.
  --yes           Skip the interactive "PURGE ALL" confirmation.
  --managed-only  Exclude legacy tenants discovered only from object prefixes.
  -h, --help      Show this help.

Environment:
  GRAPHDB_BIN     Path to the graphdb executable (default: graphdb in PATH).

All normal GraphDB configuration variables, including GRAPHDB_STORAGE,
GRAPHDB_DATA_DIR, GRAPHDB_PREFIX, and S3_*, are passed through unchanged.
EOF
}

dry_run=false
assume_yes=false
include_legacy=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      dry_run=true
      ;;
    --yes)
      assume_yes=true
      ;;
    --managed-only)
      include_legacy=false
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

graphdb_bin="${GRAPHDB_BIN:-graphdb}"
if ! command -v "$graphdb_bin" >/dev/null 2>&1; then
  echo "graphdb executable not found: $graphdb_bin" >&2
  echo "set GRAPHDB_BIN to the graphdb executable path" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to parse graphdb list-tenants output" >&2
  exit 1
fi

list_args=(list-tenants)
if [[ "$include_legacy" == true ]]; then
  list_args+=(--include-legacy)
fi

echo "Reading tenants from the configured GraphDB storage..."
tenant_json="$("$graphdb_bin" "${list_args[@]}")"
if ! jq -e '
  .tenants | type == "array" and
  all(.[]; (.tenant_id | type == "string") and (.tenant_id | length > 0))
' >/dev/null <<<"$tenant_json"; then
  echo "unexpected graphdb list-tenants response" >&2
  exit 1
fi

tenants=()
while IFS= read -r tenant_id; do
  tenants+=("$tenant_id")
done < <(jq -r '.tenants[].tenant_id' <<<"$tenant_json")

if [[ ${#tenants[@]} -eq 0 ]]; then
  echo "No tenants found. Nothing to purge."
  exit 0
fi

echo
printf 'Found %d tenant(s):\n' "${#tenants[@]}"
printf '  %s\n' "${tenants[@]}"
echo
printf 'Storage: %s\n' "${GRAPHDB_STORAGE:-auto}"
printf 'Prefix:  %s\n' "${GRAPHDB_PREFIX:-graphdb}"
if [[ -n "${S3_BUCKET:-}" ]]; then
  printf 'Bucket:  %s\n' "$S3_BUCKET"
else
  printf 'Data dir: %s\n' "${GRAPHDB_DATA_DIR:-.graphdb}"
fi

if [[ "$dry_run" == true ]]; then
  echo
  echo "Dry run; no data will be deleted."
  for tenant_id in "${tenants[@]}"; do
    printf '%q purge-tenant %q --force\n' "$graphdb_bin" "$tenant_id"
  done
  exit 0
fi

if [[ "$assume_yes" != true ]]; then
  if [[ ! -t 0 ]]; then
    echo "refusing to purge without an interactive terminal; pass --yes to confirm" >&2
    exit 1
  fi
  echo
  echo "WARNING: this permanently deletes all data for every tenant listed above."
  read -r -p 'Type PURGE ALL to continue: ' confirmation
  if [[ "$confirmation" != "PURGE ALL" ]]; then
    echo "Confirmation did not match; nothing was deleted."
    exit 1
  fi
fi

succeeded=0
failed=()
for tenant_id in "${tenants[@]}"; do
  printf '\nPurging tenant %s...\n' "$tenant_id"
  if "$graphdb_bin" purge-tenant "$tenant_id" --force; then
    ((succeeded += 1))
  else
    failed+=("$tenant_id")
    echo "Failed to purge tenant: $tenant_id" >&2
  fi
done

echo
printf 'Purged %d of %d tenant(s).\n' "$succeeded" "${#tenants[@]}"
if [[ ${#failed[@]} -gt 0 ]]; then
  echo "Failed tenants:" >&2
  printf '  %s\n' "${failed[@]}" >&2
  exit 1
fi

echo "All tenant data was purged successfully."
