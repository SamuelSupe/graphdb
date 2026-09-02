#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

duplicates=()
while IFS= read -r -d '' file; do
  name="${file##*/}"
  if [[ "$name" == *" 2."* || "$name" == *" 2" ]]; then
    duplicates+=("$file")
  fi
done < <(git ls-files --cached --others --exclude-standard -z)

if ((${#duplicates[@]} > 0)); then
  printf 'workspace contains Finder-style duplicate paths:\n' >&2
  printf '  %s\n' "${duplicates[@]}" >&2
  exit 1
fi

if [[ -d vendor && ! -f vendor/modules.txt ]]; then
  printf 'vendor exists without vendor/modules.txt; remove the stray directory or regenerate vendor\n' >&2
  exit 1
fi
