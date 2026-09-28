#!/usr/bin/env bash
set -euo pipefail
cat <<'HELP'
GGraphDB 2.0 releases use GitHub Actions (.github/workflows/release.yml).
1. Run scripts/release_gate.sh and review docs/release-checklist.md.
2. Merge the verified commit into main on github.com/SamuelSupe/graphdb.
3. Create an annotated v$(cat VERSION) tag at that commit and push that exact tag.
4. Wait for all gates, including the 30-minute soak, then verify Release assets.
This helper does not modify branches or push tags.
HELP
