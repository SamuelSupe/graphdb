#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
version="$(cat VERSION)"
[[ "$version" == 2.* ]]
grep -Fx "  version: $version" docs/openapi.yaml >/dev/null
grep -F 'SDKVersion       = "'"$version"'"' sdk/go/graphdb/client.go >/dev/null
grep -Fx 'version = "'"$version"'"' sdk/python/pyproject.toml >/dev/null
grep -Fx '__version__ = "'"$version"'"' sdk/python/graphdb_sdk/__init__.py >/dev/null
grep -Fx 'SDK_VERSION = "'"$version"'"' sdk/python/graphdb_sdk/client.py >/dev/null
grep -F 'module github.com/SamuelSupe/graphdb/v2' go.mod >/dev/null
grep -F 'data_hash:' docs/openapi.yaml >/dev/null
grep -F 'routeSpec{pattern: "POST /v1/query/graphql"' internal/httpapi/routes.go >/dev/null
echo "GGraphDB $version contracts verified"
