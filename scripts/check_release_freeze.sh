#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

grep -Fx 'release: "1.1"' release/freeze-1.1.yaml >/dev/null
grep -Fx 'status: frozen' release/freeze-1.1.yaml >/dev/null
grep -Fx '  product_name: GGraphDB' release/freeze-1.1.yaml >/dev/null
grep -Fx '  query_protocol: GraphQL' release/freeze-1.1.yaml >/dev/null
grep -F '# GGraphDB' README.md >/dev/null
grep -F 'title: GGraphDB API' docs/openapi.yaml >/dev/null
grep -F 'POST /v1/query/graphql' docs/graphql.md >/dev/null
grep -F 'POST /v1/query/graphql' docs/graphql.zh-CN.md >/dev/null
grep -F 'routeSpec{pattern: "POST /v1/query/graphql"' internal/httpapi/routes.go >/dev/null
grep -F '它不是 GraphQL' docs/gql.md >/dev/null

echo "GGraphDB 1.1 freeze contract verified"
