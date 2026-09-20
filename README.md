# GGraphDB — local disk edition

[简体中文](README.zh-CN.md)

GGraphDB is a multi-tenant property graph database for entities, relationships,
source-governed ingestion, graph queries, and operational workflows. This branch
runs one process on local disk, without a required object-storage or PostgreSQL service.
Optional [S3-compatible snapshot backups](docs/object-backup.md) support recovery onto a new local disk.

## Independent release

[`v1.3.4-local.9`](https://github.com/SamuelSupe/graphdb/releases/tag/v1.3.4-local.9)
is published from [`codex/local-disk-v2`](https://github.com/SamuelSupe/graphdb/tree/codex/local-disk-v2)
as a local disk prerelease. The default `main` branch and stable Latest release
remain unchanged. See the [release notes](release/local-disk.md) for binaries,
compatibility boundaries, and validation evidence.

## Capabilities

- JSON query DSL and GraphQL: match, pattern, neighbors, traversal, impact,
  shortest path, aggregation, pagination, and streaming.
- Entity and relationship types, field constraints, identity keys, source
  priority, idempotency, and collector state.
- Direct commits and durable WAL ingestion; Parquet commits, snapshots, and
  persistent indexes retain their existing formats.
- Import/export, saved queries, tenant lifecycle, backup/restore, repair,
  compaction, GC, and persistent background tasks.
- Go/Python SDKs, Prometheus metrics, JSON logs, and optional OTLP traces.

## Start

```sh
go build -o bin/graphdb ./cmd/graphdb
GRAPHDB_DATA_DIR=./.graphdb bin/graphdb serve
```

Or start the single-service container deployment:

```sh
docker compose up -d --build
```

The data directory is exclusive to one process. Stop the service before using
an offline CLI on its directory; use the HTTP APIs for live administration.
Linux and macOS local filesystems are supported. Separate readers/writers,
shared network filesystems, and multi-machine replication are outside this edition.

## Write and query

```sh
# 1. Create a tenant
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'

# 2. Write example graph data
curl -fsS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json

# 3. Query with the JSON Query DSL
curl -fsS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json

# 4. Query the generic graph with GraphQL
curl -fsS -X POST http://127.0.0.1:8080/v1/query/graphql \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"query":"query Find($request: QueryRequest!) { graph(request: $request) { version results stats } }","operationName":"Find","variables":{"request":{"op":"match","kind":"person","where":[{"field":"name","op":"eq","value":"Alice"}],"project":["id","name"],"limit":10}}}'
```


Use the write response's `version` as `min_version` when a query must observe it.
For WAL ingestion, set `GRAPHDB_INGEST_MODE=wal`; a 202 response confirms WAL
acceptance, while `Prefer: wait=committed` waits for the terminal result.

## Architecture and performance

Data files are synced before their manifest/catalog is published. A bounded
four-worker write path coalesces directory syncs; random file reads let Parquet
select columns and row groups. Reads use bounded caches and local invalidation.
GC and destructive lifecycle operations wait for active read views.

The benchmark tools support the original object-store and local-file baselines.
This worktree uses a focused local-to-local sample, described in the validation
report below. Historical release reports describe their own builds.

## Documentation

- [Local disk operation and validation](docs/local-disk.md)
- [Local disk v2 validation and focused benchmark (Chinese)](docs/performance-local-disk-v2.md)
- [Local disk performance: pagination, JSON encoding and Parquet layout (Chinese)](docs/performance-local-disk-optimization-2.md)
- [Architecture](docs/architecture.md)
- [User guide](docs/user/README.md)
- [Query capabilities](docs/query_capabilities.md)
- [GraphQL](docs/graphql.md)
- [OpenAPI](docs/openapi.yaml)
- [Go SDK](sdk/go/graphdb/README.md), [Python SDK](sdk/python/README.md)

## Development

Run Linux backend validation in OrbStack with a Linux volume for database data:

```sh
go test -mod=readonly ./...
go vet -mod=readonly ./...
RELEASE_GATE_VERIFY_ONLY=1 scripts/release_gate.sh
RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh
GRAPHDB_GATE_SOAK=1 RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh
```

The gate exercises direct/WAL ingestion, graph APIs, maintenance, and restart
consistency. The optional soak runs for 30 minutes. It manages only its own process
and retains evidence in the printed output directory.

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and [LICENSE](LICENSE).
