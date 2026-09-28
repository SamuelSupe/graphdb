# GGraphDB 2.0

[简体中文](README.zh-CN.md)

GGraphDB is a multi-tenant property graph database for entities, relationships,
source-governed ingestion, graph queries, and operational workflows. GGraphDB 2.0
runs one process on local disk, without a required object-storage or PostgreSQL service.
Optional [S3-compatible snapshot backups](docs/object-backup.md) support recovery onto a new local disk.

## Current release

[2.1.0](https://github.com/SamuelSupe/graphdb/releases/tag/v2.1.0) is the main
release, developed on `main`. Local disk holds the live graph; optional
S3-compatible object storage holds snapshot backups for on-demand recovery.
See the [2.1 release notes](release/local-disk.md) and
[version boundaries](docs/naming-and-compatibility.md). There is no 1.x migration
or legacy digest compatibility layer. Upgrading from 1.x requires a new directory;
2.0 installations can reuse their directory after stopping the old process.
2.1 adds [scheduled S3 backups, retries, retention, and restore drills](docs/object-backup.md#automatic-backups), disabled by default.

## Capabilities

- JSON query DSL and GraphQL: match, pattern, neighbors, traversal, impact,
  shortest path, aggregation, pagination, and streaming.
- Entity and relationship types, field constraints, identity keys, source
  priority, idempotency, and collector state.
- Direct commits and durable WAL ingestion; Parquet commits, snapshots, and
  persistent indexes with durable local publication.
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
Commit responses include `data_hash` (`sha256-shards-v2:<hex>`).
For WAL ingestion, set `GRAPHDB_INGEST_MODE=wal`; a 202 response confirms WAL
acceptance, while `Prefer: wait=committed` waits for the terminal result.

## Architecture and performance

Data files are synced before their manifest/catalog is published. A bounded
four-worker write path coalesces directory syncs with file, byte and time budgets; random file reads let Parquet
select columns and row groups. Reads use bounded caches and local invalidation.
GC and destructive lifecycle operations wait for active read views.

The benchmark tools support the original object-store and local-file baselines.
The [2.0 validation report](docs/performance-v2.0.md) separates measured results
from capacity limits. Historical release reports describe their own builds.

## Documentation

- [Local disk operation and validation](docs/local-disk.md)
- [2.0 changes and performance validation](docs/performance-v2.0.md)
- [Architecture](docs/architecture.en.md)
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

S3 backups support opt-in tenant schedules, restart retries, full download verification,
optional restore drills and automatic retention. See [backup automation](docs/object-backup.md#automatic-backups).
