# GGraphDB 2.1

[简体中文](README.zh-CN.md)

GGraphDB is a multi-tenant property graph database for entities, relationships,
source-governed ingestion, graph queries, and operational workflows. GGraphDB 2.x
runs one process on local disk, without a required object-storage or PostgreSQL service.
Optional [three-replica Raft HA](docs/raft-ha.zh-CN.md) uses independent local disks with leader failover and majority durability.
Optional [tenant sharding](docs/sharding.zh-CN.md) adds independent Raft groups, persistent placement and tenant migration for horizontal expansion.
Optional [S3-compatible snapshot backups](docs/object-backup.md) support recovery onto a new local disk.

## Current release

[2.1.2](https://github.com/SamuelSupe/graphdb/releases/tag/v2.1.2) is the main
release, developed on `main`. Local disk holds the live graph; optional
S3-compatible object storage holds snapshot backups for on-demand recovery.
See the [2.1.2 release notes](release/local-disk.md) and
[version boundaries](docs/naming-and-compatibility.md). There is no 1.x migration
or legacy digest compatibility layer. Upgrading from 1.x requires a new directory;
2.0/2.1 installations can reuse their directory after stopping the old process.
2.1 adds [scheduled S3 backups, retries, retention, and restore drills](docs/object-backup.md#automatic-backups), disabled by default.

2.1.2 reduces GC work under the tenant lock by deferring validation of orphan
index files still protected by active queries. It keeps the existing data format,
API and synchronous durability defaults. Unit/race, HTTP and S3 checks passed;
the 30-minute endurance run was stopped by explicit release decision and remains
unvalidated for this release.

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

The same binary supports standalone and both Raft deployment options:

| Deployment | Selection | Durability | Operations |
| --- | --- | --- | --- |
| Standalone (default) | Leave `GRAPHDB_RAFT_*` unset; use `docker-compose.yml` | Local synchronous direct / WAL writes | Scheduled maintenance, automatic S3 backups, offline CLI after shutdown |
| Three-replica Raft | Configure `GRAPHDB_RAFT_NODE_ID` and the other required Raft settings; use `docker-compose.raft.yml` | Majority persistence, a full copy per node | Leader failover and cluster API maintenance; external backup scheduling in the first version |
| Sharded Raft | Add catalog / shard roles and `serve-router`; use `docker-compose.sharded.yml` | Independent majority persistence in each shard and the catalog | Add shards, assign new tenants and explicitly move existing tenants |

All accept direct and WAL ingestion and can run independently with separate
ports and data directories. Existing standalone directories remain compatible.
Removing Raft settings cannot turn a replica directory into a standalone database;
existing standalone data cannot bootstrap a new replica. Migrate through API
imports or backup restoration into a new directory.

The data directory is exclusive to one process. Stop the service before using
an offline CLI on its directory; use the HTTP APIs for live administration.
Linux and macOS local filesystems are supported. Separate readers/writers,
and shared network filesystems are unsupported. For multi-machine replicas, use the optional [Raft deployment](docs/raft-ha.zh-CN.md) with independent directories.

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
GC defers files protected by active read views; destructive lifecycle operations
wait for those views to finish.

In a focused OrbStack workload with four writers, sixteen readers and background
maintenance, write P95 fell from 11.77–12.34 s to 8.08 s. This is a limited
measurement, not a production latency guarantee: writes still have second-scale
tails, and compaction duration showed an unresolved regression signal. See the
[write-tail report](docs/performance-write-tail.md) for the method and limits and
[2.1.2 validation](docs/validation-v2.1.2.md) for release checks.
Historical reports, including [2.0](docs/performance-v2.0.md), describe their own builds.

## Documentation

- [Local disk operation and validation](docs/local-disk.md)
- [2.1.2 write-tail measurements and limits](docs/performance-write-tail.md)
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
