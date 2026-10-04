# GGraphDB 2.2

[简体中文](README.zh-CN.md)

GGraphDB is a multi-tenant property graph database for entities, relationships,
source-governed ingestion, graph queries, and operational workflows. GGraphDB 2.x
runs one process on local disk, without a required object-storage or PostgreSQL service.
Optional [three-replica Raft HA](docs/raft-ha.zh-CN.md) uses independent local disks with leader failover and majority durability.
Optional [tenant sharding](docs/sharding.zh-CN.md) adds independent Raft groups, persistent placement and tenant migration for horizontal expansion.
Optional [S3-compatible snapshot backups](docs/object-backup.md) support recovery onto a new local disk.

## Current release

[2.2.4](https://github.com/SamuelSupe/graphdb/releases/tag/v2.2.4) is published as stable Latest; all exact-tag gates and downloaded asset checks passed.
Development continues on `main`. The same binary supports standalone direct/WAL,
Raft with independent local replicas, and tenant sharding across Raft groups.
It adds protected cluster administration, resumable migration and recovery,
compatible rolling upgrades, and local diagnostics. See the
[release notes](release/local-disk.md) and [version boundaries](docs/naming-and-compatibility.md).

Standalone 2.0/2.1 data directories remain compatible after stopping the old
process. Raft directories have separate ownership and protocol requirements;
use the [qualified rolling upgrade window](docs/raft-rolling-upgrade.zh-CN.md).
S3 backup automation remains available in standalone mode; Raft uses external
scheduling of cluster backup APIs. No 1.x migration is provided.

The [backup automation worker](docs/backup-automation.md) now provides
that external schedule for standalone and Raft, with persistent retries,
readback verification, isolated drills and Compose/systemd examples.

Release artifacts are published only after the exact-tag gates pass. The prior
local performance candidate failed its 30-minute maintenance soak with four
HAProxy 503 query errors; that failure remains documented. Cross-host, overall
throughput and production capacity remain unqualified. Maintenance may return
retryable 429s and cause long waits. See [2.2.4 validation](docs/validation-v2.2.4.md).

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

Disk guards, streaming snapshots, prepared maintenance, full runtime recovery and operational diagnostics are described in the [operations guide](docs/product-operations.zh-CN.md).

Raft qualification and the remaining deployment acceptance work are tracked in the
[release readiness report](docs/raft-release-readiness.zh-CN.md).

| Deployment | Selection | Durability | Operations |
| --- | --- | --- | --- |
| Standalone (default) | Leave `GRAPHDB_RAFT_*` unset; use `docker-compose.yml` | Local synchronous direct / WAL writes | Scheduled maintenance, automatic S3 backups, offline CLI after shutdown |
| Raft (three replicas by default) | Configure `GRAPHDB_RAFT_NODE_ID` and the other required Raft settings; use `docker-compose.raft.yml` | Majority persistence, a full copy per node | Leader failover and cluster API maintenance; external backup scheduling in the first version |
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

Raft maintenance preparation runs outside the application barrier where the
protocol permits; conflicting writes pause only the maintenance tenant. Control
messages use separate bounded transport queues, and migration uses disk-backed
chunks. Final publication, graph decoding and rollback still have resource costs.

The local candidate's thirty-minute workload recorded 71,140 operations without
unexpected operation errors, while ingestion included 90 expected 429s and a
40.154-second maximum wait. These are scoped correctness observations, not a
capacity or latency guarantee. See [qualification](docs/validation-v2.2.4.md).
Historical [2.1.2 write-tail measurements](docs/performance-write-tail.md) and
[2.0 results](docs/performance-v2.0.md) apply only to their recorded builds.

## Documentation

- [Local disk operation and validation](docs/local-disk.md)
- [2.2.4 validation and remaining limits](docs/validation-v2.2.4.md)
- [Raft operations and rolling upgrades](docs/raft-operations.zh-CN.md)
- [Tenant sharding and migration](docs/sharding.zh-CN.md)
- [Diagnostic metrics](docs/diagnostics-metrics.zh-CN.md)
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
