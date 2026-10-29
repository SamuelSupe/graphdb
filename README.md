# GGraphDB 2.4.3

**A multi-tenant property graph database, with a built-in management console.**

[简体中文](README.zh-CN.md) · [Download](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.3) · [Documentation](docs/README.md) · [Release notes](release/local-disk.md)

Manage entities, relationships and source-governed ingestion; query the graph with
JSON DSL or GraphQL. Start with one process and one local disk. Add independent
Raft replicas for high availability or tenant-sharded Raft groups for horizontal
expansion. Object storage and PostgreSQL are not required at runtime; S3-compatible
storage is optional for snapshot backups.

[Start](#start) · [Write and query](#write-and-query) · [Choose a deployment](#deployment-options) · [Validation](#validation-and-limits)

## Current release

**[2.4.3](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.3)** adds inode admission and cleanup, automatic standalone tenant metadata recovery, protected initialization/migration and retryable legacy WAL replay. It also includes the read-drain and forwarding fixes since 2.4.2. Release binaries use **Go 1.27.3**. The validation record separates local checks, exact-tag gates, publication and independent downloaded-asset checks.

| Area | What 2.4.3 provides |
| --- | --- |
| Management UI | Embedded English / Simplified Chinese console at `/ui/`; no separate frontend service |
| Daily workflows | Chart overview, structured forms and details, query workbench, ingestion and maintenance |
| Large tenant registries | Server-side tenant ID search and bounded pages for tenant selection and cluster placements |
| Bounded indexes | Opt-in adjacency trees, local page merging and pruned range scans; legacy layouts and global edge cursors retained |
| Reliability | Deterministic replication failures, cancelable reads, durable replay and protected GC continuation |
| Deployment | Standalone direct/WAL, independent Raft replicas and tenant-sharded Raft groups |

See [release validation](docs/validation-v2.4.3.md) for evidence and [version boundaries](docs/naming-and-compatibility.md) for compatibility.

## Start

### 1. Run the service

From the repository root, with the Go version in [go.mod](go.mod):

```sh
go build -o bin/graphdb ./cmd/graphdb
GRAPHDB_DATA_DIR=./.graphdb bin/graphdb serve
```

Prefer a packaged binary? Follow [release deployment](docs/user/release-deployment.md) to download and verify it.

<details>
<summary>Container alternative</summary>

Run from the repository root. The default Compose deployment has one service and one persistent volume.

```sh
docker compose up -d --build
```

</details>

### 2. Open the console

Open **[http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/)**. Choose a tenant in
the top bar and a language at the bottom of the sidebar. With split listeners,
open the management port. See the [console guide](docs/management-ui.md).

### 3. Check the service

In another terminal:

```sh
curl -fsS http://127.0.0.1:8080/v1/health
```

Health is a liveness check. Use the [readiness checks](docs/local-disk.md#readiness-and-memory-budgets) before sending production traffic.

## Write and query

Keep the service running. Execute these commands from the repository root so the
[example files](examples/) resolve correctly. They use the default combined listener.

### 1. Create a tenant

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

### 2. Write example graph data

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json
```

### 3. Query with JSON DSL

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json
```

For GraphQL, follow the [GraphQL examples](docs/graphql.md). The console also
provides a query workbench with table, JSON and graph result views.

| Contract | Meaning |
| --- | --- |
| `X-Tenant-ID` | Selects the tenant for data APIs |
| `version` | Pass the write response's version as `min_version` when the read must observe that write |
| `data_hash` | Commit digest, identified as `sha256-shards-v2:<hex>` |
| WAL acceptance | `GRAPHDB_INGEST_MODE=wal` enables WAL; HTTP 202 confirms acceptance, and `Prefer: wait=committed` waits for the terminal result |

## Capabilities

| Workflow | Supported operations |
| --- | --- |
| Model and govern | Entity/relationship types, field constraints, identity keys, source priority, idempotency and collector state |
| Query | Match, pattern, neighbors, traversal, impact, shortest path, aggregation, pagination and streaming |
| Store and ingest | Direct commits, durable WAL ingestion, Parquet commits, snapshots and persistent indexes |
| Operate | Import/export, saved queries, tenant lifecycle, backup/restore, repair, compaction, GC and persistent tasks |
| Integrate and observe | Go/Python SDKs, Prometheus metrics, JSON logs and optional OTLP traces |

## Deployment options

| Deployment | Use when | Start here |
| --- | --- | --- |
| Standalone | One process with synchronous local direct/WAL writes | [Local disk guide](docs/local-disk.md) · [Compose](docker-compose.yml) |
| Raft | Majority persistence and leader failover; each replica holds a full copy on its own disk | [Three-replica guide](docs/raft-ha.zh-CN.md) · [Compose](docker-compose.raft.yml) |
| Sharded Raft | Independent catalog/shard groups, tenant placement and explicit migration | [Sharding guide](docs/sharding.zh-CN.md) · [Compose](docker-compose.sharded.yml) |

All three support direct and WAL ingestion. Standalone leaves `GRAPHDB_RAFT_*`
unset; Raft requires the complete node configuration. Sharding adds catalog/shard
roles and `serve-router`. Separate deployments need separate ports and directories.

**Directory ownership.** One process exclusively owns each data directory. Stop
it before running offline CLI tools on that directory; use HTTP for live
administration. Linux and macOS local filesystems are supported. Separate
reader/writer processes and shared network filesystems are unsupported.

**Upgrade and recovery.** Stop the old standalone 2.0/2.1/2.2 process before reusing
its directory. Raft directories cannot become standalone by removing settings,
and standalone data cannot bootstrap a replica. Use API imports or backup restore
into a new directory when moving between deployments. Raft upgrades follow the
[qualified rolling window](docs/raft-rolling-upgrade.zh-CN.md). No 1.x migration is provided.

**Backups and access.** Standalone supports built-in S3 scheduling; Raft uses
external scheduling of cluster backup APIs. The [backup worker](docs/backup-automation.md)
provides retries, verification, isolated drills and Compose/systemd examples for
both. Configure authentication, tenant authorization and TLS at your
[gateway](docs/security-deployment.md) before exposing the service.

## Validation and limits

Version 2.3.6 removes foreground WAL waits on local maintenance index work,
orders concurrent rebuild publication, reduces repeated Raft filesystem work,
and prepares compact representations within the existing shared encoding limit.

In the matched candidate replay, standalone write P99 fell **9.470 → 3.948 seconds**;
Raft query maximum fell **13.132 → 5.584 seconds** during the original thirty-minute
growth plan. These compare C3 and C7 workspace candidates, not published 2.3.4 and
2.3.6 binaries. Standalone stale frequency did not improve (**131/327 → 134/327**),
all stale samples were updating, and quiet-state indexes caught up. Standalone
container memory peak rose **6.4%**; Raft baseline resource counters are unavailable.
Seconds-long query and ingestion tails remain. Each deployment uses its own matched
inputs; the two deployments cannot be compared directly.

The separate exact-tag standalone thirty-minute qualification recorded write P99 **5.627 seconds** (maximum **7.652**) and **53/59 stale index samples**, with the last sample **stale**. A passing gate does not mean staleness is resolved. Its growing workload differs from the fixed-data candidate replay.

The separate exact-tag Raft thirty-minute qualification recorded a **16.042-second** query maximum and **10.968-second** ingestion maximum, with **0/59** stale index samples. Seconds-long tails remain; this qualification workload is separate from the matched candidate replay.

See [exact release evidence](docs/validation-v2.3.6.md) and the
[matched performance report](docs/performance-tail-validation-2026-10-20.zh-CN.md).
All exact-tag gates and downloaded Linux/macOS runtime checks passed. Cross-host reliability, day-scale stability,
production capacity and a general latency SLO remain unqualified.

<details>
<summary>Historical performance evidence</summary>

An earlier local candidate recorded 71,140 operations, 90 expected ingestion 429s
and a 40.154-second maximum wait. Its maintenance soak also had four HAProxy 503
query errors and remains a historical FAIL. It is separate from the exact-tag
results above. [2.1.2 write-tail measurements](docs/performance-write-tail.md)
and [2.0 results](docs/performance-v2.0.md) apply only to their recorded builds.

</details>

<details>
<summary>Storage and publication design</summary>

Data files are synced before manifest/catalog publication. A bounded four-worker
write path coalesces directory syncs with file, byte and time budgets; Parquet
random reads select columns and row groups. Bounded caches invalidate on local
publication. GC protects active read views; destructive lifecycle operations wait
for them to finish.

Raft prepares maintenance outside the application barrier where the protocol
permits, pauses conflicting writes for the maintenance tenant, uses separate
bounded control queues and migrates with disk-backed chunks. Publication, graph
decoding and rollback still consume resources. See [architecture](docs/architecture.en.md).

</details>

## Documentation

| I want to… | Read |
| --- | --- |
| Browse all guides | [Documentation index](docs/README.md) · [User guide](docs/user/README.md) |
| Use the console | [Management UI](docs/management-ui.md) |
| Build a query or client | [Query capabilities](docs/query_capabilities.md) · [GraphQL](docs/graphql.md) · [OpenAPI](docs/openapi.yaml) · [Go SDK](sdk/go/graphdb/README.md) · [Python SDK](sdk/python/README.md) |
| Maintain the service | [Product operations](docs/product-operations.zh-CN.md) · [Raft operations](docs/raft-operations.zh-CN.md) · [Diagnostic metrics](docs/diagnostics-metrics.zh-CN.md) |
| Check release evidence | [2.4.3 validation](docs/validation-v2.4.3.md) · [Compatibility](docs/naming-and-compatibility.md) |

Some cluster operations guides are currently Chinese-only; the user guide and console documentation are bilingual.

## Development

Use OrbStack Linux with a Linux volume for backend runtime validation.

| Check | Command |
| --- | --- |
| Unit tests | `go test -mod=readonly ./...` |
| Static analysis | `go vet -mod=readonly ./...` |
| Full static release gate | `RELEASE_GATE_VERIFY_ONLY=1 scripts/release_gate.sh` |
| Runtime integration | `RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh` |
| 30-minute maintenance soak | `GRAPHDB_GATE_SOAK=1 RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh` |

The runtime gate exercises direct/WAL ingestion, graph APIs, maintenance and
restart consistency. Scripts manage their own processes and retain evidence in
the printed output directory.

[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [License — rights reserved](LICENSE)
