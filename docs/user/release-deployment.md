# GGraphDB Release Deployment Guide

[中文](release-deployment.zh-CN.md)

This guide is for service owners who need to download, deploy, upgrade, and
roll back GGraphDB 1.2. Examples use the released `v1.2.2` tag, available at
<https://github.com/SamuelSupe/graphdb/releases/tag/v1.2.2>.

## 1. Download and verify

The release page provides:

- `graphdb-v1.2.2.tar.gz`: binaries, SDKs, docs, examples, and Compose files.
- `graphdb-v1.2.2.tar.gz.sha256`: SHA-256 checksum.

Verify and unpack:

```sh
sha256sum -c graphdb-v1.2.2.tar.gz.sha256
tar -xzf graphdb-v1.2.2.tar.gz
cd graphdb-v1.2.2
```

The archive contains:

```text
bin/graphdb-linux-amd64
bin/graphdb-linux-arm64
bin/graphdb-darwin-arm64
Dockerfile
docker-compose.yml
docker-compose.rustfs.yml
docker-compose.postgres.yml
docs/
examples/
sdk/
CHANGELOG.md
LICENSE
SECURITY.md
BUILD-METADATA.json
VERSION
```

The binaries are statically built and do not require Go on the target host.
Run `bin/graphdb-linux-amd64 version` and compare its commit with
`BUILD-METADATA.json` before rollout.
Runtime still needs local disk or S3-compatible object storage. Production
deployments should use external object storage and must not reuse example
credentials.

## 2. Single-host file storage

This mode is suitable for development, demos, and small single-process
deployments. Linux amd64 example:

```sh
sudo install -m 0755 bin/graphdb-linux-amd64 /usr/local/bin/graphdb
sudo mkdir -p /var/lib/graphdb
sudo chown "$(id -u):$(id -g)" /var/lib/graphdb

export GRAPHDB_MODE=all
export GRAPHDB_STORAGE=local
export GRAPHDB_DATA_DIR=/var/lib/graphdb
export GRAPHDB_PREFIX=graphdb
export GRAPHDB_ADDR=:8080
graphdb serve
```

Check health:

```sh
curl -fsS http://127.0.0.1:8080/v1/health
```

Create the first tenant:

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

## 3. Docker Compose with MinIO

This is the simplest object-storage deployment for local integration:

```sh
docker compose up -d --build
curl -fsS http://127.0.0.1:8080/v1/health
```

Default ports:

- GGraphDB: `8080`
- MinIO API: `9000`
- MinIO Console: `9001`

Override host ports when necessary:

```sh
MINIO_API_PORT=29000 \
MINIO_CONSOLE_PORT=29001 \
GRAPHDB_PORT=28080 \
docker compose up -d --build
```

## 4. RustFS writer/reader deployment

This topology uses one GGraphDB binary and separates writer and reader traffic
through runtime modes and routing. Keep one active writer per tenant; readers
load from shared object storage.

```sh
docker compose -f docker-compose.rustfs.yml up -d --build
curl -fsS http://127.0.0.1:38080/v1/health
curl -fsS http://127.0.0.1:38081/v1/health
```

Default ports:

- writer: `38080`
- reader: `38081`
- RustFS S3 API: `39000`

Scale readers with the optional profile:

```sh
docker compose -f docker-compose.rustfs.yml \
  --profile scale-readers up -d --build
```

Replace `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, and
`S3_SECRET_ACCESS_KEY` with real credentials. Inject them through Secrets,
an environment manager, or a key service; never commit them.

Recommended traffic layout:

```text
write/control traffic -> GRAPHDB_MODE=writer -> one writer per tenant
query traffic         -> GRAPHDB_MODE=reader -> multiple readers
object data           -> shared S3/RustFS bucket and GRAPHDB_PREFIX
```

## 5. Key configuration

Minimal S3 configuration:

```sh
GRAPHDB_MODE=writer
GRAPHDB_STORAGE=s3
GRAPHDB_ADDR=:8080
GRAPHDB_PREFIX=graphdb
S3_ENDPOINT=https://s3.example.com
S3_BUCKET=graphdb
S3_PATH_STYLE=false
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=<access-key>
S3_SECRET_ACCESS_KEY=<secret-key>
```

Separate the production management plane:

```sh
GRAPHDB_ADDR=0.0.0.0:8080
GRAPHDB_ADMIN_ADDR=127.0.0.1:8081
GRAPHDB_PPROF_ENABLED=false
```

Common runtime settings:

- `GRAPHDB_POLL_INTERVAL`: interval for checking new manifests.
- `GRAPHDB_READER_CACHE_IDLE_TTL`: how long an inactive tenant graph remains
  resident independently of manifest polling.
- `GRAPHDB_READER_CACHE_LOAD_TIMEOUT`: internal budget for a shared cold load;
  the load may continue warming the cache after an individual request cancels.
- `GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT`: global cap for concurrent full
  graph loads across tenants (default `4`).
- `GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT`: maximum wait for a full graph-load
  slot before the request is rejected (default `2s`).
- `GRAPHDB_READER_CATCHUP_TIMEOUT`: maximum wait for a reader to reach
  `min_version`.
- `GRAPHDB_WRITE_MAX_PER_TENANT`: write admission per tenant; keep the
  production default at `1`.
- `GRAPHDB_MAINTENANCE_INTERVAL`: compact, GC, and index maintenance interval.
- `GRAPHDB_OTLP_ENDPOINT`: optional OTLP/HTTP trace receiver.

### Optional PostgreSQL multi-writer coordination

`GRAPHDB_COORDINATION=local` remains the default and keeps the 1.0 writer lease
and object-manifest CAS behavior. To run 2–8 optimistic writers for the same
tenant, every 1.1 writer and reader must use one PostgreSQL namespace:

```sh
GRAPHDB_COORDINATION=postgres
GRAPHDB_POSTGRES_DSN='postgres://graphdb:<password>@postgres:5432/graphdb'
GRAPHDB_POSTGRES_SCHEMA=graphdb_coordination
GRAPHDB_COORDINATOR_NAMESPACE=production-a
GRAPHDB_WRITE_CAS_MAX_RETRIES=8
GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION=24h
GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL=3m
GRAPHDB_COORDINATOR_OUTBOX_RETENTION=1h
GRAPHDB_COORDINATOR_CLEANUP_INTERVAL=1m
GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE=5000

GRAPHDB_STORAGE=s3
S3_PROVIDER=generic-s3
GRAPHDB_WRITER_TOPOLOGY=cas
```

The first PostgreSQL-coordinated release only supports generic S3-compatible
storage with working `If-Match` semantics, including RustFS. Native
OSS/OBS/COS providers remain in local single-writer coordination. PostgreSQL
failure never falls back to local coordination: writes return
`503 coordinator_unavailable`; readers may serve a cached version, but return
`reader_not_fresh` when it cannot satisfy `min_version`.

Initialize and inspect the coordination plane with:

```sh
graphdb coordinator migrate
graphdb coordinator bootstrap --dry-run
graphdb coordinator bootstrap --apply
graphdb coordinator status
graphdb coordinator sync-legacy-manifest
graphdb coordinator rollback --dry-run
```

The bootstrap command copies each winning 1.0 `manifest.parquet` and its write
rules to immutable coordination objects, creates the PostgreSQL head, and then
writes an object-store coordination marker. That marker prevents a local or
1.0 writer from starting against the same prefix.

See [Deployment And Operations](deploy-ops.md) and the root README for the full
configuration reference.

## 6. Traffic admission and health checks

Use `GET /v1/health` for process liveness and `GET /v1/readiness` for
process-level traffic admission. Health reports the last background-sampled
coordinator state without adding PostgreSQL to the liveness request path.
Readiness actively probes the object-store bucket and coordinator and returns
`503` when either is unavailable. Before adding a reader to a load balancer,
also use the tenant-level readiness check:

```sh
curl -fsS 'http://127.0.0.1:38081/v1/readiness'
curl -fsS \
  'http://127.0.0.1:38081/v1/control/reader-fleet-readiness?min_ready=1' \
  -H 'X-Tenant-ID: demo'
```

When a query must observe a specific write, pass its response `version` as
`min_version`. Use `allow_stale=true` only for explicitly eventual-consistent
workflows. `X-Tenant-ID` is routing metadata, not authentication; production
must configure auth, authorization, TLS, and rate limiting at the gateway or
service mesh.

## 7. Upgrade, rollback, and data safety

### GGraphDB 1.1 compatibility

GGraphDB 1.1 does not rewrite the 1.0 core graph. Manifests, snapshots, commits,
entities, edges, and Parquet objects remain at object layout version 2, so no
offline data migration is required. The new data is additive:

- entity labels are stored in the ordinary reserved field
  `fields.__graphdb_labels`;
- relation property schemas and reverse adjacency indexes live under
  `tenants/<tenant>/extensions/v1.1/`.

For an existing tenant, start the 1.1 writer and run an `index_rebuild` task to
materialize reverse adjacency shards before admitting latency-sensitive `in`
or `both` queries. Until then, correctness is preserved through graph-snapshot
fallback, but reverse reads may not have the 1.1 lazy-read performance.

During a rolling upgrade, 1.0 readers can keep serving the core graph and ignore
the reserved field and extension objects. Route `pattern` queries and reverse
index-dependent traffic only to 1.1 readers. Mixed-version clients should keep
using `upsert_ci_types`, `delete_ci_types`, and `ci_type`; the domain-neutral
aliases are decoded only by the 1.1 HTTP API.

When enabling PostgreSQL coordination, use this stricter rollout:

1. Migrate the PostgreSQL schema, stop every 1.0 writer, and validate the
   current legacy manifests.
2. Run `coordinator bootstrap --dry-run`, then `--apply`.
3. Start one 1.1 PostgreSQL writer and verify real reads/writes, coordinator
   health, CAS metrics, and mirror status.
4. Scale to the desired writer count. Admit 1.0 readers only when
   `max_legacy_mirror_lag=0` and `outbox_backlog=0`.
5. Permanently remove 1.0 writer routes and write credentials. A 1.0 reader is
   eventual-consistent; PostgreSQL remains the only authoritative head.

Use the coordinator command for rollback; do not remove the coordination marker
by hand:

```sh
# While PostgreSQL writers are still available, inspect every tenant and mirror.
graphdb coordinator rollback --dry-run

# Remove writer traffic, stop every PostgreSQL writer, then acknowledge that
# operational condition explicitly.
graphdb coordinator rollback --apply --writers-stopped
```

Apply first changes the authoritative coordination mode from `postgres` to
`draining`, which fences stale PostgreSQL writers. It then drains the mirror
outbox, verifies every legacy manifest hash/version/status against the
PostgreSQL head, changes the mode to `local`, and removes
`<GRAPHDB_PREFIX>/coordination/mode.json` with an ETag precondition. A local
writer is safe to start only after the command reports `applied=true`,
`mode_after=local`, `marker_removed=true`, and zero mirror lag/backlog. Never
run both writer modes. A complete backup of this topology contains both the
object-store prefix and the PostgreSQL coordination schema; an object-only
backup is incomplete.

Monitor `graphdb_coordinator_cas_total`,
`graphdb_coordinator_cleanup_runs_total`,
`graphdb_coordinator_cleanup_deleted_total`,
`graphdb_coordinator_head_revision`, and `graphdb_coordinator_status` alongside
`GET /v1/health` and `graphdb coordinator status`.

The PostgreSQL plus generic-S3 soak is a hard release dependency. With
OrbStack/Docker it starts isolated PostgreSQL and RustFS services and runs the
eight-writer correctness suite followed by the 30-minute, 20 commits/second
capacity gate with two active writers:

```sh
scripts/postgres_cas_gate.sh soak
```

The test uses a unique object prefix and PostgreSQL schema, retries terminal
`write_conflict` responses with the same idempotency key, runs legacy-mirror and
derived-index maintenance during the load, waits for every backlog to drain,
and reads the winning mirror with the tagged 1.0 binary. Its JSON evidence is
bound to the tested commit and is packaged by the release job. The gate fails
on lost or duplicate graph versions, stale maintenance state, 1.0 read failure,
or throughput below 90% of the target.

The release commit also has a separate formal PostgreSQL-to-local rollback
gate:

```sh
GRAPHDB_TEST_BUILD_COMMIT="$(git rev-parse HEAD)" \
GRAPHDB_TEST_ROLLBACK_REPORT=/path/to/rollback-drill.json \
scripts/postgres_cas_gate.sh rollback
```

Its commit-bound report proves that mirror lag and outbox backlog reached zero,
the marker was conditionally removed, a stale PostgreSQL writer was fenced, and
the local writer advanced the same graph without data loss.

Use the [release checklist](../release-checklist.md),
[security boundary](../security-deployment.md), and
[capacity envelope](../capacity.md) as the approval record.

Rolling the writer back to 1.0 is layout-safe, but a 1.0 writer does not enforce
relation property schemas. Drain or cancel `bulk_import` tasks first, and pause
schema-governed edge writes until the 1.1 writer is restored. Do not delete the
`extensions/v1.1` objects; they are reused after re-upgrade. Use the 1.1
backup/restore path when relation schemas must be carried into a new tenant;
the 1.0 backup format only knows the core graph.

Before upgrading, confirm that snapshots, manifests, and recent backups are
readable:

```sh
docker compose -f docker-compose.rustfs.yml pull
docker compose -f docker-compose.rustfs.yml up -d --build
```

For binary deployments, download the new release, stop the old process, replace
the binary, and keep `GRAPHDB_DATA_DIR` or the S3 prefix unchanged. After the
upgrade, check health, reader readiness, metrics, and one real tenant read/write
flow.

For rollback, pin the binary or image version. Do not delete manifests, commits,
snapshots, or index objects from object storage. For storage-format changes,
run a restore drill against a replica bucket before switching production
traffic.

Stop the RustFS stack:

```sh
docker compose -f docker-compose.rustfs.yml down
```

`down` does not delete named volumes. Do not use `down -v` unless local
RustFS data may be deleted.
