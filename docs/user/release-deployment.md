# GGraphDB Release Deployment Guide

[中文](release-deployment.zh-CN.md)

This guide is for service owners who need to download, deploy, upgrade, and
roll back GGraphDB v1.2.0. The previous supported writer line is v1.1.5;
the v1.1.5 -> v1.2.0 data upgrade is one-way. Use the
<https://github.com/SamuelSupe/graphdb/releases/tag/v1.2.0> release page and
verify the exact tag, checksum, and build commit before rollout. This guide
describes the release contract; it does not assert that the v1.2.0 gates have
already passed.

## 1. Download and verify

The v1.2.0 release page provides:

- `graphdb-v1.2.0.tar.gz`: binaries, SDKs, docs, examples, and Compose files.
- `graphdb-v1.2.0.tar.gz.sha256`: SHA-256 checksum for the archive.

Verify and unpack:

```sh
sha256sum -c graphdb-v1.2.0.tar.gz.sha256
tar -xzf graphdb-v1.2.0.tar.gz
cd v1.2.0
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
Run the binary matching the target architecture, for example
`bin/graphdb-linux-amd64 version`, and compare its tag, commit, build date, and
Go version with `VERSION` and `BUILD-METADATA.json` before rollout. The archive
checksum protects the download; the embedded build metadata protects the
binary-to-release mapping.
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
export GRAPHDB_COORDINATION=local
export GRAPHDB_INGEST_MODE=wal
export GRAPHDB_INGEST_METADATA_MODE=segment
export GRAPHDB_INGEST_WAL_DURABILITY=sync
graphdb serve
```

These are the v1.2.0 local-writer defaults: local coordination, WAL ingest,
segmented ingest metadata, and synchronous WAL durability. Set them explicitly
in service manifests so a deployment review does not depend on implicit
defaults. The WAL directory defaults to
`${GRAPHDB_DATA_DIR}/wal/ingest`; keep it on the same persistent volume as the
data directory.

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

The supplied `graphdb` service is still a local writer in v1.2.0: it inherits
WAL + segment + sync ingest defaults. Keep the `graphdb-data` named volume;
replacing the container without that volume can discard accepted-but-not-yet-
published WAL records.

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

The RustFS `graphdb` service is a local-coordination writer and also inherits
the v1.2.0 WAL + segment + sync defaults. The supplied `graphdb-writer-data`
named volume is part of the writer durability boundary; do not remove it while
accepted batches are pending.

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
- `GRAPHDB_READER_CATCHUP_TIMEOUT`: maximum wait for a reader to reach
  `min_version`.
- `GRAPHDB_WRITE_MAX_PER_TENANT`: write admission per tenant; keep the
  production default at `1`.
- `GRAPHDB_MAINTENANCE_INTERVAL`: compact, GC, and index maintenance interval.
- `GRAPHDB_OTLP_ENDPOINT`: optional OTLP/HTTP trace receiver.

### Optional PostgreSQL multi-writer coordination

`GRAPHDB_COORDINATION=local` remains the default and keeps the 1.0 writer lease
and object-manifest CAS behavior. PostgreSQL coordination is an opt-in
multi-writer path, not a fallback for local WAL. To run 2–8 optimistic writers
for the same tenant, every v1.2.0 writer and reader must use one PostgreSQL
namespace and the writer must explicitly select direct ingest:

```sh
GRAPHDB_COORDINATION=postgres
GRAPHDB_INGEST_MODE=direct
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

The v1.2.0 PostgreSQL correctness and direct-write regression path is valid only
with the explicit `GRAPHDB_INGEST_MODE=direct` setting above. Do not omit it and
do not use the local WAL defaults for a PostgreSQL writer; distributed WAL is
not implemented. The PostgreSQL path supports generic S3-compatible storage
with working `If-Match` semantics, including RustFS. Native OSS/OBS/COS
providers remain in local single-writer coordination. PostgreSQL failure never
falls back to local coordination: writes return
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

The bootstrap command copies each winning legacy `manifest.parquet` and its
write rules to immutable coordination objects, creates the PostgreSQL head, and
then writes an object-store coordination marker. That marker prevents a local
or pre-coordination writer from starting against the same prefix.

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

The v1.2.0 local-writer ingest contract is asynchronous by default. A request
to `POST /v1/ingest/batches` returns `202 Accepted` only after the batch is
fsynced to the WAL; `Location` points to its status resource and `202` does not
mean that the batch is query-visible yet:

```sh
curl -i -X POST http://127.0.0.1:38080/v1/ingest/batches \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data-binary @batch.json
```

When the caller needs the committed result in the same response, send the
explicit preference. The response is `200` or `207` after the metadata segment
is durable:

```sh
curl -i -X POST http://127.0.0.1:38080/v1/ingest/batches \
  -H 'X-Tenant-ID: demo' \
  -H 'Prefer: wait=committed' \
  -H 'Content-Type: application/json' \
  --data-binary @batch.json
```

When a query must observe a specific write, pass its response `version` as
`min_version`. Use `allow_stale=true` only for explicitly eventual-consistent
workflows. `X-Tenant-ID` is routing metadata, not authentication; production
must configure auth, authorization, TLS, and rate limiting at the gateway or
service mesh.

For a local WAL writer, a transient object-store or metadata-flush failure keeps
readiness at `503` while retry work is pending. Once the dependency is healthy
and the retry succeeds, the writer clears the stale error and readiness returns
to `200` without a process restart. A fatal local WAL append, short-write,
rotation, or fsync error fences the writer: readiness reports it as not
writable, subsequent ingest requests are rejected with the stable
`ingest_wal_unavailable` (`503`, `retryable=true`) error, and no new LSN is
assigned. Preserve the WAL directory and repair or replace the failed storage
before restarting; do not delete the tail to make the process start.

## 7. Upgrade, rollback, and data safety

### GGraphDB 1.1 data model and extension history

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

### v1.1.5 to v1.2.0 upgrade (one-way)

v1.2.0 can open existing v1.1.5 graph data because the graph model, logical
commit/version order, and object layout remain unchanged. The writer protocol
and physical ingest metadata do change: v1.2.0 defaults local writers to
`wal + segment + sync`, and a v1.2.0 segment manifest is not a reverse-compatible
writer boundary. Do not point a v1.1.5 writer at a prefix after v1.2.0 has
activated segment metadata.

Use this order for an in-place forward upgrade:

1. Back up the complete object-storage prefix and the local
   `GRAPHDB_DATA_DIR`, including `${GRAPHDB_DATA_DIR}/wal/ingest`. Record the
   v1.1.5 binary, tag, and backup timestamp.
2. Drain ingest traffic. For every accepted `202` batch, poll its `Location`
   status resource until it is `committed` or the batch is explicitly handled
   as failed; do not stop the writer with untracked accepted work.
3. Stop every v1.1.5 writer before changing the binary. Do not run old and new
   writers against the same local WAL or object prefix.
4. Install the v1.2.0 binary or image without changing `GRAPHDB_DATA_DIR`, the
   S3 prefix, or the coordination namespace. Set
   `GRAPHDB_INGEST_MODE=wal`, `GRAPHDB_INGEST_METADATA_MODE=segment`,
   `GRAPHDB_INGEST_WAL_DURABILITY=sync`, and
   `GRAPHDB_COORDINATION=local` explicitly for a local writer.
5. Start one v1.2.0 writer and verify `/v1/health`, `/v1/readiness`, WAL
   recovery/metrics, one `202` plus status poll, one
   `Prefer: wait=committed` request, and a real tenant query.
6. Only after those checks pass should readers and additional traffic be
   admitted. Keep the pre-upgrade backup until the release retention period
   ends.

This is a one-way data upgrade from v1.1.5 to v1.2.0, not a promise of reverse
writer compatibility. If the rollout must be aborted after v1.2.0 has written
segment metadata, stop and isolate the v1.2.0 deployment; do not start a
v1.1.5 writer against the modified prefix. Restore the pre-upgrade object prefix
and WAL/data directory into an isolated v1.1.5 deployment, validate it there,
and route traffic only after that restore is verified. Without a pre-upgrade
backup, do not claim that an in-place rollback is safe.

When enabling PostgreSQL coordination, use this stricter rollout:

1. Migrate the PostgreSQL schema, stop every pre-v1.2.0 writer, and validate the
   current legacy manifests.
2. Run `coordinator bootstrap --dry-run`, then `--apply`.
3. Start one v1.2.0 PostgreSQL writer with explicit
   `GRAPHDB_INGEST_MODE=direct` and verify real reads/writes, coordinator
   health, CAS metrics, and mirror status.
4. Scale to the desired writer count. Admit 1.0 readers only when
   `max_legacy_mirror_lag=0` and `outbox_backlog=0`.
5. Permanently remove pre-v1.2 writer routes and write credentials. A 1.0 reader is
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

### Release gates and the GitHub Action

The v1.2.0 release job must not publish an archive until the fixed-host
OrbStack performance job has passed. The
[release workflow](../../.github/workflows/release.yml) makes the publish job
depend on the `performance` job, which runs:

```sh
scripts/wal_performance_matrix.sh
```

That matrix runs the v1.1.5 baseline and the v1.2.0 candidate five times each
for 30 minutes, with local `wal + segment + sync` configuration. The candidate
must meet the recorded throughput, latency, RSS, CPU, direct-write, and query
regression thresholds in the [release checklist](../release-checklist.md),
including at least 10,000 mutations/s, at least 1.5x baseline median
throughput, no more than 5% run spread, and no more than 10% direct-write or
query regression. The release job also checks that the performance JSON reports
`success=true`, contains five valid baseline and candidate runs, and is bound
to the tagged commit before `gh release create` runs. A v1.2.0 release must
therefore wait for all five 30-minute runs and their gate to pass; this guide
does not claim that they have passed.

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

The v1.2.0 release also requires commit-bound, process-level WAL recovery
evidence for its local default path:

```sh
GRAPHDB_TEST_WAL_RELEASE_REPORT=/path/to/wal-recovery.json \
scripts/wal_release_gate.sh
```

The gate runs the real v1.2.0 binary with `GRAPHDB_INGEST_MODE=wal`,
`GRAPHDB_INGEST_METADATA_MODE=segment`, `GRAPHDB_INGEST_WAL_DURABILITY=sync`,
and local coordination; it exercises a durable `202 Accepted` batch across
restart and an object-store interruption, then records the tested commit in the
JSON report before promotion.

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

Rolling the writer back in place to v1.1.5 is not supported after v1.2.0 segment
metadata is active. If a rollback is required, use the pre-upgrade backup and
the isolated restore procedure above; do not delete the `extensions/v1.1`
objects or mutate the production prefix to force an older writer to start.
Drain or cancel `bulk_import` tasks before any restore, and pause
schema-governed edge writes until the restored writer is verified. The 1.1
backup/restore path remains the way to carry relation schemas into a new
tenant; the 1.0 backup format only knows the core graph.

Before upgrading, confirm that snapshots, manifests, and recent backups are
readable, and complete the pre-upgrade backup before stopping the old writer.
For the RustFS Compose deployment, pull/build the v1.2.0 image only after the
old writer is stopped and the named writer volume is preserved:

```sh
docker compose -f docker-compose.rustfs.yml pull
docker compose -f docker-compose.rustfs.yml up -d --build
```

For binary deployments, download v1.2.0, stop the old process, replace the
binary, and keep `GRAPHDB_DATA_DIR` or the S3 prefix unchanged. Follow the
one-way upgrade order above, then check health, reader readiness, metrics, and
one real tenant read/write flow.

For rollback, pin the v1.1.5 binary or image only in an isolated deployment
restored from the pre-upgrade backup. Do not delete manifests, commits,
snapshots, segment, or index objects from the production object store. Run a
restore drill against a replica bucket before switching production traffic.

Stop the RustFS stack:

```sh
docker compose -f docker-compose.rustfs.yml down
```

`down` does not delete named volumes. Do not use `down -v` unless local
RustFS data may be deleted.
