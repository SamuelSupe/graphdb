# Deployment And Operations

[中文](deploy-ops.zh-CN.md)

## Runtime Modes

```sh
GRAPHDB_MODE=all|writer|reader
```

- `all`: single local process for development or small deployments.
- `writer`: production write/control process.
- `reader`: read/query process.

`GRAPHDB_COORDINATION=local` is the default and keeps one active writer process
per tenant. Writer lease and manifest CAS protect against accidental duplicate
or stale writer processes. `GRAPHDB_COORDINATION=postgres` replaces that
visibility boundary with PostgreSQL head CAS and supports 2–8 optimistic
writers per tenant.

Readers are independent. In local mode object storage is authoritative; in
PostgreSQL mode the PG head is authoritative and object storage holds immutable
graph objects plus the eventual 1.0 manifest mirror.

## Object Storage

Local file storage:

```sh
GRAPHDB_STORAGE=local
GRAPHDB_DATA_DIR=.graphdb
```

S3-compatible storage:

```sh
GRAPHDB_STORAGE=s3
S3_ENDPOINT=http://127.0.0.1:39000
S3_BUCKET=graphdb
S3_PATH_STYLE=true
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=graphdbadmin
S3_SECRET_ACCESS_KEY=graphdbsecret
GRAPHDB_PREFIX=graphdb
```

Each tenant is stored under:

```text
<GRAPHDB_PREFIX>/tenants/<tenant-id>/
```

## Important Environment Variables

Server:

- `GRAPHDB_ADDR=:8080`
- `GRAPHDB_ADMIN_ADDR=` (empty keeps the 1.0-compatible combined listener)
- `GRAPHDB_PPROF_ENABLED=false` (requires a separate admin listener)
- `GRAPHDB_PREFIX=graphdb`
- `GRAPHDB_POLL_INTERVAL=2s`
- `GRAPHDB_INSTANCE_ID=<stable-instance-name>`

Query admission:

- `GRAPHDB_QUERY_MAX_CONCURRENT=64`
- `GRAPHDB_QUERY_MAX_PER_TENANT=32`
- `GRAPHDB_QUERY_QUEUE_TIMEOUT=5s`

Read path:

- `GRAPHDB_READ_MAX_CONCURRENT=128`
- `GRAPHDB_READ_MAX_PER_TENANT=64`
- `GRAPHDB_READ_QUEUE_TIMEOUT=500ms`
- `GRAPHDB_READ_OBJECT_MAX_CONCURRENT=128`
- `GRAPHDB_READ_OBJECT_SINGLEFLIGHT=true`
- `GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT=2`
- `GRAPHDB_READER_INDEX_CACHE_MAX_BYTES=256MiB`
- `GRAPHDB_READER_CATCHUP_TIMEOUT=2s`

Write path:

- `GRAPHDB_WRITE_MAX_CONCURRENT=32`
- `GRAPHDB_WRITE_MAX_PER_TENANT=1`
- `GRAPHDB_WRITE_QUEUE_TIMEOUT=2s`
- `GRAPHDB_WRITE_EXECUTION_TIMEOUT=90s`
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=1500`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=512MiB`
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB`
- `GRAPHDB_INDEX_ENTITY_RECORDS=false`
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true`
- `GRAPHDB_COORDINATION=local|postgres`
- `GRAPHDB_POSTGRES_DSN=<dsn>`
- `GRAPHDB_POSTGRES_SCHEMA=graphdb_coordination`
- `GRAPHDB_COORDINATOR_NAMESPACE=<stable-cluster-id>`
- `GRAPHDB_WRITE_CAS_MAX_RETRIES=8`
- `GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION=24h`
- `GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL=3m` (must be greater than `GRAPHDB_WRITE_EXECUTION_TIMEOUT`)
- `GRAPHDB_COORDINATOR_OUTBOX_RETENTION=1h`
- `GRAPHDB_COORDINATOR_CLEANUP_INTERVAL=1m`
- `GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE=5000`
- `GRAPHDB_READINESS_TIMEOUT=2s`

Keep `GRAPHDB_WRITE_MAX_PER_TENANT=1` for strict request serialization. Values
such as `2`-`4` allow bounded pipelining of backpressure checks and post-commit
metadata finalization; manifest publication remains protected by the per-tenant
single-writer lock in local mode or PG head CAS in PostgreSQL mode. `0` disables
that admission dimension and should only be used for controlled testing.

PostgreSQL coordination additionally requires `GRAPHDB_STORAGE=s3`,
`S3_PROVIDER=generic-s3`, and `GRAPHDB_WRITER_TOPOLOGY=cas`. Run
`graphdb coordinator migrate` and `graphdb coordinator bootstrap --apply`
before starting writers. See the release deployment guide for rollout and
rollback sequencing.

Committed idempotency rows and abandoned pending reservations are retained for
the configured idempotency window; replay protection is guaranteed only inside
that window. `GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL` must be greater than
`GRAPHDB_WRITE_EXECUTION_TIMEOUT`, and pending rows are never removed before
that ownership TTL. Completed legacy manifest jobs are deleted only after their
retention window and only when the tenant mirror watermark has reached the job
revision. A retention value of `0` disables that cleanup and should only be used
with an external archival and pruning procedure.

`/v1/readiness` performs a bounded bucket list (`max-keys=1`) and therefore the
runtime object-store identity must have bucket-list permission in reader and
writer modes. `/v1/health` and `/metrics` use the background-sampled coordinator
status instead of querying PostgreSQL on the request path; readiness remains the
active dependency probe.

Observability:

- `GRAPHDB_SLOW_QUERY_THRESHOLD=500ms`
- `GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces`
- `GRAPHDB_OTLP_INSECURE=true`
- `GRAPHDB_SERVICE_NAME=graphdb`

## MinIO Local Stack

```sh
docker compose up --build
```

This starts MinIO, creates bucket `graphdb`, and starts GGraphDB in `all` mode.

## RustFS Writer And Reader Stack

```sh
docker compose -f docker-compose.rustfs.yml up --build
```

Default services:

- `graphdb`: writer on `:38080`
- `graphdb-reader`: reader on `:38081`
- `rustfs`: object storage on `:39000`

Optional reader scale profile:

```sh
docker compose -f docker-compose.rustfs.yml --profile scale-readers up --build
```

## Reader Freshness

Single reader:

```sh
curl -sS "$READER/v1/control/reader-freshness" -H 'X-Tenant-ID: demo'
```

Fleet readiness:

```sh
curl -sS "$READER/v1/control/reader-fleet-readiness?min_ready=1" \
  -H 'X-Tenant-ID: demo'
```

Traffic gate:

```sh
curl -sS "$READER/v1/control/reader-traffic-gate?min_ready=1" \
  -H 'X-Tenant-ID: demo'
```

A reader should be considered ready for traffic only when it can report the
required version or the fleet gate returns ready. Without an external router,
each reader can expose this endpoint to the deployment or load-balancer health
check.

## Metrics, Logs, Traces

Metrics:

```sh
curl -sS "$ADMIN/metrics"
```

Important metric families include HTTP latency, query latency, write
backpressure, object store latency, CAS conflicts, commit tail length, reader
visible version, coordinator availability/head revision/mirror lag, and index
health.

Logs are JSON lines on stdout for HTTP access, write/control audit, ingestion,
index rebuild, slow query, and backpressure events.

Traces are exported over OTLP/HTTP when `GRAPHDB_OTLP_ENDPOINT` is set.
See [Production Security Boundary](../security-deployment.md) for listener
separation, gateway tenant binding, RBAC, and TLS.

## Production Operating Rules

- Keep exactly one active writer per tenant in local coordination; scale to
  2–8 only after PostgreSQL bootstrap and never mix in a 1.0 writer.
- Run multiple readers independently from the same object storage prefix.
- Use `min_version` for read-after-write flows that need freshness.
- Watch commit tail and keep auto compact enabled.
- Run index health checks routinely; use `?deep=true` only for explicit deep
  validation.
- Run restore drills, integrity audit, and GC on a schedule.
- Treat object store latency and 429 backpressure as collector slow-down
  signals, not data loss.
- For CMDB collector workloads, size batches at 200-500 logical groups before
  increasing per-tenant writer concurrency. Small batches multiply commit,
  manifest, idempotency, and collector metadata object writes.
- Retry `429` with the same `batch_id` and `idempotency_key`, plus exponential
  backoff and jitter. A retry of the same source page must not create a new
  idempotency key.
