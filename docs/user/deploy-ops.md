# Deployment And Operations

## Runtime Modes

```sh
GRAPHDB_MODE=all|writer|reader
```

- `all`: single local process for development or small deployments.
- `writer`: production write/control process.
- `reader`: read/query process.

Production write boundary is one active writer process per tenant. Writer lease
and manifest CAS protect against accidental duplicate or stale writer processes;
they are not a multi-writer scheduler.

Readers are independent. They keep local cache but object storage remains the
source of truth.

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
- `GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT=4`
- `GRAPHDB_READER_CATCHUP_TIMEOUT=2s`

Write path:

- `GRAPHDB_WRITE_MAX_CONCURRENT=32`
- `GRAPHDB_WRITE_MAX_PER_TENANT=1`
- `GRAPHDB_WRITE_QUEUE_TIMEOUT=2s`
- `GRAPHDB_WRITE_EXECUTION_TIMEOUT=90s`
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=300`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=512MiB`
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB`
- `GRAPHDB_INDEX_ENTITY_RECORDS=false`
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true`

`GRAPHDB_WRITE_MAX_PER_TENANT` must be `0` or `1`. Keep it at `1` in normal
single-writer deployment. `0` disables that admission dimension and should only
be used for controlled testing.

Observability:

- `GRAPHDB_SLOW_QUERY_THRESHOLD=500ms`
- `GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces`
- `GRAPHDB_OTLP_INSECURE=true`
- `GRAPHDB_SERVICE_NAME=graphdb`
- `DD_PROFILING_ENABLED=false`
- `DD_SERVICE=graphdb`
- `DD_ENV=production`
- `DD_VERSION=<release-version>`

Set `DD_PROFILING_ENABLED=true` to enable only Datadog continuous profiling.
GraphDB does not initialize the Datadog tracer; the existing OTLP tracing switch
remains `GRAPHDB_OTLP_ENDPOINT`.

## MinIO Local Stack

```sh
docker compose up --build
```

This starts MinIO, creates bucket `graphdb`, and starts GraphDB in `all` mode.

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
curl -sS "$BASE/metrics"
```

Important metric families include HTTP latency, query latency, write
backpressure, object store latency, CAS conflicts, commit tail length, reader
visible version, and index health.

Logs are JSON lines on stdout for HTTP access, write/control audit, ingestion,
index rebuild, slow query, and backpressure events.

Traces are exported over OTLP/HTTP when `GRAPHDB_OTLP_ENDPOINT` is set.

## Production Operating Rules

- Keep exactly one active writer per tenant.
- Run multiple readers independently from the same object storage prefix.
- Use `min_version` for read-after-write flows that need freshness.
- Watch commit tail and keep auto compact enabled.
- Run index health checks routinely; use `?deep=true` only for explicit deep
  validation.
- Run restore drills, integrity audit, and GC on a schedule.
- Treat object store latency and 429 backpressure as collector slow-down
  signals, not data loss.
- Size collectors for 200-500 logical CMDB groups per batch before increasing
  per-tenant writer concurrency. Small batches multiply commit, manifest,
  idempotency, and collector metadata object writes.
- Retry `429` with the same `batch_id` and `idempotency_key`, plus exponential
  backoff and jitter. A retry of the same source page must not create a new
  idempotency key.
