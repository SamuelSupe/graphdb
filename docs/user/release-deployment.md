# GraphDB Release Deployment Guide

[中文](release-deployment.zh-CN.md)

This guide is for service owners who need to download, deploy, upgrade, and
roll back GraphDB. The first GitHub release tag is
`release_20260722_01`: <https://github.com/SamuelSupe/graphdb/releases>

## 1. Download and verify

The release page provides:

- `graphdb-release_20260722_01.tar.gz`: binaries, docs, examples, and Compose files.
- `graphdb-release_20260722_01.tar.gz.sha256`: SHA-256 checksum.

Verify and unpack:

```sh
sha256sum -c graphdb-release_20260722_01.tar.gz.sha256
tar -xzf graphdb-release_20260722_01.tar.gz
cd graphdb-release_20260722_01
```

The archive contains:

```text
bin/graphdb-linux-amd64
bin/graphdb-linux-arm64
bin/graphdb-darwin-arm64
Dockerfile
docker-compose.yml
docker-compose.rustfs.yml
docs/
examples/
VERSION
```

The binaries are statically built and do not require Go on the target host.
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

- GraphDB: `8080`
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

This topology uses one GraphDB binary and separates writer and reader traffic
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

Common runtime settings:

- `GRAPHDB_POLL_INTERVAL`: interval for checking new manifests.
- `GRAPHDB_READER_CATCHUP_TIMEOUT`: maximum wait for a reader to reach
  `min_version`.
- `GRAPHDB_WRITE_MAX_PER_TENANT`: write admission per tenant; keep the
  production default at `1`.
- `GRAPHDB_MAINTENANCE_INTERVAL`: compact, GC, and index maintenance interval.
- `GRAPHDB_OTLP_ENDPOINT`: optional OTLP/HTTP trace receiver.

See [Deployment And Operations](deploy-ops.md) and the root README for the full
configuration reference.

## 6. Traffic admission and health checks

Use `GET /v1/health` for process liveness. Before adding a reader to a
load balancer, use the tenant-level readiness check:

```sh
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
