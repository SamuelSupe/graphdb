# Object-storage snapshots and on-demand restore

Online graph data stays on local disk. An optional S3-compatible repository stores
self-contained, full tenant snapshots. Restore downloads a snapshot and rebuilds
local indexes. AWS S3 and MinIO use the same interface; PostgreSQL is unnecessary.
The existing backup endpoint without a request body still creates a local backup.

## Configure a repository

Create the bucket first. GraphDB does not create buckets automatically.

```sh
export GRAPHDB_DATA_DIR=/var/lib/graphdb
export GRAPHDB_BACKUP_S3_BUCKET=graphdb-backups
export GRAPHDB_BACKUP_S3_PREFIX=production
export GRAPHDB_BACKUP_S3_REGION=us-east-1
# Omit these for AWS S3. Set both for MinIO or another S3-compatible service.
export GRAPHDB_BACKUP_S3_ENDPOINT=https://minio.example.com
export GRAPHDB_BACKUP_S3_PATH_STYLE=true
# Inject static credentials from your secret manager, or use the AWS credential chain.
export GRAPHDB_BACKUP_S3_ACCESS_KEY_ID=...
export GRAPHDB_BACKUP_S3_SECRET_ACCESS_KEY=...
graphdb serve
```

`GRAPHDB_BACKUP_S3_SESSION_TOKEN` supports temporary static credentials. Without
static credentials, the AWS SDK default credential chain supports instance roles.
Region defaults to `us-east-1`, prefix to empty, and path-style to `false`.
Incomplete credentials, invalid prefixes, or backup settings without a bucket
fail startup. `GRAPHDB_STORAGE=s3` remains unsupported for online storage.

The backup identity needs prefix-scoped `s3:ListBucket`, plus `s3:GetObject`,
`s3:PutObject`, and `s3:AbortMultipartUpload`. A restore-only identity can use List/Get.
GraphDB does not delete remote backups automatically; use bucket lifecycle rules
or your retention process. Configure cleanup of incomplete multipart uploads to
reclaim parts left by a killed process.

Use the [environment example](../deploy/object-backup.env.example) with Compose:

```sh
docker compose --env-file /path/to/backup.env \
  -f docker-compose.yml -f docker-compose.backup.yml up -d
```

## Create and discover snapshots

```sh
curl -X POST http://localhost:8080/v1/tenants/source/backup \
  -H 'Content-Type: application/json' -d '{"destination":"object"}'
```

The 202 response contains a task ID. Poll `GET /v1/tasks/{id}` with
`X-Tenant-ID: source`. When `status=succeeded`, `result.backup_key` is an
`s3://.../manifest.json` restore reference. `result.backup_manifest` includes
version, capture time, size, and SHA-256. Task acceptance is not backup completion.
Omitting `destination`, or using `local`, preserves the existing local behavior.

```sh
curl 'http://localhost:8080/v1/tenants/source/backups?limit=20'
```

Listing reads published manifests from the repository, even after local tenant
data and task history are lost. Pass `next_cursor` as the next request's `cursor`;
limits are 1–100. Results use lexicographic key order, not capture-time order.
Inspect `created_at` when choosing a recovery point.

## Restore on demand

Configure the same bucket and prefix on a new installation. Restore to either the
original tenant ID or a new one:

```sh
curl -X POST http://localhost:8080/v1/tenants/target/restore \
  -H 'Content-Type: application/json' \
  -d '{"backup_key":"s3://graphdb-backups/production/source/BACKUP_ID/manifest.json","dry_run":true}'
```

Use `dry_run:true` for validation; omit it or set it to false to apply the snapshot.
An existing target requires explicit `overwrite:true`. Poll the task with the
target tenant's `X-Tenant-ID`. Manifest identity, size, SHA-256, Parquet content,
and graph constraints are checked before changing the target. Failed downloads
or validation do not purge it. The snapshot and indexes are then built and verified
in a separate local directory before switching the target. The switch journal
protects the old data across interruption and keeps task history and local backups.
The restored graph lives on local disk.
The existing `restore-drill` endpoint also accepts object backup keys.

Requests can reference only valid manifests in the configured bucket and prefix,
not arbitrary download URLs. Download staging uses the data disk and is reclaimed
on close, cancellation, or process exit. Reserve space for the downloaded snapshot
and both the old and restored data/indexes. Incomplete build/switch directories are
recovered when the data directory is reopened. Network failures do not disable normal local reads/writes.

Task stages and references persist. After failure, cancellation, or restart, use
`POST /v1/tasks/{id}/retry`; orphaned running tasks become failed after the existing
recovery grace period. Retry returns a new task ID while retaining the captured
version and original backup ID. An interrupted upload restarts its multipart transfer.
Keep the captured local backup file until upload succeeds. If it was removed,
retry fails instead of silently capturing a different version. Restore retries
also check that the remote snapshot hash has not changed.

## Snapshot scope and publication

- Each backup contains the full committed graph: entities, edges, graph model,
  tenant metadata/configuration, source policy, and relation constraints.
- It reuses the Parquet backup-record format. Indexes are rebuilt on restore;
  the original machine's indexes, manifests, and task files are unnecessary.
- Unpublished WAL requests, task/idempotency history, collector cursors, and saved
  query templates are excluded. Wait for WAL writes to reach committed state
  before starting a backup that must include them; acceptance alone is insufficient.
- The capture is first persisted locally. Its content-addressed Parquet file is
  uploaded before the manifest is conditionally published. An interrupted transfer
  has no discoverable complete backup, and an ID cannot replace different content.
- Uploads use two workers and 16 MiB parts. Ordinary commits can continue; GC and
  purge wait for the backup's read view. The two remote writes are not a multi-object
  atomic transaction; failures can leave unreferenced objects.
- This feature supplies on-demand backup, restore, and task retry. Scheduling,
  incremental backups, replication, and migration of remote online data are outside it.

## SDK and validation

Python: `backup_tenant("source", destination="object")` and
`list_object_backups("source")`; restore keeps the existing `restore_tenant` method.
Go: `BackupTenantToObjectStorage`, `ListObjectBackups`, and existing `RestoreTenant`.

On Linux/OrbStack, supply a dedicated test bucket and
`GRAPHDB_TEST_BACKUP_S3_ENDPOINT`, `_BUCKET`, `_ACCESS_KEY_ID`, and `_SECRET_ACCESS_KEY`,
then run `scripts/object_backup_gate.sh`. It checks multipart transfer, publication
failure, cancellation cleanup, retry after reopening, corrupt snapshots, and race
behavior. The Python SDK drives real WAL writes, discovery on an empty data
directory, restore, overwrite, and queries after restart. CI has a dedicated MinIO
job; `GRAPHDB_GATE_OBJECT_BACKUP=1` adds this flow to the existing release gate.
