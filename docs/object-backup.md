# Object-storage snapshots and on-demand restore

This guide covers the current 2.x backup features. Version 2.1 adds optional
automation while retaining the 2.0 snapshot format. There is no 1.x backup
migration; keep older backups separate.

Internal scheduled backups, retries, retention and restore drills below apply to standalone deployments. Raft rejects enabling this internal automation; use an external scheduler with the cluster backup API, as described in [Raft operations](raft-operations.zh-CN.md).


Online graph data stays on local disk. An optional S3-compatible repository stores
full logical graph snapshots with tenant configuration, not all operational state
(see the snapshot scope below). Restore downloads a snapshot and rebuilds
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
Automatic retention additionally requires `s3:DeleteObject`; manual backups remain
untouched. Use a dedicated prefix and review bucket lifecycle rules. Configure cleanup of incomplete multipart uploads to
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

## Automatic backups

Automation is disabled by default, even when S3 is configured. Enable it per tenant
through the existing configuration API. `PUT /v1/tenant-config` replaces all overrides;
merge the `backup` section into the current config if other overrides must be kept.

```sh
# Keep GRAPHDB_MAINTENANCE_INTERVAL positive (for example 30s).
curl -X PUT http://localhost:8080/v1/tenant-config \
  -H 'X-Tenant-ID: source' -H 'Content-Type: application/json' \
  -d '{"backup":{"enabled":true,"interval_seconds":86400,"keep_count":30,"max_age_seconds":2592000,"retry_initial_seconds":60,"retry_max_seconds":3600,"restore_drill_interval_seconds":604800}}'
curl http://localhost:8080/v1/backup-automation -H 'X-Tenant-ID: source'
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Schedule backups for this tenant |
| `interval_seconds` | 86400 | Delay after a successful cycle; range 60–31536000 |
| `keep_count` | 30 | Maximum automatic snapshots; 0 disables count retention, maximum 1000 |
| `max_age_seconds` | 2592000 | Maximum capture age; 0 disables age retention |
| `retry_initial_seconds` | 60 | First retry delay, minimum 1 |
| `retry_max_seconds` | 3600 | Exponential retry ceiling; at least initial, maximum 86400 |
| `restore_drill_interval_seconds` | 0 | Optional isolated restore verification; 0 disables drills |

Age and drill intervals accept at most 315360000 seconds. The next maintenance tick
starts a newly enabled policy. Timing follows the maintenance interval and available
task capacity; this is an interval schedule, not cron. Missed cycles coalesce into
one backup. Disabling the policy stops new scheduling; existing cycles still record
their terminal state and successful captures are reclaimed locally. Failed captures
remain available for retry or an explicit reset. Cancel its task explicitly
if an in-flight upload must also stop. `GRAPHDB_MAINTENANCE_INTERVAL=0` disables scheduling.

A cycle captures a full committed snapshot, uploads it, downloads and checks its
SHA-256/length, optionally restores it into disposable local storage and audits the
restored graph/indexes, then applies retention. Success includes every requested
step. `last_success`, `last_backup_key`, `last_drill`, `next_run`, `consecutive_failures`
and `last_error` are available from `GET /v1/backup-automation`; use `task_id` with the
ordinary task API for current progress. State is observed on maintenance ticks.
Maintenance reports and existing audit logs also report scheduling failures.
Monitor the age of `last_success` and `last_error`; no email/webhook provider is added.

The schedule and planned task identity are durable before launch. Restarted or
failed cycles retry with exponential backoff and the original captured version;
retry does not silently replace it with newer data. Local task GC preserves the
current scheduled task and its retry checkpoint. After durable success is observed,
the scheduler removes its local capture; automatic task results point to S3. A failed
cycle keeps its capture until success or an explicit reset. Periodic drills share the bounded
maintenance pool. Disposable drill files are removed on completion/cancellation,
and abandoned builds are removed when the data directory is reopened after a crash.
Reserve disk space and memory for a full restored graph and its rebuilt indexes.

Retention applies **only to automatic backups**, after successful verification of
the new backup. Either the count or age limit can make an old snapshot eligible;
the newest automatic snapshot and the just-verified snapshot are always protected.
Manual snapshots and in-process restores/verifications are protected. At most 100
backups are removed per cycle. An interrupted deletion resumes from its durable
checkpoint; listing or verification failures stop cleanup. Disabled retention
(`keep_count=0`, `max_age_seconds=0`) performs no remote deletion.

Assign each installation a dedicated bucket/prefix if it runs retention. Read pins
are process-local and cannot protect restores from another installation or external
bucket lifecycle rules. Versioned buckets can retain noncurrent object versions
behind delete markers; configure their lifecycle separately. Object Lock/IAM errors
are surfaced as failed cycles. Configure incomplete-multipart cleanup separately.
Each new automatic snapshot is verified; this does not continuously rescan all older
archives. The optional periodic drill verifies a newly captured backup. Restored
tenant configuration includes its backup policy; review it on the destination.

If a captured input is permanently unavailable, fix the underlying issue and use
`POST /v1/backup-automation/reset` with the tenant header to explicitly abandon that
retry. This preserves successful history and remote snapshots; the next tick takes
a fresh capture if enabled. Reset returns 409 while a backup is active. SDKs expose
`reset_backup_automation()` / `ResetBackupAutomation`.

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
`POST /v1/tasks/{id}/retry`; orphaned running tasks become failed when inspected after restart. Retry returns a new task ID while retaining the captured
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
- At most two object-backup tasks capture/upload concurrently, using a separate
  admission limit. Each upload uses two workers and 16 MiB parts. Once the capture
  is open, uploads retain a file descriptor rather than a tenant read view or a
  compaction/GC execution slot. GC and purge can proceed during network waits.
  Purge can remove task history and retry inputs; an already open upload may still
  finish remotely, but its completion cannot recreate the purged task. The two
  remote writes are not a multi-object atomic transaction; failures can leave
  unreferenced objects.
- Backups remain full logical snapshots. Incremental backups, replication and
  migration of remote online data are outside this feature.

## SDK and validation

Python: `backup_tenant("source", destination="object")` and
`list_object_backups("source")`; tenant-scoped `get_backup_automation()` reports scheduling. Restore keeps the existing `restore_tenant` method.
Go: `BackupTenantToObjectStorage`, `ListObjectBackups`, `GetBackupAutomation`, and existing `RestoreTenant`.

On Linux/OrbStack, supply a dedicated test bucket and
`GRAPHDB_TEST_BACKUP_S3_ENDPOINT`, `_BUCKET`, `_ACCESS_KEY_ID`, and `_SECRET_ACCESS_KEY`,
then run `scripts/object_backup_gate.sh`. It checks multipart transfer, publication
failure, cancellation cleanup, retry after reopening, corrupt snapshots, and race
behavior. The Python SDK drives real WAL writes, discovery on an empty data
directory, restore, overwrite, and queries after restart. CI has a dedicated MinIO
job; `GRAPHDB_GATE_OBJECT_BACKUP=1` adds this flow to the existing release gate.
