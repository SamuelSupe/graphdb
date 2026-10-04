# Backup automation for standalone and Raft

Standalone deployments retain the [built-in tenant policy](object-backup.md). The new external worker uses the existing task API through a standalone writer/admin endpoint, a Raft entry point, or a shard router. It creates an object backup every day by default, verifies each downloaded snapshot, and runs a full isolated restore drill initially and every seven days. Tenants are explicitly selected.

This worker is included in 2.2.4. It changes no Raft protocol or mixed-version qualification. Raft still rejects `backup.enabled`; external state is stored in the worker's persistent directory, while `GET /v1/backup-automation` continues to expose the standalone built-in scheduler only. See the [detailed Chinese guide](backup-automation.zh-CN.md).

Actual standalone/three-voter S3, failover, retry, daemon restart and exclusion results are recorded in the [validation report](backup-automation-validation-2026-10-04.zh-CN.md).

The subsequent [deep fault test and fixes](backup-deep-validation-2026-10-04.zh-CN.md) cover tenant isolation, invalid/unwritable state, read-only metrics, process kills at all three admissions, S3 corruption/missing objects and quorum recovery.

## Run

Configure the server's S3 repository and create its bucket first. Python 3.10+ and the repository's standard-library SDK are sufficient. Supply HTTP credentials through `GRAPHDB_BACKUP_AUTOMATION_TOKEN` when required; never use the private Raft transport token.

```sh
python3 scripts/backup_automation.py \
  --url http://127.0.0.1:8080 --tenant production \
  --state-dir /var/lib/graphdb-backup-automation \
  --metrics-dir /var/lib/graphdb-backup-metrics \
  --interval 86400 --drill-interval 604800
```

Repeat `--tenant` for sequential processing. Add `--daemon` to keep polling, or invoke once from cron/systemd. `--drill-interval 0` disables full drills while retaining mandatory readback checks. `--retry-initial`/`--retry-max` default to 60/3600 seconds; `--http-timeout` defaults to 30 seconds and `--max-run-seconds` to 600 per tenant. Each tenant gets its own wait budget, so a queued tenant does not starve later tenants. An unfinished task survives that budget and is polled again later. Allow approximately tenant-count times the budget, plus request completion, in a multi-tenant timer/cron deadline, or use separate single-tenant workers with independent state directories. The supplied systemd example configures one tenant.

Use one scheduling owner and persistent directory per endpoint/tenant, with a stable entry URL. A process lock excludes workers sharing the same directory; independent directories on different machines do not provide distributed exclusion. Do not also enable the built-in policy for a standalone tenant managed externally. Missed intervals coalesce into one cycle, and the next interval starts after successful completion.

## Deploy

Merge the [worker environment](../deploy/backup-automation.env.example) and [server S3 environment](../deploy/object-backup.env.example) into a private configuration file.

```sh
# Standalone: worker URL is http://graphdb:8080.
docker compose --env-file /path/to/backup.env \
  -f docker-compose.yml -f docker-compose.backup.yml \
  -f docker-compose.backup-automation.yml up -d --build

# Raft: worker URL is http://gateway:8080.
docker compose --env-file /path/to/backup.env \
  -f docker-compose.raft.yml -f docker-compose.raft.backup.yml \
  -f docker-compose.backup-automation.yml up -d --build
```

The worker mounts only its own state/metrics volumes. Preserve them across restarts; do not delete unfinished state with `down -v`. Sharded installations attach it to the router network and configure repositories on all data replicas. Current runtime validation covers standalone and one three-voter group; router deployment acceptance remains pending.

Alternatively install the script and `sdk/python/graphdb_sdk` under `/opt/graphdb`, create the `graphdb-backup` system user, and install the provided [service](../deploy/systemd/graphdb-backup-automation.service), [timer](../deploy/systemd/graphdb-backup-automation.timer), and private `/etc/graphdb/backup-automation.env`. Use a host-reachable URL.

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now graphdb-backup-automation.timer
journalctl -u graphdb-backup-automation.service
```

The timer checks each minute; durable `next_run` determines when a backup is due. These examples do not install a schedule in the current production environment. Stopping the worker/timer stops future admissions; an accepted server task may still finish. Cancel it separately when necessary.

## Recovery and monitoring

The worker persists a cycle marker before admission and the returned task ID afterward. A lost response is reconciled against a bounded listing of up to 1000 same-type tasks using cycle, phase, input and retry identity. It never blindly resubmits an uncertain mutation. Definite authorization, size/semantic, capacity or pre-admission-code rejections can retry after backoff. Generic 400/404/409, 5xx and network errors retain uncertain outcome: a storage error after persistence must not be treated as unaccepted merely because it uses a 4xx status. Failed tasks use the existing retry API and preserve the original captured version.

Missing/ambiguous tasks, deleted task history, corrupt checkpoints or state stop progress. Inspect the local state and `backup_automation_cycle`/`backup_automation_phase` task parameters before changing state. A crash between persisting uncertainty and sending the request also requires operator reconciliation. Do not remove unresolved state automatically.

State loading validates phase, pending task, cycle, admission outcome/task ID and timestamp consistency. Corrupt or unwritable tenant state stops only that tenant and is not overwritten; other tenants continue. Once mode returns nonzero if any tenant fails; daemon mode remains active and reports each failure. An inaccessible shared state directory or process lock prevents all admissions.

Each backup must pass manifest, length, SHA256 and content verification. A full drill must prove recoverability and cleanup in an isolated target; dry-run success alone does not prove full recovery. Only then are `last_success`, the backup key and version updated. Failures produce nonzero exit status, durable error/backoff state and JSON logs. No credentials are persisted in state.

`--metrics-dir` emits readable `.prom` files for node_exporter's textfile collector; state JSON remains 0600. Without a separate metrics directory, the collector needs access to the private state directory. Gauges use prefix `graphdb_backup_automation_`: `last_success_timestamp_seconds`, `last_drill_timestamp_seconds`, `next_run_timestamp_seconds`, `consecutive_failures`, `pending`, `last_run_success`, with endpoint/tenant labels. Monitor completion age, repeated failures and stale drills. Data RPO additionally requires the task result's `backup_manifest.created_at` or S3 manifest's `created_at`: `last_success` measures verification completion, and a delayed retry can still contain an older captured version. Server task APIs retain detailed progress.

Metrics publication errors emit `backup_automation_metrics_error` JSON logs while durably tracked backups continue. Monitor these errors and file freshness: a missing or stale `.prom` file cannot prove current status. State JSON persistence remains mandatory before any new admission.

## Retention and scope

The standalone built-in policy retains its count/age cleanup. The external worker does not delete remote backups: its snapshots remain manual API backups, protected from built-in automatic cleanup. Use an explicitly configured, dedicated S3 lifecycle scope for external/Raft history; no rule is installed by default. Prefix expiration can affect manual, latest and in-use backups and does not guarantee keeping the newest backup. Versioned buckets need separate noncurrent-version handling; consult [S3 expiration semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html).

Snapshots include committed tenant logical data, not pending WAL, all runtime state, ingestion identity history or shard-directory topology. Follow the existing offline archive procedure for complete cluster disaster recovery. This worker does not create a globally simultaneous multi-tenant snapshot or automatically restore production.
