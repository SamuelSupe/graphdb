# Tasks And Maintenance

[中文](tasks-maintenance.zh-CN.md)

Long-running operations use the unified task model.

## Task API

Start:

```sh
curl -sS -X POST "$WRITER/v1/tasks" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"type":"compact"}'
```

List:

```sh
curl -sS "$WRITER/v1/tasks?type=compact&status=running&limit=20" \
  -H 'X-Tenant-ID: demo'
```

Get:

```sh
curl -sS "$WRITER/v1/tasks/<task-id>" -H 'X-Tenant-ID: demo'
```

Cancel:

```sh
curl -sS -X POST "$WRITER/v1/tasks/<task-id>/cancel" -H 'X-Tenant-ID: demo'
```

Retry:

```sh
curl -sS -X POST "$WRITER/v1/tasks/<task-id>/retry" -H 'X-Tenant-ID: demo'
```

Task fields include `id`, `type`, `status`, `phase`, progress counters,
`params`, `checkpoint`, `result`, `result_key`, `error`, and timestamps.

Supported task types:

- `compact`
- `gc`
- `repair`
- `export_snapshot`
- `replay_deadletters`
- `index_rebuild`
- `tenant_backup`
- `tenant_restore`
- `tenant_restore_drill`

## Compact

Synchronous endpoint:

```sh
curl -sS -X POST "$WRITER/v1/compact" -H 'X-Tenant-ID: demo'
```

Task:

```sh
curl -sS -X POST "$WRITER/v1/tasks" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"type":"compact"}'
```

Compact writes a snapshot/catalog and publishes a manifest pointing at it. It
reduces reader replay cost and commit tail pressure.

## GC

Synchronous endpoint:

```sh
curl -sS -X POST "$WRITER/v1/control/gc" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{
    "keep_snapshots": 2,
    "deadletter_max_age_seconds": 604800,
    "task_max_age_seconds": 604800,
    "cleanup_index_orphans": true,
    "dry_run": true,
    "max_deletes": 1000
  }'
```

GC respects reader heartbeats and keeps objects needed by active readers. When
`max_deletes` pauses a run, pass returned `checkpoint.next_cursor` as `cursor`
to continue.

Task:

```sh
curl -sS -X POST "$WRITER/v1/tasks" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"type":"gc","params":{"keep_snapshots":2,"dry_run":false}}'
```

## Repair And Integrity Audit

Audit:

```sh
curl -sS "$WRITER/v1/control/integrity-audit?deep=true" \
  -H 'X-Tenant-ID: demo'
```

Repair dry-run:

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":false}'
```

Repair apply:

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":true}'
```

Use audit after repair to verify the remaining issue count.

## Recovery And Commit Cleanup

Recover unpublished commits:

```sh
curl -sS -X POST "$WRITER/v1/control/recover" -H 'X-Tenant-ID: demo'
```

Cleanup obsolete commit objects:

```sh
curl -sS -X POST "$WRITER/v1/control/cleanup-commits" \
  -H 'X-Tenant-ID: demo'
```

## Indexes

Create a secondary index:

```sh
curl -sS -X POST "$WRITER/v1/indexes" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"name":"host_hostname","kind":"host","field":"hostname"}'
```

List definitions:

```sh
curl -sS "$READER/v1/indexes/definitions" -H 'X-Tenant-ID: demo'
```

Catalog:

```sh
curl -sS "$READER/v1/indexes" -H 'X-Tenant-ID: demo'
```

Health:

```sh
curl -sS "$READER/v1/indexes/health" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/indexes/health?deep=true" -H 'X-Tenant-ID: demo'
```

Rebuild:

```sh
curl -sS -X POST "$WRITER/v1/indexes/rebuild?async=true" \
  -H 'X-Tenant-ID: demo'
```

Drop:

```sh
curl -sS -X DELETE "$WRITER/v1/indexes/definitions/host_hostname" \
  -H 'X-Tenant-ID: demo'
```

## Backup, Restore, Restore Drill

Start backup:

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/backup"
```

Restore:

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/restore" \
  -H 'Content-Type: application/json' \
  -d '{"backup_key":"tenants/demo/backups/...","overwrite":true,"dry_run":false}'
```

Restore drill:

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/restore-drill" \
  -H 'Content-Type: application/json' \
  -d '{
    "target_tenant_id": "demo-drill",
    "cleanup": true,
    "query_templates": ["hosts-by-region"],
    "query_timeout_ms": 3000
  }'
```

Use restore drill to prove backups are usable without overwriting the source
tenant.

## CLI

```sh
go run ./cmd/graphdb start-task demo compact
go run ./cmd/graphdb list-tasks demo
go run ./cmd/graphdb task demo <task-id>
go run ./cmd/graphdb cancel-task demo <task-id>
go run ./cmd/graphdb retry-task demo <task-id>
go run ./cmd/graphdb compact demo
go run ./cmd/graphdb gc demo 604800 604800
go run ./cmd/graphdb repair demo --apply
go run ./cmd/graphdb integrity-audit demo
go run ./cmd/graphdb create-index demo host hostname host_hostname
go run ./cmd/graphdb rebuild-indexes demo
go run ./cmd/graphdb backup-tenant demo
go run ./cmd/graphdb restore-tenant demo <backup-key> --overwrite
go run ./cmd/graphdb restore-drill-tenant demo params.json
```
