# 任务与维护

[English](tasks-maintenance.md)

长时间运行的操作统一使用 task 模型。

## Task API

启动：

```sh
curl -sS -X POST "$WRITER/v1/tasks" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"type":"compact"}'
```

列出：

```sh
curl -sS "$WRITER/v1/tasks?type=compact&status=running&limit=20" \
  -H 'X-Tenant-ID: demo'
```

查看：

```sh
curl -sS "$WRITER/v1/tasks/<task-id>" -H 'X-Tenant-ID: demo'
```

取消：

```sh
curl -sS -X POST "$WRITER/v1/tasks/<task-id>/cancel" -H 'X-Tenant-ID: demo'
```

重试：

```sh
curl -sS -X POST "$WRITER/v1/tasks/<task-id>/retry" -H 'X-Tenant-ID: demo'
```

任务字段包括 `id`、`type`、`status`、`phase`、进度计数、
`params`、`checkpoint`、`result`、`result_key`、`error` 和时间戳。

支持的 task 类型：

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

同步接口：

```sh
curl -sS -X POST "$WRITER/v1/compact" -H 'X-Tenant-ID: demo'
```

异步 task：

```sh
curl -sS -X POST "$WRITER/v1/tasks" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"type":"compact"}'
```

Compact 写入 snapshot/catalog 并发布指向它的 manifest，降低 reader 回放成本
和 commit tail 压力。

## GC

同步接口：

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

GC 遵守 reader heartbeat，保留活跃 reader 所需对象。当 `max_deletes` 暂停
运行时，将返回的 `checkpoint.next_cursor` 作为 `cursor` 继续。

task 方式：

```sh
curl -sS -X POST "$WRITER/v1/tasks" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"type":"gc","params":{"keep_snapshots":2,"dry_run":false}}'
```

## Repair 与完整性审计

审计：

```sh
curl -sS "$WRITER/v1/control/integrity-audit?deep=true" \
  -H 'X-Tenant-ID: demo'
```

repair 预演：

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":false}'
```

repair apply：

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":true}'
```

repair 后再次运行 audit，确认剩余问题数量。

## 恢复与 Commit 清理

恢复未发布 commit：

```sh
curl -sS -X POST "$WRITER/v1/control/recover" -H 'X-Tenant-ID: demo'
```

清理过期 commit 对象：

```sh
curl -sS -X POST "$WRITER/v1/control/cleanup-commits" \
  -H 'X-Tenant-ID: demo'
```

## 索引

创建二级索引：

```sh
curl -sS -X POST "$WRITER/v1/indexes" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"name":"host_hostname","kind":"host","field":"hostname"}'
```

查看定义和目录：

```sh
curl -sS "$READER/v1/indexes/definitions" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/indexes" -H 'X-Tenant-ID: demo'
```

健康检查：

```sh
curl -sS "$READER/v1/indexes/health" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/indexes/health?deep=true" -H 'X-Tenant-ID: demo'
```

重建和删除：

```sh
curl -sS -X POST "$WRITER/v1/indexes/rebuild?async=true" \
  -H 'X-Tenant-ID: demo'
curl -sS -X DELETE "$WRITER/v1/indexes/definitions/host_hostname" \
  -H 'X-Tenant-ID: demo'
```

## 备份、恢复与恢复演练

启动备份：

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/backup"
```

恢复：

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/restore" \
  -H 'Content-Type: application/json' \
  -d '{"backup_key":"tenants/demo/backups/...","overwrite":true,"dry_run":false}'
```

恢复演练：

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

恢复演练用于证明备份可用，同时不覆盖源租户。

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
