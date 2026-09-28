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
- `bulk_import`

`bulk_import` 应通过 `POST /v1/imports` 创建；上传接口会在租户范围内暂存
源文件并安全构造 task 参数。task checkpoint 记录源位置、批次号、计数和
问题样本，使 retry 可以在保持稳定 batch identity 的前提下恢复。

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

GC 按进程内活跃读视图延迟回收旧文件，期间继续允许新查询。当 `max_deletes` 暂停
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

通过索引接口创建的新重建任务也使用 `index_rebuild` 任务模型，可以用同一个 task ID
在 `/v1/tasks` 下查询、取消或重试。索引接口仍保留原有状态响应。以前单独保存的索引任务
记录继续可读，但不支持取消旧任务记录。重建后的清理问题记录为 `result.cleanup_warning`；
索引接口同时通过 `error` 展示该警告，此时重建本身仍可成功。

本地进程直接管理任务存活与取消，不需要任务心跳。重启后查询到没有执行者的活动任务时，
将其标记为失败，可显式重试并复用支持的 checkpoint；不会自动重跑所有维护任务。


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

## S3 自动备份

自动备份复用 `tenant_backup` 任务和维护循环。定时、重试、校验、恢复演练及保留清理见[对象备份指南](../object-backup.zh-CN.md#自动备份)。GC 在旧读视图阻止回收时报告 `checkpoint.deferred_files`，扫描完成不代表所有孤儿文件都已删除。
