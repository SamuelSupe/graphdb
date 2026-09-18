# 对象存储快照备份与按需恢复

在线图数据继续使用本地磁盘。可选的 S3 兼容备份库保存独立、完整的租户逻辑快照；
恢复时下载快照到本地并重建索引。AWS S3 和 MinIO 使用同一接口，不需要 PostgreSQL。
默认不连接对象存储；原有无请求体的备份接口仍创建本地备份。

## 配置备份库

先创建备份桶，给 GraphDB 配置桶和可选目录前缀。备份库不自动创建桶。

```sh
export GRAPHDB_DATA_DIR=/var/lib/graphdb
export GRAPHDB_BACKUP_S3_BUCKET=graphdb-backups
export GRAPHDB_BACKUP_S3_PREFIX=production
export GRAPHDB_BACKUP_S3_REGION=us-east-1
# AWS S3 使用默认端点；MinIO 等 S3 兼容服务配置以下两项：
export GRAPHDB_BACKUP_S3_ENDPOINT=https://minio.example.com
export GRAPHDB_BACKUP_S3_PATH_STYLE=true
# 可从秘密管理系统注入静态凭据，也可留空并使用 AWS 默认凭据链。
export GRAPHDB_BACKUP_S3_ACCESS_KEY_ID=...
export GRAPHDB_BACKUP_S3_SECRET_ACCESS_KEY=...
graphdb serve
```

`GRAPHDB_BACKUP_S3_SESSION_TOKEN` 支持临时静态凭据；未配置静态凭据时使用 AWS SDK
默认凭据链，包括实例角色。区域默认 `us-east-1`，前缀默认空，path-style 默认 `false`。
配置了其他备份参数而未提供桶、凭据不成对或前缀含越界路径时，启动明确报错。
远端在线存储配置 `GRAPHDB_STORAGE=s3` 仍不支持。

备份身份需要桶内指定前缀的 `s3:ListBucket`，对象的 `s3:GetObject`、`s3:PutObject`
及 `s3:AbortMultipartUpload` 权限。仅用于恢复的实例可使用 List/Get 权限。
GraphDB 不自动删除远端备份；保留周期由桶生命周期策略或运维流程管理。
建议为未完成的 multipart upload 配置生命周期清理，以回收进程被强制终止时留下的分片。

Compose 可使用 [配置样例](../deploy/object-backup.env.example) 和可选覆盖文件：

```sh
docker compose --env-file /path/to/backup.env \
  -f docker-compose.yml -f docker-compose.backup.yml up -d
```

## 创建和查询快照

```sh
curl -X POST http://localhost:8080/v1/tenants/source/backup \
  -H 'Content-Type: application/json' -d '{"destination":"object"}'
```

返回 202 和任务 ID。使用 `GET /v1/tasks/{id}`，携带 `X-Tenant-ID: source` 查询；
`status=succeeded` 后，`result.backup_key` 是可恢复的 `s3://.../manifest.json` 地址，
`result.backup_manifest` 含版本、快照时间、字节数和 SHA-256。
任务受理不等于备份完成。未传 `destination` 或传 `local` 时保持原本地备份行为。

```sh
curl 'http://localhost:8080/v1/tenants/source/backups?limit=20'
```

该接口从对象存储列出已发布清单，即使本地源租户和任务记录已丢失也可使用。
将返回的 `next_cursor` 作为下一页的 `cursor` 参数，`limit` 范围 1–100。
排序为备份键的字典序，需要按时间选择时查看 `created_at`，不要把最后一项当作最新备份。

## 按需恢复

在新实例上配置相同桶和前缀后，可以将快照恢复为原租户或新的租户。

```sh
curl -X POST http://localhost:8080/v1/tenants/target/restore \
  -H 'Content-Type: application/json' \
  -d '{"backup_key":"s3://graphdb-backups/production/source/BACKUP_ID/manifest.json","dry_run":true}'
```

先使用 `dry_run:true` 验证内容，正式恢复时改为 `false` 或省略。
目标租户已存在时，必须显式传 `overwrite:true`。轮询返回的任务 ID，并携带目标租户的
`X-Tenant-ID`。恢复先验证清单身份、文件长度、SHA-256、Parquet 内容校验和图约束，
再在独立本地目录构建并校验快照和索引，最后切换目标目录。失败的下载或校验不会清空目标租户。
持久化切换日志保护中断时的旧数据，并保留任务历史和本地备份。恢复完成后图数据位于本地盘。
既有 `restore-drill` 接口也接受对象备份键。

请求只能引用已配置桶和前缀下的合法备份清单，不能指定任意下载地址。
恢复暂存文件位于数据目录所在磁盘，关闭、取消或进程退出后由操作系统回收；需预留快照
下载、新旧两份数据和索引所需空间。未完成的构建和切换目录在重新打开数据目录时恢复。
网络失败不影响正常本地查询和写入。

任务阶段和引用持久化。失败、取消或服务重启后，使用
`POST /v1/tasks/{id}/retry` 继续；重启遗留的运行中任务会在原有恢复宽限期后标记失败。
重试返回新的任务 ID，并复用已捕获的版本和原备份 ID。上传未完成的分片会重新上传。
保留原本地备份文件直到上传成功；若该文件已被主动清理，重试明确失败，不另取一个版本。
重试恢复还会检查远端快照哈希未发生改变。

## 快照范围和发布语义

- 每次保存完整的已提交图版本：实体、边、图模型、租户元数据/配置、来源策略和关系约束。
- 复用现有 Parquet 备份记录格式。索引在恢复时重建，不依赖原机器的索引、manifest 或任务文件。
- 不包含尚未发布的 WAL 请求、任务历史、幂等历史、采集游标或保存的查询模板。
  WAL 的 202 只表示接管；需要纳入备份的写入应先等待 committed 终态，再创建备份。
- 先持久化本地捕获文件，随后上传内容寻址的 Parquet 文件，最后条件发布清单。
  清单发布前的中断不会形成可发现的完整备份；已发布的不同内容不能被同一备份 ID 覆盖。
- 上传使用两个工作线程、16 MiB 分片；普通提交可继续进行，GC/清空租户等待备份读视图结束。
  远端两次写入不是一个多对象原子事务，失败可能留下未引用文件。
- 本轮提供按需备份、恢复和任务重试；未增加定时调度、增量备份、跨实例复制或远端数据迁移。

## SDK 和验证

Python：`backup_tenant("source", destination="object")`、`list_object_backups("source")`；
恢复沿用 `restore_tenant("target", backup_key, overwrite=False, dry_run=False)`。
Go：`BackupTenantToObjectStorage`、`ListObjectBackups`，恢复沿用 `RestoreTenant`。

在 Linux/OrbStack 中提供独立测试桶及 `GRAPHDB_TEST_BACKUP_S3_ENDPOINT`、`_BUCKET`、
`_ACCESS_KEY_ID`、`_SECRET_ACCESS_KEY` 后运行 `scripts/object_backup_gate.sh`。
流程包含分片上传、清单发布故障、取消回收、重启后重试、损坏快照拒绝、race 检查，
以及 Python SDK 驱动的真实 WAL 写入、空目录恢复、覆盖恢复和重启查询。
CI 有独立 MinIO 验证任务；原 release gate 可用 `GRAPHDB_GATE_OBJECT_BACKUP=1` 加入该流程。
