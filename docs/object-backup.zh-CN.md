# 对象存储快照备份与按需恢复

本指南覆盖当前 2.x 备份能力。2.1 新增可选自动化并沿用 2.0 快照格式；
不提供 1.x 备份迁移，旧版备份独立保留。

以下内置定时备份、重试、保留清理和恢复演练适用于单机。Raft 拒绝开启该内置自动化，应由外部调度调用集群备份 API，见 [Raft 运维手册](raft-operations.zh-CN.md)。

本轮尚未发布的修复将 Raft 恢复传输的校验报告固定为任务受理时间，切主后重新准备同一份 S3 快照可保持相同摘要；单机仍记录实际检查时间。2.2.2 及此前程序的部分 S3 恢复可能报 `restore input changed during transfer`。切换到包含修复的程序后，通过 `POST /v1/tasks/{id}/retry` 重试已失败任务；旧程序留下的 queued 传输先取消再重试。混部升级前先完成或取消这些传输，并暂停发起新的 S3 恢复，直到所有投票节点使用包含修复的程序；窗口内旧 Leader 接管仍可能复现缺陷。本轮修复与回归证据见 [产品审核](product-p0-p1-review-2026-10-03.zh-CN.md)。


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
自动保留清理还需要 `s3:DeleteObject` 权限；手动备份不会自动删除。每个实例使用独立前缀。
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

## 自动备份

默认关闭自动化，配置 S3 后也不会自动上传。按租户通过现有配置接口开启。
`PUT /v1/tenant-config` 会替换全部覆盖配置；已有其他配置时，应先读取并合并 `backup` 部分。

```sh
# GRAPHDB_MAINTENANCE_INTERVAL 必须大于 0，例如 30s。
curl -X PUT http://localhost:8080/v1/tenant-config \
  -H 'X-Tenant-ID: source' -H 'Content-Type: application/json' \
  -d '{"backup":{"enabled":true,"interval_seconds":86400,"keep_count":30,"max_age_seconds":2592000,"retry_initial_seconds":60,"retry_max_seconds":3600,"restore_drill_interval_seconds":604800}}'
curl http://localhost:8080/v1/backup-automation -H 'X-Tenant-ID: source'
```

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `enabled` | `false` | 是否调度该租户备份 |
| `interval_seconds` | 86400 | 成功完成后到下次备份的间隔，范围 60–31536000 |
| `keep_count` | 30 | 自动快照保留数量，0 关闭数量限制，最大 1000 |
| `max_age_seconds` | 2592000 | 按捕获时间计算保留期限，0 关闭期限限制 |
| `retry_initial_seconds` | 60 | 首次失败重试等待，至少 1 秒 |
| `retry_max_seconds` | 3600 | 指数退避上限，不小于初始值，最大 86400 秒 |
| `restore_drill_interval_seconds` | 0 | 可选恢复演练间隔，0 关闭演练 |

保留期限和演练间隔最大 315360000 秒。新启用的策略在下一个维护周期调度，具体时间还受任务
容量影响；这是间隔调度，不是 cron。停机期间错过的多个周期合并为一次备份。关闭策略只停止
后续调度；已运行轮次仍会记录终态，成功后回收本地捕获文件，失败输入保留供重试或显式重置。
如需停止当前上传，还应取消相应任务。`GRAPHDB_MAINTENANCE_INTERVAL=0` 会停用调度。

每轮执行完整快照捕获、上传、下载并验证 SHA-256/长度、可选隔离恢复及图/索引审计，最后执行
保留清理。全部成功后才记录本轮成功。`GET /v1/backup-automation` 返回 `last_success`、
`last_backup_key`、`last_drill`、`next_run`、`consecutive_failures` 和 `last_error`；
用 `task_id` 查询普通任务接口可以查看实时进度。调度状态在维护周期中更新；维护报告和现有
审计日志也记录调度失败。可针对最后成功时间和错误设置监控，本功能不引入邮件或 webhook 服务。

调度状态与计划任务 ID 在启动任务前持久化。重启或失败后，按指数退避复用原捕获版本和检查点，
不会悄悄换成较新的图版本。本地任务 GC 保留当前调度任务及重试所需检查点。调度器确认成功终态已落盘后回收本地捕获文件，
自动任务结果指向 S3；失败轮次保留捕获文件，直到重试成功或显式重置。恢复演练共用有界
维护执行池；临时数据在完成或取消后清理，强制退出遗留的构建目录在下次打开数据目录时清理。
需为完整恢复图及重建索引预留磁盘和内存。

保留策略**只清理自动备份**，且必须先完成新备份校验。数量或期限任一超限即可清理；最新自动
快照和本轮刚验证的快照始终保留。手动备份及同进程正在恢复/校验的备份受到保护。每轮最多删除
100 份，多余部分留到下轮；删除中断通过持久化检查点续做，列举或校验失败则停止清理。
`keep_count=0` 且 `max_age_seconds=0` 表示完全关闭远端清理。

运行自动清理的实例必须独占一个桶/前缀。进程内引用不能保护另一实例或外部生命周期规则发起的
删除。启用版本控制的桶可能通过删除标记保留旧对象版本，应另设旧版本生命周期；Object Lock
或 IAM 拒绝会使本轮明确失败。未完成分片的清理仍由桶生命周期负责。
每份新自动快照都会完整校验，不持续扫描全部历史归档；周期演练验证当轮新快照。恢复会带回租户
备份策略，目标环境应检查该配置。

若原捕获文件永久不可用，先修复原因，再携带租户头调用
`POST /v1/backup-automation/reset` 显式放弃旧重试。成功历史及远端备份保留；策略开启时下个
维护周期重新捕获。存在运行中备份时返回 409。SDK 对应 `reset_backup_automation()` / `ResetBackupAutomation`。

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
`POST /v1/tasks/{id}/retry` 继续；重启遗留的运行中任务会在查询/恢复检查时标记失败。
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
- 对象备份最多同时运行两个捕获/上传任务，使用独立并发名额。每个上传使用两个工作线程、
  16 MiB 分片。捕获文件打开后，上传只保留文件句柄，不再持有租户读视图或 compact/GC
  执行名额；网络等待期间 GC 和清空租户可以继续。清空租户会移除任务历史和重试输入，
  已打开的上传仍可能在远端完成，但其收尾不能重新创建已清空的任务。
  远端两次写入不是一个多对象原子事务，失败可能留下未引用文件。
- 快照仍为完整逻辑备份，不包含增量备份、跨实例复制或远端数据迁移。

## SDK 和验证

Python：`backup_tenant("source", destination="object")`、`list_object_backups("source")`；
恢复沿用 `restore_tenant("target", backup_key, overwrite=False, dry_run=False)`。
Go：`BackupTenantToObjectStorage`、`ListObjectBackups`、`GetBackupAutomation`，恢复沿用 `RestoreTenant`。
Python 客户端的 `get_backup_automation()` 查询当前租户的自动化状态。

在 Linux/OrbStack 中提供独立测试桶及 `GRAPHDB_TEST_BACKUP_S3_ENDPOINT`、`_BUCKET`、
`_ACCESS_KEY_ID`、`_SECRET_ACCESS_KEY` 后运行 `scripts/object_backup_gate.sh`。
流程包含分片上传、清单发布故障、取消回收、重启后重试、损坏快照拒绝、race 检查，
三副本使用 Leader 备份仓库和 S3 恢复传输中换主续传的回归；
Python SDK 驱动真实 WAL 写入、定时备份/演练、关闭与重置策略、状态重启恢复、
空目录恢复、覆盖恢复和重启查询。
CI 有独立 MinIO 验证任务；原 release gate 可用 `GRAPHDB_GATE_OBJECT_BACKUP=1` 加入该流程。
