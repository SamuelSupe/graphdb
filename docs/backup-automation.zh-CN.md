# 自动备份：单机与 Raft

单机可以继续使用[内置租户备份策略](object-backup.zh-CN.md#自动备份)。新增的外部 worker 适用于单机写入/管理入口、单组 Raft 的集群入口及分片 router：通过现有任务 API 创建 S3 逻辑备份，下载验证，每七天执行一次隔离恢复演练。默认每天备份，首次运行立即开始。未配置的租户不会自动加入计划。

该能力纳入 2.2.4，不改变 Raft 协议或此前的混部资格。Raft 的 `backup.enabled` 仍拒绝开启；外部 worker 的计划和诊断保存到自己的持久目录，不由 `GET /v1/backup-automation` 返回。后者仍查询单机内置调度状态。

单机和单组三副本的实际 S3、换主、重试、常驻重启及互斥结果见[本轮验证报告](backup-automation-validation-2026-10-04.zh-CN.md)。

后续[深度故障测试与修复](backup-deep-validation-2026-10-04.zh-CN.md)补充多租户、损坏/不可写状态、只读指标、三阶段强制退出、S3 损坏/缺失和多数派恢复。

## 运行一次或持续运行

服务器须先配置 S3 备份库和已创建的桶。使用能调用 `/v1/tasks` 的写入/管理入口；如配置 HTTP 鉴权，从秘密管理系统注入 `GRAPHDB_BACKUP_AUTOMATION_TOKEN`。它是 HTTP 凭证，不能使用私有 Raft 传输令牌。

```sh
python3 scripts/backup_automation.py \
  --url http://127.0.0.1:8080 --tenant production \
  --state-dir /var/lib/graphdb-backup-automation \
  --metrics-dir /var/lib/graphdb-backup-metrics \
  --interval 86400 --drill-interval 604800
```

Python 3.10 及以上即可，使用仓库自带的标准库 SDK，无新增依赖。可重复传 `--tenant`，顺序处理。添加 `--daemon` 后持续检查；否则适合 cron 或 systemd timer。示例未向当前生产环境安装计划。

每个端点与租户只部署一个调度所有者，保留稳定的入口 URL 和持久目录。进程锁防止同一目录的多个 worker 并行运行；不同机器各自创建独立目录不会获得分布式互斥。不要对同一单机租户同时开启内置与外部计划。

| 参数 | 默认值 | 语义 |
| --- | --- | --- |
| `--interval` | 86400 | 一轮校验/演练全部成功后到下一轮的秒数，至少 60 |
| `--drill-interval` | 604800 | 完整恢复演练间隔；0 关闭完整演练，仍验证下载内容 |
| `--retry-initial` / `--retry-max` | 60 / 3600 | 失败指数退避，最大值不超过 86400 秒 |
| `--http-timeout` | 30 | 单次 HTTP 请求期限 |
| `--max-run-seconds` | 600 | 每个租户每次轮询的等待预算；到期保留任务，下次继续查询 |
| `--poll-interval` | 1 | 已受理任务的查询间隔 |

停机错过的多个周期合并为一次备份；未完成轮次优先恢复。完整演练在首轮执行，后续按间隔决定。恢复、索引构建和传输仍遵守服务器现有资源及快照预算。完整演练不覆盖生产租户，临时目标完成后清理。

租户按顺序处理，各自获得完整等待预算；一个租户长期排队不会耗尽其他租户的预算。多租户最坏总轮询时间约为租户数乘以预算，另加请求结束余量；给 cron/systemd 配置相应期限，或分为独立状态目录的单租户进程。提供的 systemd 示例只配置一个租户。

## Compose

[worker 镜像](../deploy/backup-automation/Dockerfile)包含脚本与 SDK。[配置样例](../deploy/backup-automation.env.example)和[容器覆盖文件](../docker-compose.backup-automation.yml)提供持久状态、Prometheus 指标和自动重启。将服务器 S3 配置与 worker 配置合并到私有 env 文件中。

```sh
# 单机：env 中将 worker URL 设为 http://graphdb:8080。
docker compose --env-file /path/to/backup.env \
  -f docker-compose.yml -f docker-compose.backup.yml \
  -f docker-compose.backup-automation.yml up -d --build

# Raft：worker URL 使用 http://gateway:8080，三个副本配置同一 S3 命名空间。
docker compose --env-file /path/to/backup.env \
  -f docker-compose.raft.yml -f docker-compose.raft.backup.yml \
  -f docker-compose.backup-automation.yml up -d --build
```

worker 不挂载 GraphDB 的数据、WAL 或 Raft 目录，只挂载自己的 state/metrics 卷。不用 `down -v` 删除未完成轮次的状态。分片部署将 worker 接入 router 所在网络，并在所有数据组副本配置一致的备份库；本轮运行验证范围为单机和单组三副本，分片 router 组合仍需部署验收。

## systemd

将脚本与 `sdk/python/graphdb_sdk` 安装到 `/opt/graphdb` 对应路径，创建 `graphdb-backup` 系统用户。将私有配置放到 `/etc/graphdb/backup-automation.env`，URL 使用宿主机可达地址。安装[service](../deploy/systemd/graphdb-backup-automation.service)和[timer](../deploy/systemd/graphdb-backup-automation.timer)后：

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now graphdb-backup-automation.timer
journalctl -u graphdb-backup-automation.service
```

timer 每分钟检查；实际备份间隔由 worker 的持久 `next_run` 控制。service 创建私有状态目录和可读指标目录，单次最长等待十分钟，加上请求结束和退出余量。停止 timer/worker 停止后续调度；已受理的服务器任务仍可能完成，需要停止当前任务时通过任务 API 取消。

## 重试与诊断

受理前先持久化轮次标记，受理后保存服务器任务 ID。HTTP 响应丢失时从最多 1000 条同类型任务中核对唯一的轮次、阶段、输入和 `retry_of`，不盲目再次提交。明确的鉴权、请求大小/语义、容量或受理前错误码可按退避重试；一般 400/404/409、5xx 及网络错误仍按结果未知处理，持久化后的存储错误不能仅凭 4xx 判定未受理。已失败任务通过现有 retry API 复用捕获文件和检查点；源图后续写入不会替换旧重试版本。

无法唯一认领、任务被 GC 删除、检查点损坏或状态文件损坏时停止推进并报错，不自动重新捕获。先查看状态文件与任务列表中的 `backup_automation_cycle`、`backup_automation_phase`，确认已受理任务的最终结果；未确认前不要删除状态目录。若进程恰在写下“提交结果未知”后、实际发送前退出，也需要人工核对后归档该状态，再允许新轮次。

读取状态时检查阶段、待处理任务、轮次标记、受理结果/任务 ID 和时间字段的一致性。损坏或无法持久化的租户状态只停止该租户，不覆盖原文件；其他租户继续运行。一次性模式在任一租户失败时返回非零，常驻模式保留进程并逐轮报告错误。状态目录或进程锁整体不可用时不能安全运行任何租户。

每份备份必须完成 S3 清单、长度及 SHA256/内容校验；定期完整演练还须返回可恢复证明和成功清理。仅全部成功后更新 `last_success`、`last_backup_key` 和版本。dry-run 的校验通过不等于完整恢复通过。错误轮次保持非零退出码、`last_error` 和失败计数；延迟轮询不会把旧错误改成成功。

状态 JSON 权限 0600，不保存 HTTP 或 S3 凭证。`--metrics-dir` 输出 0644 的 `.prom` 文件，可供 node_exporter textfile collector 只读采集；未配置时位于私有 state 目录，采集器须有读取权限。指标包括 `graphdb_backup_automation_last_success_timestamp_seconds`、`last_drill_timestamp_seconds`、`next_run_timestamp_seconds`、`consecutive_failures`、`pending`、`last_run_success`，统一使用 `graphdb_backup_automation_` 前缀，标签为端点与租户。监控完成时间、持续失败及演练过期。数据 RPO 还须核对任务结果/S3 清单中的 `backup_manifest.created_at`/`created_at`：`last_success` 是校验完成时间，长时间重试成功仍可能得到较早捕获的数据。日志为 JSON，完整任务进度仍通过服务器任务 API 查询。

指标文件发布失败时输出 `backup_automation_metrics_error` JSON 日志，已持久化的备份仍继续。此时 `.prom` 可能缺失或过期，须同时监控该错误和文件更新时间；不能把旧指标当作当前状态。状态 JSON 的写入失败仍会阻止该租户发起新任务。

## 保留与备份范围

单机内置策略继续提供数量/期限保留清理。外部 worker 不远端删除备份；其任务在备份库中仍属于手工 API 备份，不会被内置自动清理删除。Raft 或外部单机计划使用对象存储生命周期管理历史，需要显式配置独立桶/前缀及期限；默认不会自动设置生命周期。过期规则会影响匹配前缀中的手工、最新和正在恢复的备份，没有“始终保留最新一份”的保证。版本化桶还需单独处理旧版本；实际规则与异步过期语义见 [AWS S3 说明](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)。

备份仍是已提交版本的完整租户逻辑快照；不包含未提交 WAL、全部运行状态、目录组拓扑和接入幂等历史。完整集群灾备仍按[停机归档步骤](product-operations.zh-CN.md#完整运行状态灾备)执行；本功能不把跨租户快照变成同一全局时间点，也不自动恢复生产。
