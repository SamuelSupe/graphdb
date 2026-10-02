# GGraphDB 产品边界与后续能力

发行版为 **2.1.2**；本文同时说明 `codex/raft-ha` 候选。产品定位为持续更新、在线查询当前状态的属性图数据库，保留单机并支持 Share-Nothing Raft 和按租户分片。候选不等于已发布资产。
CMDB 是重点应用场景，数据模型、查询和存储不绑定 CMDB。
版本、数据格式和升级边界以[版本契约](naming-and-compatibility.zh-CN.md)为准。

## 已实现的能力

- 通用 EntityType/RelationType、labels、可选字段与关系 schema。
- 当前态 commit、来源治理、幂等 ingest、CSV/JSONL import、deadletter 和采集游标。
- GraphQL/JSON DSL 查询、过滤、排序、聚合、分页、流式读取与 explain/profile。
- 本地 Parquet 持久化、单进程多租户并发、direct/同步 WAL、版本固定读视图。
- 租户生命周期、快照备份恢复、审计、压实、GC、索引和持久化任务。
- S3 自动备份调度、持久化重试、完整下载校验、保留清理、可选周期恢复演练和状态/重置 API。
  自动化默认关闭，详见[备份指南](object-backup.zh-CN.md#自动备份)。
- `replay_deadletters` 异步任务及检查点重试；进一步的筛选与预演仍属后续工作。
- 分片 `data_hash`，配套 Go/Python SDK 2.1.2。

SDK 保留 direct `200/207` 结果、WAL durable `202` 接管、`Location`/本地状态查询、
poll/wait，以及 ingest 的 `expected_version`、`failure_mode` 和 `preconditions`。
GraphQL 公开合同使用 `graph` 查询根；检索增强扩展不属于当前能力。

## 当前发行与验证边界

2.1.2 已发布，单元/vet/race、SDK、direct/WAL HTTP、重启、S3 备份恢复和发行包检查通过。
按本次发布要求，30 分钟混合负载提前停止，**未完成，不计为通过**。
默认后续发布流程仍包含该检查；见[发布清单](release-checklist.md)和
[本版验证记录](validation-v2.1.2.md)。历史测试不能替代当前候选证据。

[写入长尾实测](performance-write-tail.md)记录了重点负载的改善，也保留了秒级长尾和
压实退化信号。容量状态仍为 `performance_unqualified`，没有任意实体/边规模或延迟保证。
目标部署应按实际字段宽度、关系密度、索引和查询组合核验资源与延迟。

## 当前限制与后续候选

以下内容没有已承诺的交付版本或日期。

### 生产部署与运维

- 认证、租户授权、TLS 和网络隔离由外部网关负责；默认 Compose 不是完整的安全生产部署。
- 候选已增加实际磁盘剩余空间准入、维护/恢复/迁移预检；并发估算不能替代容量资格验证。
- 候选已增加统一诊断、磁盘/复制/任务告警和受保护部署模板，见[运维说明](product-operations.zh-CN.md)。真实认证后端和故障域仍待部署验收。

### 备份恢复

- 已实现定时备份、重试、保留清理和周期恢复演练，不再列作缺失能力。
- 当前是完整逻辑图快照，不包含待发布 WAL、任务历史、幂等历史、采集游标和保存查询模板。
  候选新增离线完整运行状态归档，覆盖这些状态并验证恢复后幂等续跑；在线完整备份与按租户可移植运行状态恢复仍未提供。
- 尚无增量备份或按任意时间点恢复；大租户恢复时间、可恢复时间范围和长期演练审计需量化。
- 远端权限、独占桶/前缀和版本化桶边界见[备份指南](object-backup.zh-CN.md)。

### 长任务、查询与治理

- repair 可进一步细化到 page/object 检查点；export 仍缺少分片 manifest 和下载断点续传。
- saved query 执行已保存的固定请求，尚无参数 schema、默认值和调用参数校验。
- explain/profile 字段版本、常用查询模板及与异步导出的组合需要明确的产品契约。
- source 覆盖率、冲突趋势、policy 影响预演及 deadletter 按 collector/batch/time 的筛选可继续完善。

## 未承诺支持的能力

- RDF/JSON-LD/Turtle/OWL 原生导入、RDFS/OWL 推理或本体一致性校验。
- 历史版本或时态图查询。
- 独立 reader/writer、租户内自动图分区。候选 Raft 已支持复制、换主和租户分片扩容；一个大租户仍属于一个数据组。
- 跨租户事务或租户内部行级授权。
- 1.x 数据迁移、旧 MD5 响应或跨主版本回滚。

这些能力需要单独设计和版本承诺，不能由已有接口名称或兼容路由推断支持。
