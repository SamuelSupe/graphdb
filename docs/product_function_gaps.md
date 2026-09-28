# GGraphDB 2.0 产品边界与后续能力

本文以通用当前态属性知识图谱产品为边界。CMDB 是重点 profile，但数据模型、
查询和存储不绑定 CMDB；RDF/OWL 导入与规则推理不在 2.0 承诺中。

## 当前定位

GGraphDB 2.0 已具备可部署的核心闭环：

- 通用 EntityType/RelationType、labels、可选字段与关系 schema；
- 当前态 commit、来源治理、幂等 ingest、CSV/JSONL import、deadletter 和采集游标；
- GraphQL/JSON DSL 查询、过滤、排序、聚合、分页与 explain/profile；
- 本地 Parquet 持久化、单进程多租户并发、direct/同步 WAL、版本固定读视图；
- 租户生命周期、快照备份恢复、S3 兼容备份、恢复演练、审计、压实、GC 和索引任务；
- 新的分片 `data_hash`、Go/Python 2.0 SDK；不提供 1.x 迁移。

Go/Python SDK。

Go/Python SDK 的 2.0 合同包括 direct `200/207` 结果、WAL durable `202`
acceptance、`Location`/local status、poll/wait，以及 ingest 的
`expected_version`、`failure_mode` 和 `preconditions` 字段。GraphQL 公开合同
只保留 `graph` 查询根；检索增强扩展不属于当前产品能力。

这意味着产品已经越过 demo 和“只有内核”的阶段；GA tag 仍必须由发行证据
认证，不能仅凭功能数量判定。

## GA 发布门禁

### 1. 发行证据

- 单元、vet、race、SDK、direct/WAL HTTP、重启和备份恢复检查；
- 含 compact、GC、index rebuild 的 30 分钟混合负载；
- 可复查性能报告、构建 commit、二进制校验和与发布包验证。

全部门禁通过后才发布，历史报告不能替代当前候选版本证据。

### 2. 生产安全集成

内核默认关闭 pprof，并支持独立 data/admin listener；生产还必须由实际
网关完成认证、租户 header 覆写、RBAC、TLS、限流和网络隔离。参考配置不是
身份系统本身，正式环境需要一次端到端安全验收。

### 3. 目标数据规模容量证据

2.0 发布门禁认证并发提交，不等于认证任意实体/边规模。每个部署目标还要
在等价字段宽度、关系密度、索引和查询混合下运行 capacity baseline，并记录
内存高水位、对象数量/字节、p95/p99 与 compact/restore 时间。

## 非阻断但重要的后续能力

### 长任务可恢复性

- repair 继续下沉到 page/object 级 checkpoint；
- export 增加分片 manifest 和断点续传；
- 对不响应 context 的对象存储调用，cancel 只能在调用返回后生效。

### 治理与运营

- suppressed conflict 按 source/kind/field 聚合与趋势；
- source 覆盖率、policy 变更影响分析和批量导出；
- deadletter 按 collector/batch/time range dry-run 与 task 化 replay。

### 单机边界

分布式 reader/writer、复制、自动故障切换和跨租户事务不属于 2.0。

### 查询产品化

- saved query 参数 schema、默认值和参数校验；
- 稳定 explain/profile 字段版本；
- CMDB、治理和知识图谱常用模板库；
- 模板与异步导出任务组合。

### 备份运营

- 跨区域复制延迟、长期恢复审计和大租户 RTO/RPO 报告；
- 定期自动恢复演练和证据归档。

## 不应误解为 2.0 已支持

- RDF/JSON-LD/Turtle/OWL 原生导入；
- RDFS/OWL 规则推理、本体一致性校验；
- 历史版本或时态图查询；
- 跨租户事务；
- 租户内部行级授权；
- 超过当前单机容量边界的自动图分区。

这些能力需要单独版本承诺，不能通过给现有字段换名来宣称支持。

## 结论

2.0 的正确下一步是把已实现能力变成可重复发布、可安全部署、可量化容量的
产品，而不是继续横向堆查询语法。GA 判定应由 release gate、安全验收和目标
规模报告共同决定。
