# GGraphDB 1.1 产品成熟度与剩余缺口

本文以通用当前态属性知识图谱产品为边界。CMDB 是重点 profile，但数据模型、
查询和存储不绑定 CMDB；RDF/OWL 导入与规则推理不在 1.1 承诺中。

## 当前定位

GGraphDB 1.1 已具备 release candidate 所需的核心闭环：

- 通用 EntityType/RelationType、labels、关系属性 schema，以及 1.0
  CIType 和 layout version 2 兼容；
- 当前态 commit、source/field priority、幂等 ingest、CSV/JSONL import、
  deadletter 和 collector cursor；
- match/pattern/traverse/impact/shortest path、过滤、排序、聚合、游标、
  explain/profile 和 saved query；
- Parquet snapshot/page/index/shard、reader/writer 分离、reader freshness；
- tenant lifecycle、clone、backup/restore/dry-run、restore drill、跨
  bucket/prefix migration；
- integrity audit、repair dry-run/apply、compact、GC、统一 task；
- local 单 writer 与 PostgreSQL CAS 2–8 writer 两种协调模式；
- 真实 1.0/1.1 双向二进制兼容门禁、结构化错误码、OpenAPI 和双 SDK。

这意味着产品已经越过 demo 和“只有内核”的阶段，但 GA 仍取决于发行证据，
不是仅凭功能数量判定。

## GA 阻断项

### 1. 发行证据

- 每个候选版本必须完成 8 writer 并发正确性门禁，以及 2 个活跃 writer、
  20 commit/s、30 分钟 PG/RustFS 容量门禁；
- RustFS、PG CAS、restore drill、race、真实 1.0 二进制兼容必须成为发布
  job 的硬依赖；
- 保存容量报告、失败日志、构建 commit、校验和和恢复证明。

门禁和机器可读边界已经实现，只有全部 CI 结果通过的 tag 才能标记 GA。

### 2. 生产安全集成

内核默认关闭 pprof，并支持独立 data/admin listener；生产还必须由实际
网关完成认证、租户 header 覆写、RBAC、TLS、限流和网络隔离。参考配置不是
身份系统本身，正式环境需要一次端到端安全验收。

### 3. 目标数据规模容量证据

1.1 发布门禁认证并发提交，不等于认证任意实体/边规模。每个部署目标还要
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

### Reader Fleet

- reader inventory、drain/undrain、指定租户 reload；
- 跨进程运行中查询与取消需要独立控制平面；
- fleet 容量、版本分布和流量闸门的统一视图。

### 查询产品化

- saved query 参数 schema、默认值和参数校验；
- 稳定 explain/profile 字段版本；
- CMDB、治理和知识图谱常用模板库；
- 模板与异步导出任务组合。

### 备份运营

- 跨区域复制延迟、长期恢复审计和大租户 RTO/RPO 报告；
- PostgreSQL coordination schema 与对象存储的一致备份编排；
- 定期自动恢复演练和证据归档。

## 不应误解为 1.1 已支持

- RDF/JSON-LD/Turtle/OWL 原生导入；
- RDFS/OWL 规则推理、本体一致性校验；
- 历史版本或时态图查询；
- 跨租户事务；
- 租户内部行级授权；
- 超过当前单租户 CAS 容量边界的自动图分区。

这些能力需要单独版本承诺，不能通过给现有字段换名来宣称支持。

## 结论

1.1 的正确下一步是把已实现能力变成可重复发布、可安全部署、可量化容量的
产品，而不是继续横向堆查询语法。GA 判定应由 release gate、安全验收和目标
规模报告共同决定。
