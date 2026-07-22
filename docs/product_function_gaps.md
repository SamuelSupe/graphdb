# GraphDB 图数据库功能成熟度缺口

本文从通用图数据库内核和场景能力的功能产品化角度评估，不覆盖性能、容量、压测、UI、复杂认证、上游数据校验等问题。文中标注的 CMDB 能力代表一个重点应用场景，不是图数据库内核的使用限制。

## 当前功能完成度

在当前“通用图内核 + CMDB 首个重点场景 profile”的目标范围内，完成度约为 **8/10**。

在“通用实体关系图数据库内核、对象存储持久化、当前态查询，以及 CMDB 作为首个重点场景 profile；上游负责验证、不需要历史版本、默认写入覆盖”的边界下，系统已经具备试生产能力。核心图模型、写入治理、批量接入、查询 DSL、对象存储持久化、多租户隔离、reader/writer 分离、基础运维 API 都已经覆盖。

## 已具备的主要能力

- 多租户基础：`X-Tenant-ID` 隔离、租户配置、租户状态、租户用量、reader freshness。
- 数据模型：schemaless entity、CIType、RelationType、canonical edge、`type/from/to` 去重。
- 写入治理：写入即覆盖、MD5 一致跳过、source policy、字段级 source 优先级、edge 字段和存在性 source 治理、suppressed conflict。
- 采集接入：batch ingest、collector status、idempotency、source cursor、conflict report、deadletter。
- 查询能力：match、neighbors、traverse、impact、shortest path、range/in/exists/prefix/fuzzy、sort、project、aggregate、cursor、timeout、cost limit、explain/profile。
- 运维 API：scan/list/export、index rebuild、compact、GC、task 基础、health、reader readiness、running query 控制。
- 错误与保护：结构化 error envelope、backpressure 429、reader 拒写、writer/reader 模式隔离。
- 存储功能：commit log、manifest、snapshot、Parquet page/index/shard、GC/retention 基础、repair/inspection 基础。

## P0 缺口

### 1. 统一任务系统

统一 task 模型已经覆盖 compact、GC、repair、export、deadletter replay、
index rebuild、tenant backup/restore。task 对象具备统一 ID、状态、阶段、
进度、checkpoint、开始/结束时间、错误信息、list/get/cancel/retry。
GC 和 deadletter replay 已支持 cursor checkpoint 续跑，cancel 状态会持久化并被运行中的 writer 轮询观察。
compact、export_snapshot、tenant_backup、tenant_restore 已增加 `actions`
checkpoint，每个 action 记录稳定 action ID、状态、输入/输出对象和校验信息；
retry 会在输出对象仍可读、版本或 integrity 校验通过时跳过已完成子步骤。

剩余需要补齐：

- repair 的 checkpoint 仍主要是 action 级进度，部分大对象扫描还不是 page/object 级断点。
- repair 需要更细的 action-level checkpoint，避免大租户修复一次性跑完。
- export 需要面向大结果集的分页/分片 checkpoint 和 export manifest。
- cancel 对不响应 context 的底层对象存储调用仍只能在操作返回后生效。

### 2. 租户生命周期管理

租户当前已经不是纯 prefix 自然存在，基础生命周期已经具备：
create/list/disable/delete、软删除/purge、clone、backup/restore task，以及
create 时写入 tenant config/source policy 模板。

剩余需要补齐：

- 租户迁移，例如跨 bucket 或 prefix 迁移。
- backup/restore dry-run 和更严格的完整性校验。
- 恢复演练和跨对象存储迁移工具。

### 3. 备份、恢复和迁移

当前 export current snapshot 可以满足部分运维导出，tenant_backup /
tenant_restore task 已覆盖元数据、tenant config、source policy 和当前
snapshot 的基础备份恢复，并已增加独立 backup manifest、restore dry-run
和同 tenant ID 跨 bucket/prefix 迁移工具。

仍需补齐：

- 灾难恢复演练命令或 e2e 测试。
- 跨 tenant ID 迁移的重写式流程和恢复演练自动化。
- 更长周期的备份恢复审计、跨区域复制延迟和大租户恢复耗时报告。

### 4. 全链路完整性审计和 Repair

已有 health/inspect/repair 基础，但还需要更产品化的审计入口。

需要补齐：

- manifest -> snapshot catalog -> schema/entity pages/edge shards -> index catalog 的完整链路校验。
- content_hash、schema_hash、row_count、bytes 的统一审计报告。
- repair dry-run，明确会修什么、删什么、重建什么。
- repair checkpoint，大租户修复不能一次性跑完。
- repair 后的验证报告。

## P1 缺口

### 5. 场景查询模板和运维查询产品化

查询 DSL 能力已经比较完整，但面向具体领域的常用查询还需要更明确的产品封装；CMDB 查询是当前优先补齐的一组模板。

需要补齐：

- saved query/template 的稳定 API 契约。
- 常见 CMDB 查询模板，例如影响分析、依赖展开、孤儿 CI、重复 CI、按 source 覆盖率统计。
- explain/profile 输出字段稳定化。
- 查询模板参数校验和默认 limit/cost。
- 查询结果导出与模板结合。

### 6. 数据治理可观测入口

当前 source policy 和 suppressed conflict 已有写入时反馈，但缺少面向运营的汇总查询。

需要补齐：

- suppressed conflict 按 tenant/source/entity kind/field 的汇总。
- source 覆盖率统计。
- 低优先级写入被抑制的趋势。
- source policy 变更后的影响分析。
- conflict 批量导出。

### 7. Deadletter Replay 产品化

deadletter 已有基础，但 replay 应进入统一任务系统。

需要补齐：

- 按 source/collector/batch/time range replay。
- replay dry-run。
- replay 结果报告。
- replay 幂等。
- replay 与当前 source policy、写入覆盖规则一致。

### 8. Reader Fleet 级运维控制

reader freshness 已有基础，但生产运维还需要更完整的 reader fleet 视图。

需要补齐：

- list readers。
- 每个 reader 当前版本、lag、最近 heartbeat、是否可放流量。
- reader 手动 drain/undrain。
- reader reload 指定 tenant。
- reader 版本落后时的明确错误码和运维建议。

## P2 缺口

### 9. 错误码契约

已有结构化 error envelope，稳定 top-level code 已在
`docs/error_codes.md` 文档化，并由 OpenAPI/HTTP 测试锁定。

仍需持续遵守：

- 新增错误场景必须复用现有 code，或先扩展 `docs/error_codes.md`、
  `docs/openapi.yaml` 和契约测试。
- backpressure `reasons[]` 可以细分原因，但 top-level `code` 必须保持稳定。

### 10. 运维导出体验

已有 scan/list/export，但还可以继续稳定契约。

需要补齐：

- export task 化。
- export checkpoint。
- export manifest。
- export current snapshot 与按 kind/type 增量导出的区别文档。
- 大结果分页游标稳定性说明。

## 建议实施顺序

1. 统一任务系统：继续把 task checkpoint/resume 做到更多长任务。
2. 全链路 integrity audit + repair dry-run。
3. tenant backup/restore dry-run、完整性校验和跨 bucket/prefix 迁移。
4. 租户生命周期 API 稳定化。
5. 查询模板和治理统计。
6. reader fleet 运维视图。
7. 错误码文档冻结。

## 结论

当前系统已经不是 demo，功能上可以进入内部试生产验证。下一阶段不应继续优先堆查询语法，而应补齐运维闭环：任务、租户生命周期、备份恢复、完整性审计、repair、治理统计和错误码契约。
