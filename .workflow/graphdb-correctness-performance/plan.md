# GraphDB correctness and performance hardening

## Goal

一次性修复本轮审查确认的 GraphDB 正确性与规模化性能问题，同时保持对象存储架构、单 active writer 产品边界和现有 API 合约。

## Success Criteria

- purge/recreate 不会删除或混入新一代租户对象。
- lease 接管后旧 writer 无法发布，禁用/删除状态不会被在途提交绕过。
- reader 能及时看到跨实例生命周期变化，普通请求不再逐次读取 purge tombstone。
- 崩溃遗留 GC task 自动失效；任务取消和进度更新不会相互覆盖；写入检查不扫描历史任务。
- 单实体提交不再为了 no-op 判断遍历、排序和编码全图。
- cursor 翻页从 cursor shard 开始，catalog spec 使用 O(1) 定位。
- 所有 tenant-scoped Put/Delete 都绑定当前 generation；purge 完成后旧请求、旧任务和旧 heartbeat 不能复活对象。
- `data_md5` 在升级前后保持同一内容同一值；增量 fingerprint 不产生共享缓存数据竞争。
- 单实体索引提交只重建受影响的 secondary index、edge shard 和 entity page，不再构造整图 artifacts。
- write cache 的字节上限覆盖可变长字段；写 admission 和 cursor 翻页不重复执行可避免的远端 GET/全 catalog hash。
- 定向回归、全量测试、vet、关键 race、benchmark 和 OrbStack/RustFS S3 集成全部通过。

## Current Context

- 基线：`main` 与 `origin/main` 同步，当前分支 `codex/fix-correctness-performance`。
- 基线验证已通过 `go test ./...`、`go vet ./...`、关键包 race 与 RustFS S3 集成。
- 唯一无关文件为未跟踪的 `internal/.DS_Store`，必须保留且不纳入变更。

## Constraints

- 单文件保持适度长度，按职责拆分；实现简洁，不引入通用框架式抽象。
- 测试需要对象存储时使用本机 OrbStack。
- 不改变公开 API，除非正确性需要新增向后兼容的持久化字段。
- 不启动子代理；按隔离工作包在当前 agent 内执行。

## Risks

- lease/tombstone/task marker 都是持久化控制对象，必须兼容现有 Parquet 记录。
- 对象存储不支持条件 DELETE，清理逻辑必须通过代际键或条件 PUT 避免 ABA。
- 性能优化不得降低 no-op、内容一致性和 cursor snapshot pinning 的正确性。

## Approval Required

本地可逆代码修改和测试不需要额外批准。推送、发布、生产数据操作不在本目标范围内。

## Work Packets

1. `packets/01-fencing-lifecycle.md`
2. `packets/02-task-state.md`
3. `packets/03-commit-noop.md`
4. `packets/04-scan-pagination.md`
5. `packets/05-verification.md`
6. `packets/06-generation-fencing.md`
7. `packets/07-fingerprint-compatibility.md`
8. `packets/08-true-incremental-indexes.md`
9. `packets/09-cache-admission-scan.md`
10. `packets/10-reverification.md`

## Integration Policy

按正确性依赖顺序集成：generation fencing -> fingerprint/API compatibility -> true incremental indexes -> cache/admission/scan。每个包先跑定向测试；若持久化格式变更，必须先补兼容读取测试。2026-07-15 的复审结论覆盖此前 final report 中“全部完成”的判断。

## Verification

窄测试 -> package tests -> benchmark 对比 -> `go test ./...` -> `go vet ./...` -> 关键包 race -> OrbStack/RustFS S3 integration。

## Reusable Artifacts

保留本目录中的计划、包结果和最终报告，作为后续 GraphDB 边界修复与性能回归模板。
