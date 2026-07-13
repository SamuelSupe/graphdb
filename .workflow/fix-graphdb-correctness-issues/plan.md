# Fix GraphDB correctness issues

## Goal

一次性修复已确认的六类 GraphDB 正确性问题，并用故障注入、竞态和集成测试证明关键不变量。

## Success Criteria

- GC checkpoint 不能删除当前 manifest 引用的 snapshot 或其 sharded objects。
- disable/delete 返回后，不再有先前进入的 commit 能发布；purge 与所有 tenant mutation 串行。
- 所有会修改 tenant 数据的维护入口在 storage 层统一拒绝 disabled/deleted tenant。
- direct commit 在 manifest 已发布但幂等记录写失败后，重试同 key/body 能恢复原结果；writer 切换不会因本地负缓存漏读记录。
- restore drill 只能使用空的隔离目标，cleanup 只能删除本次 drill 创建的 tenant。
- index rebuild 终态可见后，新请求一定创建新 task。
- `go test -mod=readonly ./...`、`go vet -mod=readonly ./...`、相关 `-race` 回归和 OrbStack 对象存储集成检查通过。

## Current Context

- 当前分支 `main`，HEAD `41687b70`。
- 工作树原有未跟踪文件 `internal/.DS_Store`，必须保留。
- 默认 `go test` 受本机残留空 vendor 目录影响，验证显式使用 `-mod=readonly`。

## Constraints

- 单体文件保持短小，优先把新逻辑放在职责明确的小文件中。
- 实现简洁，不引入通用框架或过度抽象。
- 不修改、清理或提交无关文件。
- 需要对象存储集成时使用当前 OrbStack Docker context。
- 本任务无前端变更，不需要浏览器验证。

## Risks

- GC、purge、restore 测试涉及删除，只能针对 MemoryStore 或一次性 OrbStack 测试前缀。
- 生命周期锁调整需避免嵌套获取同一个 tenant lock 造成死锁。
- 幂等恢复必须校验 tenant、key 和完整 request body，不能把不同请求误判为 replay。

## Approval Required

本地代码和测试修改无需额外批准。删除真实数据、部署、提交、推送均不在当前授权范围内，若后续需要必须先询问。

## Work Packets

- C1: GC checkpoint 安全校验与回归测试。
- C2: tenant lifecycle 原子性、purge 串行化、storage writable guard。
- C3: direct commit 幂等恢复与跨 writer 缓存正确性。
- C4: restore drill 目标所有权和 cleanup 安全。
- C5: index rebuild 完成态竞态。
- C6: 集成、全量验证与剩余风险审计。

## Integration Policy

按 C1-C5 分别实现并运行窄测试；每个包只接受能保持 manifest visibility、tenant lifecycle 和 idempotency 不变量的改动。C6 统一解决交叉影响，不以放宽断言通过测试。

## Verification

先运行每个 packet 的定向单测，再运行 storage/httpapi race 回归、全量测试与 vet；最后在 OrbStack 上运行仓库现有的 S3/RustFS 集成测试或最接近的 disposable-prefix 验证。

## Reusable Artifacts

保留本目录的 packet/result/final-report，作为后续数据库正确性修复模板。
