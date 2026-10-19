# GGraphDB v2.3.2 失败候选记录

**NOT_RELEASED**：标签 `v2.3.2` 保留在 `f8592dd8254bffd72c846fd83ba71f599a020849`，没有创建发行版。
[标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39251439319) 的全量普通测试失败：
`TestHAMembershipReplacementAndTenantRestore` 中，新节点反复拒绝不包含自己的旧成员快照，无法追平。

根因：快照构建期间应用成员变更时，后续快照触发条件被丢弃；没有新提案时，旧快照一直保留。
新增确定性回归 `TestSnapshotIncludesMembershipChangedDuringBuild` 在此标签生产代码下稳定失败，
修正后连续 20 轮普通、5 轮 race 通过。后续版本继续独立验收，见 [2.3.3 验证](validation-v2.3.3.md)。

本候选单机 direct/WAL、S3、TLS 网关门禁已通过；因候选失效而停止其余运行，
未完成的长测不算通过。没有门禁豁免，没有移动或覆盖失败标签。
