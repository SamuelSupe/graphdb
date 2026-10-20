# GGraphDB v2.3.10 发布验证 / Release validation

**CANDIDATE — main CI, exact-tag gates, publication and independent download checks pending.**
[机器记录](validation-v2.3.10.json)与本页区分源码、本地运行、远程门禁及实际发布；没有门禁豁免。

## 来源与标签

本版承接 [v2.3.9 候选的完整修复](validation-v2.3.9.md)。v2.3.9 标签 `8bc9884e` 的 main CI 六项通过，但[确切标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39477688507)的静态测试和旧版滚动追赶失败，没有发行资产；该标签与 v2.3.8 标签、资产均不移动。

## 本次修正与本地证据

- 静态门禁失败来自快照重试后继续使用五秒前的旧 Leader。补入真实领导权转移场景，原测试三次重现；改为选择当前已恢复的 Leader 后，六个场景各三轮 race 通过（91.408 秒）。保留拒写、强读、无需新业务提案即可恢复、文件清理和 source 关闭断言；原 15 秒场景预算及生产代码不变。
- 完整 replication 包 unit／vet／race 通过（47.009／49.310 秒）。此前生产源码 `8416525b` 的 OrbStack Go 1.26.7 全量 unit／vet／race、范围页写入及真实 Raft／迁移／GC／备份新目录恢复证据保持其原范围，本版还必须通过新的 main 和标签门禁。
- CI 的 v2.3.0 回滚节点已安装新快照并继续提供服务，但追赶检查超时。使用真实旧源码 `91ffe503` 编译的程序与相同存储／复制核心，在 OrbStack 完整执行原滚动流程两次（默认资源及限制到两颗 CPU），均通过。未稳定复现该 CI 超时，因此没有修改追赶条件、并发流量、数据一致性或时限。仅增加失败时最后一次节点状态文件 `catchup-failure.json`，并让现有滚动门禁也在 main CI 运行。这个问题必须由后续远程门禁实际通过，不能以本地成功豁免。
- SDK／OpenAPI／文档版本同步为 2.3.10；Go 的 docs／SDK／cmd 测试及版本冻结检查通过；Python SDK 共 13 项，1 项可选 live-API 跳过，其余通过。工作流 YAML 与 shell 语法检查通过；受控追赶失败保留原错误并正确记录节点 commit／applied 状态。

## 范围分页与兼容边界

[启用说明与基准](entity-range-paging-implementation.zh-CN.md)记录实体及正反向边的有界物理页。开关 `GRAPHDB_INDEX_RANGE_PAGES` 默认关闭，全部相关节点升级后才能开启；旧程序不能直接分页新目录。数据页最多 1024 条，以实际编码后 8 MiB 为目标，单条超大合法；完整目录的成本仍随页数增长。索引降级需要先用新程序重建旧布局，不能据此降级整个 WAL／Raft 目录。

## 待完成资格

最新 main 全部 CI，确切标签全部发行门禁（含真实 v2.3.0 滚动、S3／网关及单机／Raft 各 30 分钟长测），独立下载源码／资产／校验和／二进制、OrbStack 新目录恢复与重放，以及最终验证文档提交后的 main CI 均待完成。发布资产只由全部必需门禁通过后的工作流生成。

未声明物理跨主机故障域、断电、日级稳定性或生产容量保证。
