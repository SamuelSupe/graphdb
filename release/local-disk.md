# GGraphDB v2.3.2

## 中文

本版修复查询、Raft 恢复、文件系统兼容和导出完整性问题，并做聚焦精简：

- ID 查询和 pattern 起点遵守 kind；整数 DSL 与持久化索引共享数值编码，保留含点类型名。
- 时间条件按真实时刻比较，时间排序归一到 UTC，混合数值／文本排序保持传递性；GraphQL 支持 explain/profile 包装。
- 丢失待发送快照时向 Raft 报告失败；失败快照每 5 秒独立重试；取消已移除节点的发送任务，临时准备错误保持可重试。
- 允许数据目录以上的父目录为符号链接；修复 WAL 部分清理失败后的字节统计。
- 普通 JSON 导出逐条编码；流式写入有进展时续期超时；Go/Python SDK 拒绝缺少完成标记的快照流。
- 删除三处只检查已移除 evidenceSearch 的过期测试，以及三层重复数值索引键包装；保留恢复、持久化及当前兼容性回归测试。

标签流程要求全量 unit/vet/race、SDK、direct/WAL、S3、TLS 网关、Raft 协议 1–3、
已发布 v2.3.0 到本版的协议 1 滚动，以及单机和 Raft 各 30 分钟维护长测通过后才发布；
无门禁豁免。标签源码中的验证记录是待验收计划，最终结果见
[2.3.2 验证](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.2.md)，
确切构建证据随发行包提供。

本版不声称消除单机写入长尾、瞬时 stale 或 Raft 极端延迟。上一版测量属于各自负载，
两种部署不能直接比较。普通 JSON 导出仍需物化图；持续超预算的快照仍拒写；
已提交日志应用失败和 CRC 损坏 WAL 仍按现有保护策略停止。单宿主机验收不构成
跨宿主机、日级稳定性、生产容量或统一低延迟 SLO 认证。

产品、SDK 和 OpenAPI 为 2.3.2，Go 模块 `/v2` 和 HTTP `/v1` 不变。
单机先备份、停止旧进程，再以原目录和前缀启动；Raft 遵循
[滚动升级手册](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md)。
协议激活仍是全组升级后的独立操作，不提供任意版本混部或降级保证。

## English

This patch fixes kind-aware ID lookups, integer and dotted-kind index access,
timestamp predicates, mixed-type ordering and GraphQL explain/profile wrappers.
It retries failed snapshots without new proposals, reports missing outgoing
snapshots to Raft, cancels retired peer senders and retries transient preparation
errors. Local storage accepts symlink ancestors above the data root and preserves
accurate WAL accounting after partial cleanup failures. JSON export encodes records
incrementally, streaming deadlines renew on progress, and both SDKs require a
snapshot completion marker. Three retired evidence-search tests and three redundant
scalar-key wrappers are removed; recovery and compatibility regressions remain.

Publication requires every exact-tag gate, including standalone and Raft
thirty-minute maintenance soaks, with no waiver. Evidence is packaged under
`release/evidence/`; the linked validation record tracks the final downloaded-asset
checks. This patch does not certify elimination of latency tails or staleness,
cross-host reliability, production capacity or a general latency SLO. JSON export
still materializes the graph. Existing committed-apply and corrupt-WAL safeguards
remain. Module `/v2`, HTTP `/v1` and Raft protocol numbering are unchanged.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.2.tar.gz.sha256
tar -xzf graphdb-v2.3.2.tar.gz
cd v2.3.2
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, matching SDKs/OpenAPI,
console sources, bilingual guides, deployment/backup examples, build identity and
gate evidence. No container image publication is implied.
