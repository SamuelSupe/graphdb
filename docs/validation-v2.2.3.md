# GGraphDB 2.2.3 validation and release scope

## 中文

2.2.3 包含 2.2.2 发布后的持久化、恢复、租户迁移、来源身份、网关路径授权、UTC 内容摘要与可用性修复，以及持久化外部备份 worker、快照捕获和维护分块传输优化。同一二进制继续支持单机 direct/WAL、单组 Raft 和租户分片。SDK/OpenAPI 为 2.2.3，Go 模块 `/v2`、HTTP `/v1` 不变。

本页在标签门禁启动前记录验收范围；不能把以下清单当作 PASS。正式发行由 [GitHub 标签工作流](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml) 的全部依赖成功后创建。实际工作流和包内 `release/evidence/` 绑定确切提交与二进制 SHA256；下载资产另行核对外层校验文件、内层 SHA256SUMS 和版本信息。

### 历史候选结果

- [完整测试及修复](full-validation-2026-10-03.zh-CN.md)、[备份自动化](backup-automation-validation-2026-10-04.zh-CN.md)和[备份深度故障测试](backup-deep-validation-2026-10-04.zh-CN.md)只认证各自固定候选，保留失败及未运行项。
- [可用性修复候选](raft-availability-fixes-2026-10-04.zh-CN.md)在本机完成 62,029 次操作、零非预期错误；该结果不转移到后续候选或发行二进制。
- [最终性能候选](performance-raft-batching-2026-10-04.zh-CN.md)的相关 race、单机/Raft/分片、混部滚动和 42 个集群正确性窗口通过。三十分钟维护长测完成 45,116 次操作，但四次查询收到 HAProxy 503；该轮为 **FAIL**，不会因重新构建或新的门禁通过而改写。共享内核负载达到 174，并有严重换页；环境争用不是排除服务端缺陷的证明。
- 捕获微基准分配字节下降约 63–66%，分配次数下降约 60–64%；阶段耗时测量改善不构成整体吞吐或维护任务提速保证。整体吞吐仍未获得稳定 A/A 对照结论，出现回退的强读合并实验已撤回。

### 本次标签必须通过的门禁

全仓 unit、vet、race、Go/Python SDK、备份 worker 回归和版本契约；单机 direct/WAL HTTP、重启一致性和三十分钟维护负载；真实 MinIO/S3 备份、拒绝损坏、重开和全新目录恢复；真实 TLS/角色/租户头网关；Raft 协议 1/2/3、分片、故障恢复、磁盘压力、串行重启和三十分钟维护负载；告警规则和实际指标格式；发行包解压后的 Compose、备份 worker 和容器启动。

发行滚动门禁的来源为已修复图/来源身份、迁移、前缀和离线恢复完整性的开发提交 `5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a`，不是已发布的 v2.2.2。来源与目标摘要记录在 `release/evidence/raft-gate/rolling/metadata.json`；资格仅限协议 1 和实际验证窗口。受来源身份解释修复影响的数据组不能与旧解释程序混部。升级前完成或取消旧迁移和部分 S3 恢复，窗口内暂停这些新任务；未验证版本使用维护升级或经过校验的备份恢复到新组。协议 3 激活仍是全组升级后的独立变更。

Linux amd64 发行程序必须与协议 1/2/3、滚动目标和网关门禁二进制摘要相同。Linux arm64 与 macOS arm64 单独构建并校验；amd64 运行结果不能转移为其他架构的全量运行认证。发行包包含备份自动化脚本、SDK、Compose、worker Dockerfile、systemd 示例和诊断/恢复证据。

### 尚未验收

按用户决定，跨宿主机保留 NOT RUN。真实容量、慢盘/网络长尾、24/72 小时稳定性、生产身份系统、告警通知和分片 router 的备份调度联测仍待验收。默认快照预算 512MiB；逻辑备份不含全部运行态或待发布 WAL，不提供 PITR、自动租户均衡或租户内部图分区。历史 `release/capacity-envelope.yaml` 不认证本版或 Raft 容量，不承诺固定 RTO、统一吞吐增幅或低延迟 SLO。

## English

This patch retains standalone direct/WAL, Raft and tenant-sharded deployments,
and includes integrity, recovery, gateway authorization, rolling preflight,
UTC timestamp, availability and backup automation fixes. Capture and maintenance
transfer optimizations preserve majority durability and strong reads.

The prior performance candidate's 30-minute workload **failed** with four
HAProxy 503 query errors among 45,116 operations. That historical result remains
unchanged. Severe shared-kernel pressure was observed but does not exonerate the
server. Phase-specific allocation reductions do not qualify overall throughput.

The exact tag workflow must independently pass all release gates and bind the
packaged amd64 binary to Raft/rolling/gateway evidence. Its rolling source is the
fixed development commit `5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a`, not the published
v2.2.2 artifact. Affected multi-source graphs cannot mix with programs retaining
the old identity interpretation. See the [upgrade boundary](raft-rolling-upgrade.zh-CN.md).
Cross-host, production capacity, day-scale stability and external integrations
remain unqualified. No release-specific gate waiver is requested or applied.
