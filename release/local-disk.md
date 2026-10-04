# GGraphDB v2.2.3

## 中文

本版修复 2.2.2 发布后发现的持久化和恢复完整性问题：副本缺失已发布依赖时停止应用、S3 恢复切主摘要稳定性、迁移分块与图快照冷校验、来源身份与前缀绑定、离线备份恢复的独占锁和目标目录保护、非 UTC 类型时间的内容校验，以及写后校验 I/O 错误误删发布对象。生产 NGINX 示例同时修复编码写路径的角色绕过；现有部署需更新配置并重新加载。

新增持久化外部备份 worker，支持单机和 Raft 的定时任务、结果不确定时的核对、重试、下载校验、隔离恢复演练和诊断指标，提供 Compose/systemd 示例。保留单机内置备份策略；Raft 不启用内部策略，外部备份历史由显式 S3 生命周期管理。

可用性修复将旧快照清理放到后台，保留有年龄的诊断采样，避免同步 Raft 状态阻塞取消；滚动预检先固定 Leader 的提交位置，压缩幂等记录扫描不再长时间持有前台写锁。快照捕获按目录复用检查与创建，允许读者，编码在捕获锁外执行；维护传输确认首块后最多并发四块。多数派、强读、回滚日志与 fsync 顺序保持原契约。默认单机 direct/WAL、Raft 和租户分片均继续支持。

**性能与可用性边界：** 最终本机性能候选长测完成 45,116 次操作，出现四次入口 503，为 FAIL，故障记录保留。共享内核有严重争用，但根因没有据此被认定为完全解决。捕获微基准分配字节减少约 63–66%，不等于整体读写吞吐收益。本版通过完整标签门禁后才发布，确切资产的运行证据位于包内 `release/evidence/`；跨宿主机、真实容量和天级稳定性仍未认证。

SDK/OpenAPI 为 2.2.3，Go 模块 `/v2`、HTTP `/v1` 不变。单机升级先备份并停止旧进程；不要让两个版本共用目录。Raft 滚动资格仅限包内记录的已修复开发基线 `5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a` 与本版的协议 1 窗口，不代表已发布 2.2.2 的无条件滚动兼容。受来源身份修复影响的数据组不能与旧解释程序混部。升级前完成或取消旧迁移、部分 S3 恢复；全组升级完成前暂停新迁移/恢复，协议 2/3 在全组支持后独立激活。

实际门禁、下载核对和未验收项见 [2.2.3 验证](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.2.3.md)及 [升级说明](https://github.com/SamuelSupe/graphdb/blob/v2.2.3/docs/raft-rolling-upgrade.zh-CN.md)。历史报告只证明对应候选，不将失败或跳过变为 PASS。

## English

This patch fixes replica dependency integrity, S3 restore identity after leader
loss, migration/snapshot validation, source identity and prefix binding, offline
recovery locks/targets, UTC timestamp content hashes and post-write I/O handling.
Update and reload the production NGINX template to receive the encoded-path
write authorization correction.

It adds a persistent external backup scheduler for standalone and Raft, with
uncertain-outcome reconciliation, retries, mandatory readback, isolated drills,
metrics and Compose/systemd examples. Standalone built-in scheduling remains;
Raft uses the external worker and explicit S3 lifecycle policies.

Background snapshot cleanup, deadline-aware admission and rolling preflight fixes
improve operational behavior. Snapshot capture reuses directory work and permits
readers; prepared maintenance can pipeline four chunks after its first manifest
is acknowledged. Majority durability, strong reads and fsync ordering remain.

The prior local performance candidate completed 45,116 operations but had four
HAProxy 503 query failures: **FAIL**, retained in the report. Resource interference
does not establish that the server is fault-free. Capture allocation reductions
are phase-specific; overall throughput, cross-host and production capacity remain
unqualified. Publication requires the complete exact-tag gates, with evidence
included in the distribution.

SDK/OpenAPI are 2.2.3; module `/v2` and HTTP `/v1` are unchanged. Standalone upgrades
require stopping the prior process. Rolling qualification binds the fixed
`5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a` development build to this target,
not every published 2.2.2 deployment. Affected multi-source identity graphs cannot
mix with the old interpretation. Finish/cancel old migration and partial S3
restore work, and activate protocol 2/3 separately after all voters support it.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.2.3.tar.gz.sha256
tar -xzf graphdb-v2.2.3.tar.gz
cd v2.2.3
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, source, matching SDKs and
OpenAPI, bilingual docs, standalone/Raft/sharded and backup automation examples,
upgrade scripts, alert rules, build identity and gate evidence. No registry image
or production fault-domain certification is implied.
