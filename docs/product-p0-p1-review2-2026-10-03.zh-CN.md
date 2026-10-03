# 产品 P0/P1 后续审核与修复

本轮审核基于独立工作树 `codex/raft-ha` 的 `dbafd81c63897726207f7e7ac86ce7d2c1ed33b2`，包含上一轮尚未发布的修复。覆盖单机 direct/WAL、Raft 复制、持久化图加载、孤儿提交恢复、备份恢复和分片迁移边界。未复现新增 P0，复现并修复三项 P1；本轮不是新版本发布。

| 问题 | 修复前的可观察结果 | 修复 |
| --- | --- | --- |
| P1：manifest 丢失被解释为空租户 | 真实 FileStore 冷重开后，原版本 1 被读取为版本 0 的空图；继续写入会发布版本 1 且原实体不可见。三副本中，故障副本完成同一压缩任务并推进 checkpoint，与健康副本形成分歧。 | 头部不存在时检查已有图对象；普通读取和发布返回错误。故障发生在 Raft 应用时回滚并停止副本，不推进 checkpoint。首次隐式写入在暂存提交前持久化版本 0 的空 head，保留失败重试和幂等语义。显式单机 repair/recover 及已验证输入的克隆、恢复仍可用。 |
| P1：有效快照未绑定到当前头部的图摘要 | 同租户、同版本的有效 Parquet 快照具有不同逻辑内容，但完整图加载仍成功；对象自身的校验无法证明它属于当前已发布图。 | 完整图冷加载核对当前 `sha256-shards-v2` 摘要，拒绝不匹配内容。孤儿提交恢复和 manifest 重建在发布前更新图摘要，避免正确恢复的数据被新校验拒绝。 |
| P1：快照加载制造来源身份并错误合并实体 | 高优先级 manual 拥有实体来源元数据，外部 ID 实际来自 aws，显式 `sources` 仅记录 aws 身份。旧加载路径拼接出不存在的 manual 身份，把同名外部 ID 的独立 manual 实体合并掉；边也增加虚假来源别名。 | 显式来源身份存在时保留其权威性；仅为没有来源身份的旧记录回填标量来源，并保留真实传入边 ID 的别名。压缩后的来源优先级、身份和图摘要保持一致。 |

快照回归模拟错误恢复副本，使用正常编码器生成有效文件，没有绕过 Parquet 校验。空摘要及旧算法摘要继续可读。本轮未更改日志编码或协议编号，但来源身份修复的执行语义不等同于旧程序，升级边界见下文。局部 Parquet/索引读取和已发布内存视图不逐次重算全图摘要；这不是全请求磁盘损坏探测，也不能识别整个目录被替换为另一份自洽数据。完整图冷加载增加摘要计算，本轮没有吞吐提升声明。

## 回归证据

- `TestMissingManifestRejectsColdReadAndWriteUntilRepair`：真实目录，分别覆盖 commit 尾部和 compact 后的快照；冷重开拒绝空图、新写入和同名租户创建，显式 repair 保留实体，后续写入成功。修复前见 `missing-head-before.log`；补充的暂存目录创建入口复现见 `create-before.log`。
- `TestLoadRejectsSnapshotWithDifferentLogicalContent`：保留原 manifest 和摘要，只复制另一份同租户、同版本的自洽快照目录；冷加载拒绝错误内容，同时保留空摘要和旧摘要兼容性。修复前见 `catalog-before.log`；最初完整快照记录路径的复现保留在 `storage-before-final.log`。
- `TestRecoverTenantAndCleanupStaleCommits`：恢复孤儿提交后清空内存缓存，重新加载并核对实体与摘要；`TestRecoverInitialUnpublishedCommit` 验证没有初始头部时的显式恢复。
- `TestHAReferencedGraphFailureStopsReplicaUntilReplay/missing_manifest`：真实三节点、独立本地目录，停副本后删除其 head 并冷重开；检查 checkpoint 不推进、任务仍 queued、健康多数派继续写入；恢复原文件并重开，原日志重放及两实体完整性通过。修复前见 `ha-before.log`，错误副本曾保存 succeeded 并推进至相同 checkpoint。
- `TestFromSnapshotPreservesExplicitSourceIdentities`：检查两个来源中的同名外部 ID 保持两个独立实体，边的显式 AWS 别名不增加虚假 manual 身份。修复前见 `identity-before.log`。扩展现有 `TestCommitIngestAndCompactApplySourcePolicyFieldPriorities`，检查实际写入、ingest、幂等重放和压缩冷加载后的来源身份。

完整验证还捕获首次失败写入的重试边界与正常克隆误判，均通过图数据发布流程修正。统计缓存测试只拦截真正的全量 usage 扫描，分页用例使用已有空 head 保留原分页数量断言；新增头部存在性探测不计为全量 usage 扫描。

## 验证与边界

最终验证通过；构建身份和逐包结果记录在[验收 JSON](product-p0-p1-review2-2026-10-03.json)。运行环境为 OrbStack Linux arm64、Go 1.26.7，S3 服务为 MinIO `RELEASE.2025-09-07T16-13-09Z`。原始证据位于 `/tmp/graphdb-p0p1-review6-20261003/`。

| 验证 | 当前结果 |
| --- | --- |
| 单机 direct/WAL 实际进程 | PASS：E2E、4 writers/16 readers、12 项 Python SDK 测试、重启前后完整快照相同 |
| 三副本与分片部署 | PASS：同候选镜像的 23 项门禁，含单机共存、SIGKILL、隔离多数派、36 MiB 恢复换主续传、扩容、迁移中断、迁回和目录组失联回退 |
| MinIO S3 备份恢复 | PASS：storage/backupstore 和 HA 的 S3 race 联测；WAL 自动备份、空目录恢复、重启核对 |
| 全仓 Go 测试 | PASS：16 个有测试包 |
| vet | PASS：静态检查 |
| Graph、HTTP API、storage race | PASS：3 个完整包 |
| HA、replication race | PASS：2 个完整包 |
| 工作树卫生、版本契约、diff | PASS：静态检查；不是运行验证 |
| 主工作区原有四个文件 | PASS：逐文件 SHA256 与改动前相同 |

候选生产源码摘要为 `39bd7c7d090f34ad70f06569aa2f70721b751f0cfb6d5b20b4a1ce76e3bec73e`，覆盖按路径排序的 `cmd`/`internal` 非测试 Go 文件及 `go.mod`/`go.sum`（路径、NUL、文件字节、NUL），共 457 个文件。Linux arm64 候选二进制 SHA256 为 `ec7d47f679f4323cf9a1f0fa636321fbd6baaf1030e630b7fe3db1a39d7fe11a`；镜像 `graphdb:product-p1-review6-final-20261003` 为 `sha256:23427095c984d68fb0e996be54e2dfc806fb382478d5180b0cebd19daaf037a0`。构建字段 `dbafd81c-review6` 是基线加本轮改动的候选标识，源码摘要才用于绑定本次测试对象；不能把它当成已提交源码的 Git SHA。

本轮使用缓存依赖、`--network none` 和仓库 Dockerfile 的 Go 构建参数生成二进制，再用相同最终镜像层构建运行对象。标准 Dockerfile 的联网依赖下载阶段本轮 **NOT RUN**；上一轮的证书信任失败没有通过关闭 TLS 校验绕过。

早期运行结果保留，不覆盖为 PASS：首次全仓运行暴露了失败写入重试、来源归一化和 usage 测试边界；首次单机运行暴露已验证克隆发布被误拒绝，以上均已修复。并发验收时一轮 S3 恢复在预定停 Leader 前失去领导权，原因尚未确认；最终单独运行 S3 门禁通过。另一轮部署门禁的单组 12 项通过，但分片目录服务 HTTP 就绪失败，使用偏移 2300 的主机端口落在 macOS 临时端口范围，原因仍未确认；最终单独运行、偏移 1700 的 23 项全部通过。构建完成前误启动的镜像预检没有开始运行场景，不计为产品测试失败。

原始日志和构建身份另归档到本地 `dist/graphdb-product-p0-p1-review2-20261003-evidence.tar.gz`，附 `.sha256`；凭据、测试数据目录和二进制未包含在归档内。

跨宿主机按用户决定保留待验收。真实坏盘、生产容量、长时间负载和新的跨版本滚动矩阵未在本轮运行。修复不能恢复丢失对象，也不会自动纠正旧程序已经发布的错误 head、任务结果或副本分歧。运维处置见[产品可靠性与运维](product-operations.zh-CN.md)及[Raft 故障恢复](raft-operations.zh-CN.md)。

来源身份修复改变了受影响快照的解释。含来源所有者与外部 ID 来源不同、且存在显式 `sources` 的数据组，不得把本候选与旧解释的程序混部运行；先备份和验证原始身份，再按全组维护流程升级并核对完整图。该类数据不在现有不停机滚动升级资格内。本轮没有修复旧程序已持久化的虚假身份或错误合并，不能简单删除所有同名来源身份，需与原始采集记录、备份核对。
