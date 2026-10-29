# GGraphDB v2.4.3

## 中文

2.4.3 修复单机长期写入后的 inode 压力、租户管理元数据缺失与恢复、创建排队和迁移覆盖风险，并包含 2.4.2 之后的读排空及转发修复。发行二进制使用 **Go 1.27.3**。

- **inode 保护与清理**：数据、独立 WAL 和 Raft 目录同时检查字节及 inode 余量，暴露诊断与 `disk_inodes_low` 背压；删除和 GC 回收空父目录，独占目录下的启动清理安全处理遗留空目录和暂存文件。
- **默认在线恢复管理元数据**：单机全部租户自动恢复，无需配置或名单。有效 pending/副本保留原状态和字段；没有有效副本时，仅在 manifest 有效且无生效中的 purge 标记时按 active 重建。生效、未知或损坏的 purge 状态不允许自动复活；Raft 读取不做本地修复写入。
- **修复缺失来源**：首次直接写入、批量摄取及 `init-tenant` 在发布 manifest 前持久化管理记录。迁移输入缺失、损坏或租户身份不符时，在整图解码及目标替换前拒绝，保留目标数据；离线迁移仍需要暂存复制 IO。
- **降低恢复与创建成本**：只补管理状态的创建不再遍历或链接整棵图目录；已完成暂存清理移出请求和租户锁。后台修复有界、可取消；只修副本时保留主文件及 inode，不另建 pending；健康副本不重复解码。
- **WAL 与诊断**：资源故障后的重试兼容旧版已持久化的字段别名冲突结果，继续校验提交身份、版本、图摘要和其他结果；HTTP 499 在调用链中标记为取消错误。
- **包含已合入的相邻修复**：读排空时先注销中断回调再释放屏障；转发不隐式重放结果未知的写入；缓存路由器仍能查询迁移状态。

### 升级与恢复

inode 已耗尽时先停止新写入、保留完整数据/WAL 和离线备份并恢复资源余量，再用新版本重放。核对 readiness、pending 和幂等批次结果后恢复流量，不能删除 WAL 或绕过 PREPARED 校验强行启动。

新增元数据恢复控制文件不改变图/WAL 编码；有 pending 时应先由新版本完成恢复，再考虑降级。2.4.2 引入的双槽 WAL durable marker 降级边界继续适用：旧程序不能读取新 marker，备份包含整个 WAL 目录。HTTP `/v1`、Go 模块 `/v2`、逻辑 `data_hash`、Raft 协议及全局 `ListEdges` 游标契约保持不变。范围页、邻接页继续默认关闭；全部相关节点升级后才能显式启用。

原门禁保持，包括完整 unit/vet/race/SDK、direct/WAL/恢复、S3/网关、真实 v2.3.0 滚动、增强/协议 3，以及单机和 Raft 各 30 分钟长测。发布后独立下载验证另行记录；既有标签和资产不变。不承诺生产容量、跨宿主机或天级稳定性。

[验证记录](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.4.3.md) · [运维说明](https://github.com/SamuelSupe/graphdb/blob/main/docs/product-operations.zh-CN.md) · [升级手册](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md)

## English

2.4.3 addresses inode pressure, missing tenant management metadata, slow tenant creation and unsafe migration inputs. It includes the post-2.4.2 read-drain and forwarding fixes. Binaries use **Go 1.27.3**.

- Check both free bytes and inodes on data/WAL/Raft filesystems; expose diagnostics and backpressure. Reclaim empty object directories and abandoned staging files under exclusive ownership.
- Automatically recover standalone metadata for every tenant without configuration. Valid pending/backup records retain state and fields. Only a valid manifest with no active purge fence permits an active fallback when no valid copy remains. Unknown/corrupt purge state blocks revival; Raft reads never perform independent local repair writes.
- Persist metadata before the first manifest in direct commit, batch ingest and tenant initialization. Reject absent/corrupt/mismatched migration metadata before graph decoding and target replacement. Staging-copy IO remains necessary.
- Avoid whole-tree work for metadata-only creation. Bound background repair and move completed staging cleanup off request locks. Repair only the secondary copy when the primary is valid, retaining its inode and avoiding another pending allocation; skip redundant decoding.
- Preserve legacy prepared ingest alias results across resource-failure retries without weakening identity, version, graph-hash or result checks. Mark HTTP 499 spans as canceled errors.
- Retain completed read responses during drain, prevent implicit replay of mutations after a lost forwarding response, and report migration status through cached routers.

**Upgrade boundary:** recover free resources before replay on an exhausted filesystem; preserve complete data/WAL backups and verify readiness and accepted batch identities before resuming traffic. Finish metadata pending recovery with the new binary before downgrade. The two-slot WAL-marker restriction introduced in 2.4.2 remains. HTTP/module namespaces, logical hashes, global edge cursors and Raft protocol identifiers are unchanged. Range/adjacency flags remain off until all participants are upgraded.

All existing gates, including both thirty-minute soaks, remain required. [Validation](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.4.3.md) distinguishes local checks, exact-tag qualification and independent downloads. These checks do not establish production capacity or cross-host/day-scale stability.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.4.3.tar.gz.sha256
tar -xzf graphdb-v2.4.3.tar.gz
cd v2.4.3
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
```
