# GGraphDB v2.3.7

## 中文

本补丁修复 v2.3.6 审查中复现的 WAL 推进、查询结果、索引追平、快照重试和租户作用域问题。
[版本验证记录](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.7.md)
区分源码回归、确切标签门禁及独立下载资产核验；发布流水线仅在全部必需门禁成功后发布。

- WAL 按条数／字节分组，过大的未准备组拆分重试；捕获生成号超时生效，JSON 解码先获取准入。
- 延迟索引追平按租户估算预留内存，临时失败保留并重试；旧 rebuild 不覆盖新 catalog。
- 修复拆分分片等值漏行、大小写／空串范围裁剪、负零、非标量字段扫描及旧 catalog 分页。
- 时间谓词支持 RFC3339/Nano、UTC 日期和整数 Unix 毫秒；非法字面值返回错误。
- 快照磁盘压力采用可恢复状态，保持 Raft 运行；Drain 节点重新当选时自动移交健康副本。
- 带租户头的生命周期和恢复操作校验目标及备份来源。平台跨租户迁移保留在无租户头的私有接口；
  SDK 生命周期方法不附加隐式默认租户。自动隔离恢复演练继续支持租户作用域。
- Raft 离线恢复须确认 `--isolated-cluster-recovery`。参数不是网络隔离机制，禁止把旧身份接回在线集群。
- 默认 Compose 仅绑定回环；未知存储错误返回脱敏 500；无效／不存在的 HTTP 租户不进入健康监控。

完整 WAL 帧 CRC 损坏仍拒绝启动，不能把可能已确认的数据自动截掉。已有合法跨租户迁移不被删除。
HTTP `/v1`、Go 模块 `/v2`、WAL/Parquet 布局及 Raft 协议编号不变；缺少类型信息的冷 catalog
必要时回退实体扫描。升级前备份，停止旧单机进程后复用目录；Raft 遵循滚动升级手册。
Raft 混合版本窗口暂停租户生命周期与恢复操作，先完成或取消已有任务，全部副本升级后恢复。
旧版已持久化为失败的 batch 不会被本版自动改写成功；需核对源数据并用新 batch ID 重放。
本版不承诺消除秒级长尾、持续索引滞后或提供生产容量资格。

## English

This correctness patch bounds WAL coalescing, retries oversized unprepared groups,
uses tenant-sized deferred index reservations, retains failed catch-up work, and
prevents catalog rollback. It fixes split-shard equality, unsafe range pruning,
signed-zero lookup, non-scalar scan parity and legacy list cursors. Timestamp
predicates accept RFC3339/Nano, UTC dates and integer Unix milliseconds; invalid
literals return a query error.

Transient snapshot preparation pressure keeps Raft running and retries without
restart. A drained node elected again hands leadership to a healthy peer. Scoped
lifecycle/restore requests check their tenants; private platform migration remains
available without a tenant header. Go/Python lifecycle SDK methods omit the
implicit default tenant. Raft runtime restore requires explicit isolated-cluster
acknowledgement, not a substitute for actual network isolation.

All mandatory exact-tag gates, including both thirty-minute soaks, must pass
before automated publication. See the linked validation record for actual results
and independent downloaded-asset verification. Complete-frame WAL corruption
remains fail-closed. Old terminal failed batches require source reconciliation and
a new batch ID; this patch does not rewrite historical outcomes. Existing data
formats and protocol numbers are retained; cold catalogs without type information
use conservative entity-scan fallback. No production latency/capacity
or cross-host reliability claim is made.
Finish or cancel lifecycle/restore tasks before a Raft rolling upgrade and pause
these management operations until all replicas are upgraded. Historical committed
commands keep their original authorization semantics during replay.

## Verified publication / 发布核验

已作为稳定 Latest 发布。完整标签流水线全部通过，包含单机与 Raft 各 30 分钟长测。
独立下载后双层 SHA256、构建提交及包内源码一致；Linux arm64（OrbStack）与 macOS arm64
真实写入、重启、幂等重放及新目录恢复通过。Linux amd64 由原生 GitHub 门禁验证。
单机仍有 55/59 次 stale 采样；长测通过不表示消除索引滞后或秒级长尾。
完整逐项处理和证据见上方版本验证记录，最终记录位于 main，标签及二进制保持不变。

Published as stable Latest after all mandatory exact-tag gates passed, including both thirty-minute soaks.
Independent downloads passed SHA256, source/build identity and native Linux/macOS runtime recovery checks.
Transient index staleness and seconds-long tails remain; detailed results and limits are in the validation record.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.7.tar.gz.sha256
tar -xzf graphdb-v2.3.7.tar.gz
cd v2.3.7
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
```
