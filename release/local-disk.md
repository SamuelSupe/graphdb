# GGraphDB v2.2.0

## 中文

同一二进制现在支持默认单机 direct/WAL、独立磁盘的 Raft 副本，以及按租户拆分的多个 Raft 数据组。
单组默认三个完整副本，多数派持久化并提供强一致读取；分片目录保存稳定归属，新增组不会自动搬迁已有租户。
通过显式迁移增加容量，每个租户仍由一个数据组完整管理，不支持租户内部图分区。

本版包含受保护的集群管理、兼容窗口内的滚动升级、租户迁移/取消、分块续传恢复、可选流式快照、
维护公平推进和租户写入背压、协议 3 的 GC 准备/发布分离，以及本地诊断、Prometheus 指标和告警示例。
同时修复副本配置分歧、成员变更竞态、过期任务/WAL 发布、导入损坏和维护期间误摘流等问题。

单机 2.0/2.1 目录保持兼容，替换程序前必须停止旧进程。单机升级仍需重启；Raft 滚动升级只限于
[已验收窗口](https://github.com/SamuelSupe/graphdb/blob/v2.2.0/docs/raft-rolling-upgrade.zh-CN.md)，不能推广到任意版本。
默认 Raft 协议仍为 1；协议 2/3 必须在全部投票节点具备支持后独立启用。协议 3 生效后，最高协议为 2 的旧程序不能打开原副本目录。
保留原 WAL 受理/终态区分、幂等键、租户代次和多数派/应用屏障；超时可能已经提交，重试须使用原身份。

SDK/OpenAPI 版本为 2.2.0，Go 模块仍为 `/v2`，HTTP 仍为 `/v1`。单机自动 S3 备份继续支持；Raft 使用外部调度调用集群备份 API。
业务认证、租户授权与 TLS 由网关负责，私有 Raft/目录/router 管理令牌不代替用户授权。完整运行态灾备仍要求一致离线边界。

候选在 OrbStack 通过单机、协议 1/2/3、分片、故障恢复与实际混部滚动验证。
30 分钟 Raft 负载记录 71,140 次操作、零非预期错误，同时有 90 次预期写入 429，最大写入等待 40.154 秒。
这些数据属于本机候选正确性观测，不是发行资产性能基线。
跨宿主机、真实容量、天级稳定性、生产认证/TLS 与真实告警通知仍待验收；不承诺统一吞吐增幅、固定 RTO 或低延迟 SLO。

发行标签工作流另行执行完整测试、vet/race、SDK、HTTP/重启、S3 恢复、单机及 Raft 30 分钟负载，并从该提交构建和核验发行包。
实际结论以 [GitHub Actions](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml)、包内 `release/evidence/` 和
[2.2.0 验证范围](https://github.com/SamuelSupe/graphdb/blob/v2.2.0/docs/validation-v2.2.0.md) 为准；历史候选证据不代替发行二进制资格。

## English

The same binary supports default standalone direct/WAL, independent local Raft
replicas, and tenant sharding across data groups. A group defaults to three full
replicas with majority durability and strong reads. The catalog keeps placement
stable; adding a group does not automatically move existing tenants. Migration
moves whole tenants, and intra-tenant graph partitioning is not supported.

This release adds protected cluster administration, qualified rolling upgrades,
resumable migration/recovery, optional streaming snapshots, fair maintenance,
tenant write backpressure, protocol-3 prepared GC, and local diagnostic metrics.
It fixes replica configuration divergence, membership races, obsolete background
work, corrupt imports, and maintenance-induced gateway withdrawal.

Standalone 2.0/2.1 directories remain compatible after stopping the old process.
Raft rolling compatibility is limited to the documented source/target/protocol
window. The default protocol remains 1; separately activate 2/3 after all voters
support them. A protocol-2-only binary cannot reopen a directory after protocol 3
has been persisted. Keep idempotency identities when retrying uncertain writes.
SDK/OpenAPI versions are 2.2.0; HTTP `/v1` and the Go module `/v2` remain unchanged.
Standalone automatic S3 backups remain available; Raft uses external scheduling.

The local candidate passed standalone, protocol 1/2/3, sharding, recovery and
actual dual-binary rolling checks. Its thirty-minute Raft workload recorded
71,140 operations without unexpected errors, but included 90 expected ingestion
429s and a 40.154-second maximum write wait. Cross-host, capacity, day-scale
stability and production integration remain unqualified. No general throughput,
low-latency or fixed recovery-time guarantee is claimed.

The tag workflow independently qualifies and packages the release commit.
Consult its actual conclusions and packaged `release/evidence/`; local candidate
results do not certify a different binary. See the
[upgrade guide](https://github.com/SamuelSupe/graphdb/blob/v2.2.0/docs/user/release-deployment.md).

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.2.0.tar.gz.sha256
tar -xzf graphdb-v2.2.0.tar.gz
cd v2.2.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, source, SDKs, OpenAPI,
bilingual documentation, standalone/Raft/sharded deployment examples, serial
upgrade scripts, alert rules, build identity and gate evidence. No registry image
or production fault-domain qualification is implied by the download archive.
