# GGraphDB v2.4.4

多租户属性图数据库，支持单机、Raft 高可用和按租户分片，内置中英文管理控制台。
A multi-tenant property graph database with standalone, Raft and tenant-sharded deployments, plus an embedded management console.

## 本版更新

- **身份匹配正确性**：修复 CI 组合字段及 source/external_id 分隔符碰撞导致的误合并。新提交记录匹配规则，旧提交保留原重放语义。
- **WAL 持续写入**：无效标识在接收前拒绝；历史尚未准备发布的无效批次会持久化为失败，并放行后续合法批次。采集状态可跨重启查询。
- **重复数组写入**：`append_unique` 使用值索引去重，保留顺序与原有等价规则。同机三次 HTTP 对照中，16,000 项字符串数组重复写入中位数由约 4.25 秒降到 7 毫秒；这是局部回归结果，不是生产吞吐或延迟保证。
- **查询响应**：避免平均值累加溢出；求和超出浮点范围时返回明确错误，JSON 编码失败不再返回 HTTP 200 空响应。
- **升级性能**：首次升级后的无变化写入保留新索引，且不更改实体时间、版本或来源归属。

延续 2.4.3 的 inode 保护和租户元数据在线自动恢复；单机无需新增配置。

## Upgrade notes

New writes use unambiguous identity tuples and persist their matching rules. Old commits retain their original replay semantics; already merged entities cannot be automatically separated. Keep a verified pre-upgrade backup: do not downgrade a directory after new-format writes.

Raft still defaults to protocol 1. Protocols 1–3 retain historical write semantics. Upgrade every voter and router before separately activating `GRAPHDB_RAFT_PROTOCOL_VERSION=4`; the persisted protocol floor prevents unsafe downgrade. Protocol 4 also retires invalid legacy accepted batches through replicated state.

Ingest identifiers must fit their escaped path components: `source` up to 255 bytes; `collector_id`, `batch_id` and `idempotency_key` up to 247 bytes; NUL is rejected. Already prepared publication groups retain their integrity checks and are never silently discarded.

## Download and run

The archive includes **Linux AMD64, Linux ARM64 and macOS ARM64** binaries built with **Go 1.27.3**, source, SDKs, deployment examples and gate evidence.

```sh
sha256sum -c graphdb-v2.4.4.tar.gz.sha256
tar -xzf graphdb-v2.4.4.tar.gz
cd v2.4.4
sha256sum -c SHA256SUMS
GRAPHDB_DATA_DIR=./data bin/graphdb-linux-amd64 serve
```

Choose the binary for your platform, then open `http://127.0.0.1:8080/ui/`.

## Validation

Publication requires the existing unit/vet/race/SDK, direct/WAL recovery, S3 backup, gateway, rolling upgrade, and both thirty-minute soak gates, plus protocol-4 process/recovery checks. No gate waiver. Packaged evidence and the final verification record distinguish candidate, publication and independent-download results. Cross-host recovery, day-scale stability and production capacity remain unqualified.

[README](https://github.com/SamuelSupe/graphdb/blob/main/README.md) · [中文 README](https://github.com/SamuelSupe/graphdb/blob/main/README.zh-CN.md) · [Validation](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.4.4.md) · [Upgrade guide](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md)
