# GGraphDB 2.3.1 发布验证 / Release validation

## 当前状态 / Current status

**PREPARING**：本记录随发行候选保存，尚未把运行中的门禁记为通过。
标签工作流必须通过完整门禁后才创建 GitHub Release。发布后 main 将追加实际工作流、
下载资产校验和最终结论，保持标签和包内候选记录不变。

This candidate record does not claim publication or passing in-progress checks.
The post-publication main record will identify actual workflow conclusions and
downloaded assets without moving the tag or replacing the packaged source.

## 修复资格 / Repair qualification

OrbStack Go 1.26.7 下已运行 storage/HA 完整包测试及相关 race 回归；相同固定数据和
负载分别验证单机、Raft 的前后对照、冷启动、重启、三个独立副本完整快照及深度审计。
正式 v2.3.0 在独立副本上读取新数据布局一致。
完整方法、数值与剩余限制见[固定负载报告](maintenance-tailfix-2026-10-13.zh-CN.md)。
这些诊断构建的证据不代替确切发行构建的门禁。

## 确切标签门禁 / Exact-tag gates

| 范围 / Scope | 状态 / Status |
| --- | --- |
| 全量 unit / vet / race、版本冻结、SDK、控制台与备份 worker | PENDING |
| 单机 direct/WAL HTTP、恢复、负载与重启 | PENDING |
| MinIO/S3 与真实 TLS 网关 | PENDING |
| Raft 协议 1、2、3、恢复、分片、流式与准备维护 | PENDING |
| 已发布 v2.3.0 → v2.3.1 协议 1 滚动 | PENDING |
| 单机与 Raft 各 30 分钟维护长测 | PENDING |
| 三平台打包、配置与容器启动 | PENDING |
| 发布后下载包、两层 SHA256 和二进制运行 | PENDING |

## 边界 / Boundaries

固定温负载对照中单机 stale 为 3/36，停写后索引 ready；冷启动仍有 22/480 次
5 秒查询超时。旧约 6 秒 Raft 极端查询未在固定基线中复现，其彻底消除尚未证明。
restore-drill 测试改为有上限的终态等待，历史 2 秒偶发超时的底层原因仍未证明。
跨宿主机故障域、日级稳定性、生产容量和统一低延迟 SLO 未认证。

Transient standalone staleness and cold-query timeouts remain observable. The
historical Raft extreme and restore-drill scheduling timeout are not claimed to
have a fully proven root cause. Single-host gates do not certify production fault
domains, day-scale reliability, capacity or a uniform low-latency SLO.
