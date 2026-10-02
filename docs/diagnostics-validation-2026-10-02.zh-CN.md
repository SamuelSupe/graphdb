# 诊断指标验证记录：2026-10-02

已在 `codex/raft-ha` 补齐单机 direct/WAL、单组 Raft、catalog、数据分片和 router 的诊断指标。指标契约及采集方式见[诊断说明](diagnostics-metrics.zh-CN.md)。本记录是本地候选验证，不代表正式发布或跨宿主机资格。

## 最终候选

- 镜像：`graphdb:raft-diagnostics-final-20261002`，Linux arm64，沿用已验证的 Alpine 3.20 运行时并替换离线构建的二进制及 OpenAPI。
- 镜像 ID：`sha256:5c147eea59d07ec040eabbadce1743af0683ef1df6829def23e647511c09c1ad`。
- 二进制 SHA256：`d6bff204faa791f06c903780c3924b36031a34c93989b13127d98aba514c17a3`。
- 生产 Go 源码清单 SHA256：`dff1a961856ee128369c4b5e758f981a4578a13dfa874291acc80a6e487fe29c`，清单包含 `cmd`、`internal` 非测试 Go 文件、`go.mod` 和 `go.sum`。
- 二进制标识：`2.1.2-raft-diagnostics1`、`cffd8e86-diagnostics-dirty`、Go 1.26.7。内嵌提交标识表示基于 `cffd8e86` 的未提交构建，精确身份以源码清单和二进制摘要为准。
- 最终证据目录：`/tmp/graphdb-diagnostics-20261002`；`release-*` 为最终结果，早期尝试另行保留。

## 实际验证

| 范围 | 结果 | 证据与边界 |
| --- | --- | --- |
| 全量 Go 测试 | PASS | OrbStack / Go 1.26.7，`go test ./... -count=1`；`release-tests.log`，包含单机存储/WAL、Raft、分片、HTTP、CLI 和 Go SDK |
| Go vet | PASS | `release-vet.log`；静态检查 |
| 关键路径 race | PASS | 准入排队/取消、WAL HTTP 观测、Leader 缓存、router drain/鉴权、本地无 quorum 诊断、复制/快照/迁移；`qualified-race.log`。其后的修改仅保留原数据磁盘问题码，并通过最终全量测试和容器门禁 |
| 协议 1 / 快照格式 1 | PASS，22 项 | `release-compat`；同一镜像同时运行单机 direct/WAL 与 Raft，含故障、恢复及分片迁移 |
| 协议 2 / 流式快照 | PASS，22 项 | `release-enhanced`；同一场景启用协议 2 和流式快照 |
| 实际指标格式 | PASS，36 份 | 两种模式的 HTTP `/metrics` 响应，`promtool check metrics`；`release-prom-all.log`。校验 HELP、类型、名称及文本格式，采集助手同时拒绝重复样本和非有限数值 |
| 告警与采集配置 | PASS | `promtool check rules` 共 18 条；采集配置 `check config --syntax-only`。没有实际部署 Prometheus/Alertmanager 或真实通知通道 |
| OpenAPI / CI 配置 | PASS，静态 | YAML 解析及 Python 门禁语法；CI 增加实际采集 payload 的 Prometheus 校验，未执行远端 CI |

实际场景包括：

1. 单机 direct、单机 WAL 与三副本 Raft 同时运行，分别采集真实数据、WAL 和 Raft 目录的文件系统状态；保留独立租户数据、写入及 SIGKILL 恢复行为。
2. 孤立原 Leader 的 Raft 网络，其强读/写入被拒绝，多数派继续提交；孤立进程的指标与 JSON 诊断仍返回 200。回归还在应用锁被占用、请求 context 已超时的条件下检查诊断不等待应用屏障，并保护 catalog 的入口。
3. 36MiB 逻辑备份恢复遇到 Leader SIGKILL 后继续完成，检查真实快照构建与维护准备计数。使用可压缩测试数据，不能把此场景当作大图性能资格。
4. catalog、每个数据副本及 router 均被直接采集。暂停迁移目标后，catalog 的 `move_errors=1` 和 `moves{phase="copy"}=1`；迁移完成后错误和 copy 数量清零。
5. router drain 后暂停整个 catalog 组，router 自身诊断仍可读；错误 token 和 POST 请求分别拒绝为 401/405。router 诊断不主动探测集群，不宣称集群健康。
6. 接入队列与 catalog 观测使用缓存/原子发布；已提交状态更新后发布，恢复及失败时清除缓存，known=0 明确表示未知。逐副本进度来自本地 Raft engine，日志复制位置与远端图应用位置分别观察。

## 验收中修正

原有磁盘及 Raft 基础指标缺少 HELP，被 Prometheus lint 拒绝；补齐说明后最终 36 份响应均通过。耗时桶覆盖 1ms 至 1800s，避免长快照/维护只落入 30s 以上的无穷桶。

早期容器门禁有两处环境/脚本假设失败：网关不一定把已有请求发送到被采集的那个 router，成员重启后网关切换也可能未完成。门禁改为明确向目标 router 发只读请求，并向当前 Leader 准备恢复测试租户；维护计数在实际执行进程上、重启前检查。没有加入写请求盲目重放，也没有放宽故障与最终数据断言。早期失败输出保留，不能替代最终两种模式的 PASS 结果。

## 未取得资格

- 跨宿主机、独立磁盘与故障域仍为 **NOT RUN**，按用户决定保留待验收。本地多容器不代表三台独立主机。
- 本轮未重跑生产容量/A-A 吞吐校准、长时间 soak、真实旧/新二进制滚动及冷备灾备门禁；没有声称指标开销为零、吞吐提升或正式版本可立即发布。
- 未在真实监控平台验证 dashboard、告警发送、OS 指标采集、身份服务/TLS 和私有链路加密；示例规则和配置检查不能替代这些环境验证。
- 未重跑 macOS 原生文件系统回归；本轮后端运行验证使用 OrbStack Linux。之前产品完善的验证记录仍保留，不能冒充本轮新二进制结果。
- 未推送、打发布标签或上传发布资产；仅本地实现、验证和提交。

最终采集文件、日志和源码清单归档在 `dist/graphdb-diagnostics-20261002-evidence.tar.gz`，附 SHA256 校验文件。`dist` 为本地忽略目录。
