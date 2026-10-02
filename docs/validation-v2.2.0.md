# GGraphDB 2.2.0 验证范围 / Validation scope

2.2.0 从 `codex/raft-ha` 合并到 `main`，使用同一发行二进制支持默认单机、单组 Raft 和租户分片。
版本、SDK 和 OpenAPI 对齐为 2.2.0；HTTP `/v1`、Go 模块 `/v2` 与单机 2.0/2.1 数据格式保持兼容。

## 本机候选证据

[故障隔离与迁移验收](raft-isolation-validation-2026-10-02.zh-CN.md) 对应提交 `8dc259be` 的候选源码，
包括全量 Go/vet、关键并发回归、单机 direct/WAL、真实分区、磁盘压力、进程中断、36MiB 恢复、完整运行态冷恢复、
协议 1/2/3、分片扩容/迁移/取消和实际不同二进制滚动。
30 分钟维护负载完成 5 次 compact、2 次 GC、2 次索引重建，71,140 次操作、零非预期操作错误。
写入有 90 次预期 429，最长等待 40.154 秒。滚动最终逻辑失败为 0，有 6 次临时重试。

候选镜像基于本机离线模块缓存，不是最终发行资产；历史 FAIL/NOT COMPLETED 均保留，不能推广为通过。
历史单机与性能报告只覆盖各自记录的程序和环境。

首次主分支 CI 的协议 3 分片启动检查失败，记录为 FAIL，见[运行记录](https://github.com/SamuelSupe/graphdb/actions/runs/37024878696)。
网关已通过 router2 提供服务，但直接检查的 router 尚处于选主前失败探测的一秒退避，尚未就绪。
夹具现在分别等待网关和该 router 就绪后进入诊断阶段；未修改服务端、重试写入、不放宽持续负载错误标准。
协议 1/2 和协议 3 的恢复/串行重启在该次已完成，不能据此把整个失败门禁计为通过。修正夹具后，本机使用原隔离候选的完整协议 3 分片门禁重验通过；发行身份仍需远端重验。

该次全量 race 还在大恢复用例的准备写入遇到换主后返回结果不确定的 `503 raft_unavailable`，未报告 DATA RACE。
用例现在为各准备批次保留原幂等身份，仅有界重试明确可重试的 Raft 不可用响应；最终备份版本、恢复代次、三个副本内容和暂存清理断言仍保留。
该恢复用例使用每 100 条日志快照和默认的 30 tick 选举等待；真实部署门禁仍保持每 5 条日志快照，独立覆盖快照故障及默认时序。
修正后在 OrbStack / Go 1.26.7 下执行该恢复用例的 `-race -count=2`，两轮均通过（514.945 秒），未报告数据竞争；全量发行检查仍以修正后的远端运行记录为准。
全量 race 为每个测试包留出 20 分钟预算，CI 静态验收作业为 30 分钟；这是包含大数据恢复等重型场景的执行预算，不是服务请求或选举时限。

[下一次主分支检查](https://github.com/SamuelSupe/graphdb/actions/runs/37028446953) 的 Raft、分片、指标、单机和 S3 门禁均通过，但全量 race 仍记录为 FAIL。
大恢复用例运行 354.42 秒后，在最终副本检查遇到准备数据前创建的共享上下文过期；日志已显示恢复成功、代次为 2、暂存为 0，未报告数据竞争。
夹具现在在准备数据完成后，为备份和恢复分别创建五分钟上下文，并在副本追齐后为最终检查创建独立的一分钟上下文；每批写入与恢复推进仍使用有界上下文，内容一致性断言保留。最终结论仍需新的完整远端检查。

[随后一次完整检查](https://github.com/SamuelSupe/graphdb/actions/runs/37031372528) 的运行验收均通过，全量 race 在两处副本追齐等待超时，仍为 FAIL。
大恢复副本从重启时的索引 20 推进到 57，目标为 58；慢副本恢复后剩三条日志，保留内存约 1.4 MiB。没有报告数据竞争。
这两个用例的 race 重型追齐预算分别为三分钟和 30 秒，前者覆盖超过 32 MiB 的图解码，后者覆盖被故意阻塞的持久化积压；恢复版本、代次、暂存清理及 5 MiB 内存上限断言不变。真实部署、读写错误与持续负载门禁保持原标准，不以这些仪器化测试预算宣称生产 RTO。

[再一次主分支检查](https://github.com/SamuelSupe/graphdb/actions/runs/37034559294) 在普通 HTTP 单元检查的排队拒绝用例准备阶段失败，该次全量 race 未执行。
用例原先用同一个一毫秒排队期限预先占用空闲名额，调度暂停即可在真正的竞争请求到来前失败。准备步骤现在使用请求上下文，随后仅对被阻塞的 HTTP 请求启用一毫秒期限；429、错误码和可重试语义断言保留，服务端准入实现不变。
修正后在 OrbStack / Go 1.26.7 / 两核条件下连续执行该用例的 1000 次 race 检查，全部通过（4.477 秒）。

## 发行提交与二进制资格

[发布工作流](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml) 从 `v2.2.0` 的确切提交执行：

- 全量单元、vet、race、Python SDK 与版本契约；
- 单机 direct/WAL、HTTP 端到端、重启一致性、30 分钟维护负载；
- MinIO/S3 备份、损坏拒绝、重开与新目录恢复；
- 实际容器的 Raft 协议 1、协议 2 恢复、协议 3 及 30 分钟负载、分片与故障隔离；
- 从诊断开发提交 `b22cac8b046a695f14e7d02909d6570f546e989a` 重新构建来源，验证两个实际不同二进制的协议 1 单组/分片滚动、旧程序安装快照、有限回退、严格副本/router 协调；
- 告警规则、实际诊断抓取格式、发行包解压后二进制和 Compose、容器启动。

仅当全部依赖门禁通过时工作流发布 Release。实际运行结论、日志和身份见工作流及包内
`release/evidence/`，不以本文的门禁列表代替 PASS。
`BUILD-METADATA.json` 记录版本、提交、构建时间和 Go 版本；Linux amd64 发行二进制 SHA256
必须与协议 1/2/3 的实际部署验收二进制一致。外层 `.sha256` 校验归档，内层 `SHA256SUMS` 校验各平台二进制。

实际跨版本滚动窗口绑定发行门禁的来源/目标 SHA256 和协议；包内 `raft-gate/rolling/metadata.json` 保存具体身份。诊断开发基线不是已发布的单机 2.1.2，不能据此声称单机到 Raft 可以滚动升级。部署前按 [滚动升级说明](raft-rolling-upgrade.zh-CN.md) 的兼容窗口和阶段要求执行。

## 未验收与边界

- 按用户决定，三台独立宿主机的故障域验收保留 NOT RUN。
- 真实大图/多租户容量、慢盘与网络长尾、天级稳定性、生产认证/TLS 和真实告警通知尚未验收。
- 没有当前发行的 A/A 吞吐校准，不承诺整体吞吐百分比、固定 RTO 或低延迟 SLO。
- 维护期间允许明确的 429；最终发布仍使用共享应用屏障，图解码和单个大对象回滚仍需内存。
- 每个租户属于一个数据组；不支持租户内部图分区、自动按容量/流量均衡、在线完整运行态备份或 PITR。
- 单机升级需重启；Raft 协议 3 生效后，最高协议为 2 的旧程序不能打开同一目录。
- `release/capacity-envelope.yaml` 是 2.1.2 的历史记录，不是本版或 Raft 容量认证。

A stable release tag provides a versioned artifact, not cross-host, production
capacity or low-latency qualification. Candidate results, tag workflow evidence,
and deployment acceptance are separate scopes. Preserve idempotency identities
when retrying uncertain writes and inspect expected backpressure and latency tails.
