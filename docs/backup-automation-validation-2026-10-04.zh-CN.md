# 单机与 Raft 备份自动化验证

日期：2026-10-04。工作区：`GraphDB-raft-ha`，分支：`codex/raft-ha`。
本轮实现和验证通过；尚未提交、推送、发布或向生产环境安装计划。
机器可读结果见 [JSON](backup-automation-validation-2026-10-04.json)，部署方式见[自动备份指南](backup-automation.zh-CN.md)。

## 实现范围

新增标准库 Python worker，通过现有任务 API 定时创建 S3 逻辑备份，持久化轮次、任务 ID、退避和下一次运行时间。每份备份下载校验；默认首轮和每七天执行隔离恢复演练。失败任务使用原检查点重试；网络异常或含义不明确的 400/404/409 等响应不触发盲目重新提交。

提供一次性/常驻模式、目录进程锁、JSON 日志、Prometheus textfile 指标、非 root 容器、Compose 和 systemd service/timer 示例。六项恢复边界回归加入 CI；GitHub 尚未执行该新增步骤。单机内置自动备份策略继续支持，不修改 Go 后端、Raft 协议和 API。

## 环境与源码绑定

- OrbStack Ubuntu Linux arm64，独立测试容器、网络、S3 桶及前缀，数据和 worker 状态使用 Linux 命名卷。
- 后端复用 `graphdb:availability1-20261004`；基础提交 `5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a`，包括此前未提交的修复。508 份生产 Go 文件逐一核对，聚合 SHA256 为 `c0a4e33983c5730235a9897c65f11b52e5e5620781786a680557293ba310c093`。
- 后端二进制 SHA256：`c35f935bb5ee07ce41429d0933b9abc293ee23fdb555fc8b6da8d01fc1e81d81`。新 worker 镜像中的脚本和 SDK 文件与最终工作区逐一核对。
- Python 3.12 Alpine 非 root UID 10001。MinIO 使用已校验的 Linux arm64 二进制，版本 `RELEASE.2025-09-07T16-13-09Z`，SHA256：`5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d`。
- Raft 使用单组三投票副本、协议 3 和流式快照。测试期间停止当前 Leader，由剩余多数派接管；没有模拟三个独立宿主机。

## 实际结果

| 检查 | 结果与范围 |
| --- | --- |
| Worker 恢复回归，Linux 最终镜像 | PASS，6 项；含丢失响应/受理后的一般 400、409、容量拒绝、原版本重试、损坏校验、等待超时、歧义及任务输入变化 |
| 单机真实 S3 上传中断 | PASS，首次已捕获版本 1，源图继续写到版本 2，重试仍备份版本 1，并完成下载校验与完整演练 |
| 单机进程重新运行 | PASS，持久 `next_run` 未到时延后，不增加备份 POST |
| 常驻运行、重启和互斥 | PASS，同一目录的第二进程返回 busy；常驻进程重启后延续计划，不增加备份 POST |
| 非 root 与文件权限 | PASS，UID 10001，state 目录 0700、JSON 0600、metrics 目录 0755、指标 0644 |
| Raft 丢失 202 后换主 | PASS，认领同一备份任务；源图继续写入后仍备份版本 1，备份 POST 总计一次 |
| Raft 下载校验和完整演练 | PASS，两项演练任务成功，完整恢复版本 1并清理临时目标；原图版本 2 的前后实体保持可读 |
| Raft worker 重新运行 | PASS，已完成轮次未到下次时间时不重新提交 |
| 单机/三副本 Compose 合并配置 | PASS，`config --quiet`；实际服务联测使用专属端口和测试覆盖配置 |
| systemd 示例 | PASS，Linux `systemd-analyze verify`；未安装 timer 或验证其实际触发 |
| 文档链接、差异检查、源码绑定 | PASS；主工作区四份原有用户文件的 SHA256 保持一致 |

首次联测在三个场景通过后，因测试脚本误读导出响应结构失败；改为通过实体 API 检查生产图。第二次在准备桶时遇到 MinIO liveness 已就绪但 S3 写 API 尚未就绪的 503；修正为有期限的桶创建重试。保留这两次失败日志，不将它们记为服务缺陷或成功结果。随后完整阶段通过；最终受理错误边界收紧后再次完整执行，六项运行场景全部通过。

## 验证边界

外部 worker 不删除远端备份。单机内置数量/期限清理保持原功能；外部单机/Raft 计划需要操作者显式配置 S3 生命周期。没有安装或验收真实 AWS 生命周期、IAM、版本控制和 Object Lock。

本轮未运行分片 router 与 worker 的部署组合、跨宿主机故障域、大图/多租户容量矩阵、生产 timer 或 GitHub CI。Python 回归、真实 S3 与单组 Raft 联测不替代这些验收；本轮没有重新运行全仓 Go 测试，也没有量化性能增益。

此前[可用性与滚动升级报告](availability-rolling-validation-2026-10-04.zh-CN.md)中的 Raft 三十分钟负载失败仍然有效。本轮备份自动化通过不改变该发布阻塞状态，也不资格验证新的混部窗口。

## 证据与复现

原始证据目录：`/private/tmp/graphdb-backup-automation-20261004`，包含最终 S3/服务/worker 日志、状态与指标、六项运行结果、镜像源码校验、失败尝试日志和本轮测试驱动。
持久归档：`/Users/livesite/Documents/GraphDB-test-results-2026-10-04/backup-automation-evidence.tar.gz`，另附 SHA256 和逐文件清单。测试临时凭证仅在进程环境中生成，不收入归档。测试专属容器、卷和网络已经清理。

```sh
python3 -m unittest discover -s scripts -p backup_automation_test.py -v
docker build -f deploy/backup-automation/Dockerfile -t graphdb-backup-automation:local .
systemd-analyze verify deploy/systemd/graphdb-backup-automation.service \
  deploy/systemd/graphdb-backup-automation.timer
```

真实 S3 故障联测需要专属桶与容器。归档中的驱动绑定上述镜像和 OrbStack 路径，重新运行前按当前测试环境调整，不作为生产安装脚本使用。
