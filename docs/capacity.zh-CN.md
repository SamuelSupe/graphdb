# 容量边界与推荐拓扑

[English](capacity.md)

GGraphDB 1.2 发布的是可复现的发行容量边界，而不是没有条件的“大图”
承诺。机器可读契约位于 `release/capacity-envelope.yaml`。

## 1.2 发行门禁

主写入吞吐门禁在固定 8 CPU/8 GiB OrbStack 容器中运行一个 local writer，
使用 sync WAL 与 segment metadata。8 个租户、16 个采集器持续 30 分钟，
v1.1.5 与 v1.2.0 各跑 5 次。每次候选运行都必须达到至少 10,000 committed
mutations/s；候选中位数至少为基线 1.5 倍，运行间离散不超过 5%。accepted
p95/p99 不超过 20/250 ms，committed p95/p99 不超过 8/15 秒，RSS 不超过
7 GiB 和基线 110%，每 1,000 mutation 的 CPU 不超过基线 75%，direct 写入
和查询回归不超过 10%。
RSS 从 writer 进程的 `VmRSS` 采样；CPU 读取 writer 容器 cgroup，并按每
1,000 个 committed mutation 归一化。

完整的提交绑定矩阵通过以下命令运行：

```sh
scripts/wal_performance_matrix.sh
```

GitHub release 依赖带 `orbstack` 标签的 self-hosted runner；任一候选运行或
相对门禁失败都不会发布。

### 本地 WAL 运行时默认值

以下是 v1.2.0 本地 writer 的默认值，与固定主机容量 profile 及其有意更严格的
测试阈值分开：

| 设置 | 默认值 |
| --- | ---: |
| WAL 耐久性 | `sync` |
| 最老 pending 上限 | `2m` |
| Graph flush | `250ms`，flush trigger 为 8 请求 / 2 MiB（忙租户可合并同一轮队列），2 个 worker |
| Metadata flush | `500ms`，trigger 为 256 请求 / 8 MiB，2 个 worker |
| Write cache | `4GiB` |
| Commit-tail 上限 | `20,000` |
| 后台重型任务执行 | 1 个并发任务 |
| Maintenance ingest 空闲窗口 | 每租户 `1m` |

PostgreSQL direct 路径继续作为正确性和回归门禁：

每个候选版本必须在 CI 中满足：

- generic S3/RustFS 上的 8 writer 并发正确性门禁；
- 单租户由 2 个活跃 writer 以每秒 20 个成功 commit 持续 30 分钟；
- 节流与重试后吞吐不低于目标的 90%；
- 丢失 commit、重复 graph version、终态写入错误均为 0；
- 最终实体数和 graph version 与计划 commit 数一致；
- 每个服务端请求仍遵守公开的 8 次 CAS 重放上限；压测客户端对返回的
  `write_conflict` 使用最长 2 秒的封顶抖动退避，跨 writer 实例轮转重试，
  最多重提 64 次；
- commit tail 达到 1,000 条时执行在线 compact，且不得丢失并发 commit 或
  降低 graph version；
- 压测期间运行 legacy mirror 与派生索引 worker，结束后 mirror lag 和所有
  pending backlog 必须归零；
- 指定 tag 的真实 1.0 二进制必须能读取最终镜像 graph version。

这是并发与持久性边界，不是最大图规模。200 个 commit 的短测只算 smoke，
不能作为发行认证。
运行 `scripts/postgres_cas_gate.sh soak` 时设置
`GRAPHDB_TEST_CAS_STRESS_REPORT=/path/report.json`，可保存机器可读的发行证据。
CI 会把该报告绑定到被测 commit，并随 release archive 一同打包。

### 历史吞吐基线

2026-07-23 曾在本机 OrbStack PostgreSQL 与 RustFS 环境中完成一次 30 分钟
数据路径压测：

| 指标 | 结果 |
| --- | ---: |
| 活跃 writer / 持续时间 | 2 / 30 分钟 |
| 目标提交 / 实际提交 | 36,000 / 36,000 |
| 实际吞吐 | 19.96 commits/s（目标的 99.78%） |
| 在线 compact / 可恢复 CAS 冲突 | 36 / 377 |
| 最终 graph version / head revision | 36,000 / 36,036 |
| 最终实体 / snapshot / commit tail | 36,000 / 36,000 / 0 |

完整报告位于
[`release/evidence/cas-stress-orbstack-2026-07-23.json`](../release/evidence/cas-stress-orbstack-2026-07-23.json)。
该 schema 1 报告早于当前完整门禁新增的 mirror/派生 backlog、指定 tag 的
真实 1.0 二进制读取和被测 commit 绑定检查，因此它只能作为持续吞吐基线，
不能认证当前发行版本，也不是硬件性能保证。每个候选版本都必须由 CI 生成
新的 schema 2 报告，并在目标部署环境重跑相同门禁。

## 可复现基线

在 OrbStack RustFS writer/reader 栈上执行：

```sh
CAPACITY_PROFILE=smoke scripts/capacity_baseline.sh
CAPACITY_PROFILE=baseline scripts/capacity_baseline.sh
```

`smoke` 执行小规模读写检查；`baseline` 包含：

| 场景 | Writer | Reader | Batch 数 | 每批逻辑组 | 计划图规模 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mixed-10k` | 4 | 8 | 50 | 200 | 20,002 实体 / 10,001 边 |
| `write-25k` | 8 | 0 | 50 | 500 | 50,002 实体 / 25,001 边 |

报告写入 `capacity-runs/<timestamp>/`，包括配置、计划图规模、HTTP 状态、
错误数和 p50/p95/p99/max 延迟。该目录默认不进入 Git；发布时应把报告
作为发行证据或上传性能平台。

## 推荐拓扑

| 场景 | 协调方式 | Writer | Reader | 说明 |
| --- | --- | ---: | ---: | --- |
| 开发/评估 | 本地文件 | 1 | 0 | `GRAPHDB_MODE=all`，无 HA |
| 小型生产 | local + generic/native 对象存储 | 1 | 2+ | writer lease，reader 独立扩展 |
| 并发生产 | PostgreSQL + generic S3 | 2–8 | 2+ | 已发布容量边界中每个热点租户使用 2 个活跃竞争者；PG head 权威 |

建议从 writer 4 vCPU/8 GiB、reader 2 vCPU/4 GiB 和满足写入速率的 HA
PostgreSQL 开始验证；这些是验证起点，不是支持保证。查询并发通过 reader
扩容。部署到 8 个 writer 可提供放置和故障转移能力，但让一个热点租户由
8 路持续争抢并不是线性扩吞吐方案。

## 容量规则

- writer 会物化当前租户图，并以 copy-on-write 应用 mutation。内存要按
  目标图、字段宽度和 commit batch 实测，同时为一次 mutation 与 compact
  保留余量。
- 查询 `limit` 上限为 1,000；批量导出应使用 scan/stream。
- 采集 batch 建议保持 200–500 个逻辑组；过小 batch 会放大 commit、
  manifest、幂等记录和 collector state。
- PostgreSQL direct 模式支持部署 2–8 个 writer；已认证的热点租户容量为 2 个活跃竞争者、
  约 20 commit/s。8 路同租户并发仍是正确性门禁，不是持续吞吐承诺。
  更高吞吐需要图/实体分区并重新发布容量边界。
- 发布的 20 commit/s 配置保持自动 compact 开启，compact 阈值为 1,000，
  写入背压阈值为 1,500，维护循环间隔不超过 30 秒。
- 对象存储延迟、PostgreSQL 延迟、字段宽度、索引数量和关系密度都会显著
  影响容量，必须在生产同地域、等价数据上重新跑基线。

生产容量不能只看实体数。至少记录图规模、平均字段字节、关系密度、活跃
索引、commit rate、查询混合、p95/p99、内存高水位、对象数量/字节和 PG
CAS 冲突率。
