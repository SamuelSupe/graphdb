# 2.0 架构精简与验证记录

日期：2026-09-28。基线：`7171330caa38ec2c953cc11c3f7cae0beec691fb`。
本文件记录第一阶段结果，后续实现与验证见[架构收尾和备份自动化验证](architecture-backup-automation-validation.md)。
候选版本为工作区改动，尚未提交、推送或发布。

## 实现结果

- 索引重建接入普通任务执行器，统一排队、取消、重试、终态持久化与恢复检查。删除旧索引执行器、独立启动锁和状态表；保留旧记录读取兼容。
- 本地任务直接使用进程内存活注册表，取消不再轮询磁盘，GC 不再写心跳。写入准入直接检查注册表，不扫描历史索引任务。
- 本地目录独占提供进程存活边界，writer token/epoch 保留租户代次隔离。移除本地续租需求，以文件元数据验证已解码代次记录。
- 已持久化图在存储层统一发布；Direct、WAL、恢复和 compact 共用，HTTP 不再额外同步读缓存。保留不可变图、读视图及失效保护。
- 普通/流式查询共用存储层索引选择、版本约束和请求内元数据复用。失败退避使用有界表，去掉逐项定时器。
- 维护调度从 HTTP 移至 `internal/maintenance`，继续复用现有预算、用量缓存和审计。删除无生产调用者的单写入者存储包装器及转发函数。
- 更新中英文架构、任务维护文档及英文入口链接。未新增依赖。计入新文件后，生产 Go 代码净减少 **284 行**；该数字不含测试、文档和已有无关文件。

架构及不变量见[中文说明](architecture.md)和[英文说明](architecture.en.md)。

## 验证环境与结果

OrbStack Docker，`golang:1.26.7-bookworm`，Linux arm64，4 CPU、6 GiB。
后端验证在容器中的源码快照运行；性能基准的临时数据位于 Linux 命名卷。

| 验证 | 结果 |
| --- | --- |
| `go test -mod=readonly ./... -count=1 -timeout=5m` | PASS，涵盖存储、HTTP、图/查询、Go SDK、工具及文档测试 |
| `go vet -mod=readonly ./...` | PASS；收尾改动后再次通过 |
| 任务、索引、代次、读视图和 HTTP 写入的 race 检查 | PASS；收尾涉及的本地恢复、查询和准入路径再次通过 |
| 收尾相关测试：Backpressure / Index / LocalTasksRecover / Query / Read / Cache | PASS |
| 现有 release gate 的 direct / WAL HTTP 阶段 | PASS |
| Python SDK | PASS，12 项测试，连接真实 HTTP 服务 |
| 4 写入客户端 + 16 查询客户端的短负载 | PASS，direct / WAL 均无请求错误 |
| 导出、备份、覆盖恢复及恢复后的查询 | PASS，沿用现有 HTTP e2e 流程 |
| direct / WAL 正常退出并重新打开数据目录 | PASS，重启前后导出 JSON 相同 |
| 格式与差异检查 | PASS，修改后的 Go 文件已 gofmt，`git diff --check` 无错误 |

补充的现实回归场景包括：索引任务通过普通 Task API 取消并重试；旧任务进度不能覆盖替代任务；
固定 instance ID 重启后立即识别中断任务；租户清空重建后旧任务不能写入新代次；
无需 HTTP/WAL 回调即可发布最新读缓存，旧图仍不可变。

全量测试在核心重构及任务锁收拢后执行；之后移除 HTTP 转发函数、跳过本地旧任务标记读取、
加强同 instance ID 重启用例，均通过上述收尾测试、race、vet 和最新候选的 HTTP 验证。

HTTP 命令沿用仓库脚本：

```sh
RELEASE_GATE_SKIP_STATIC=1 \
GRAPHDB_GATE_OUTPUT=/tmp/architecture-http-cold \
bash scripts/release_gate.sh
```

静态/Go 检查已单独执行。脚本默认用 1 字节读图缓存强制验证磁盘索引路径。
首次误用 512 MiB 缓存时，e2e 在 `indexed_read=true` 路径断言处失败，响应走了热图路径；
恢复脚本默认配置后完整通过。未修改或放宽该断言。热图发布由本地缓存与 HTTP 专项测试覆盖。

### 工作区边界

原有未跟踪文件保持原样，未纳入验证源码快照：

- `docs/high-availability-design.zh-CN.md`
- `docs/performance-v1.3.3 2.md`
- `internal/graph/read_range 2.go`
- `internal/httpapi/scan_graph 2.go`

后两个是原有重复 Go 副本，会干扰直接在当前工作区执行全量编译。此次使用包含所有本次新增/修改
文件的源码快照验证，未删除或改写这些副本。快照生成脚本及原始证据位于
`.workflow/architecture-simplification/`：`full.log`、`race.log`、`focused.log`、`latest-race.log`、
`vet.log`、`http/` 和两个 `bench-*.log`。

## 单次性能诊断

同一容器内先基线、后候选，复用现有 `BenchmarkLocalIndexedConcurrentCommit10K`，各一次、10 轮。
种子为 1 万实体，每轮 4 个并发提交，每提交更新 20 个实体；种子和索引初始化不计入计时。
执行基准时没有并行运行本任务的其他测试，但宿主机不独占，未做操作系统页缓存清空或 A/A 校准。

```sh
TMPDIR=/cache/architecture-bench-tmp go test -mod=readonly ./internal/storage \
  -run '^$' -bench '^BenchmarkLocalIndexedConcurrentCommit10K$' \
  -benchtime=10x -count=1 -benchmem
```

| 每轮指标 | 基线 | 候选 |
| --- | ---: | ---: |
| 时间 | 549.883 ms | 547.612 ms |
| 分配字节 | 297,743,747 | 297,585,900 |
| 分配次数 | 2,546,858 | 2,547,343 |
| 每提交索引警告 | 0 | 0 |

时间差约 -0.41%，分配字节差约 -0.05%，分配次数差约 +0.02%，均不足以证明有意义的性能变化。
这次交付确认的是架构重复与后台轮询减少，**不据此宣称整体吞吐或尾延迟提升**。

## 本轮边界

- 保持已发布 2.0 的 Parquet/WAL 格式；没有引入新元数据编码或迁移。
- 保留现有租户读视图保护与缓存预算；未实现按文件固定引用的 GC，也未把全部缓存合成一个预算池。
- 未重跑 30 分钟持续负载、10 万实体性能矩阵、RSS/CPU/磁盘同步统计或对象存储备份网络验收。
  本次备份恢复 HTTP 验证使用本地备份目的地，对象备份单元测试包含在全量 Go 检查中。
- 此报告是本次改动的验证记录，不是新版本发布或完整性能验收报告。
