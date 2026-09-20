# 本地磁盘版代码简化与验证（2026-09-19）

基线：`codex/local-disk-v2` 的 `aa2c9f4c841f7cf00f1b57fbf33dcbf67f11ab36`。
对比对象：随 `v1.3.4-local.2` 发布的本轮代码简化实现。原 `main` 和已有发行版本未修改。

## 完成的改造

- 死信内部遍历使用一次新鲜目录扫描，保留游标、提前停止和错误传播；避免每翻一页重新枚举整个目录。
- 深度索引检查按实体页、边分片一次分组；当前和历史分片规则共用一次哈希计算，保留内容校验。
- 删除不可达的 PostgreSQL 协调实现及其专用测试。保留本地租户锁、写入围栏、WAL 恢复、持久化格式，以及拒绝 PostgreSQL 标记和不支持配置的边界。
- 本地 GC 默认按最多 64 次删除分批，在批次间释放读视图锁和租户锁。每批重新读取 manifest/catalog，重新判断文件是否仍被引用；显式 `MaxDeletes` 继续返回单批检查点。索引孤儿清理也支持预算、游标和 dry-run。

生产 Go 代码从 93,606 行降至 86,456 行，净减少 **7,150 行**。测试代码从 65,963 行降至 61,391 行；移除的是已退役协调路径测试，同时补充目录扫描、历史分片和 GC 并发边界回归。统计包含新增文件，不含忽略目录中的临时工具。

## 定向性能结果

环境：OrbStack，Linux arm64，`golang:1.25-bookworm`（Go 1.25.14），容器限制 4 CPU / 6 GiB，`GOMAXPROCS=4`。源码放在 Linux 卷，基准使用 FileStore 和 Go 测试临时目录。

复用 `BenchmarkIndexHealth10K`：10,000 实体、5,000 边，先写入、compact、重建索引并检查一次，然后计时。每个版本执行一次 `-benchtime=3x -count=1`；最终基准没有与本任务的其他测试并行运行。

| 指标 | 基线 | 最终实现 | 本次变化 |
| --- | ---: | ---: | ---: |
| ns/op | 1,650,654,591 | 1,486,446,947 | -9.95% |
| B/op | 3,395,348,826 | 3,370,343,616 | -0.74% |
| allocs/op | 18,425,038 | 17,488,888 | -5.08% |

这是预热后的深度检查小样本，未清空操作系统页缓存，不代表冷启动、冷盘、请求 p95 或整体吞吐。`B/op` 是累计分配字节，不是峰值 RSS；单次检查的累计分配仍较高。本轮没有重跑三方案多轮对比、十万实体负载或 30 分钟持续负载，不能据此宣称原计划的整体性能验收目标已达成。

## 正确性验证

以下均在 OrbStack 实际执行并通过：

| 验证 | 结果 |
| --- | --- |
| 干净源码的 `go test -mod=readonly ./... -timeout 120s` | 全包通过 |
| `go vet -mod=readonly ./...` | 通过 |
| `go test -mod=readonly -race -p 2 ./... -timeout 300s` | 全包通过，storage 包耗时 159.466 秒 |
| 新增 GC 场景 | dry-run 不删除、删除预算、批次间新增引用受保护、活跃读视图阻止删除、查询可在批次间获得读视图 |
| HTTP release gate | direct/WAL 写入、查询、索引重建、导出和重启前后快照一致；4 写入客户端 / 16 查询客户端小负载通过 |
| Python SDK | 实际 HTTP 服务上 12 项测试通过 |
| 二进制兼容 | `release_20260722_01` 与当前工作区双向本地数据兼容通过；发行契约检查通过 |
| MinIO 对象备份 gate | race 集成测试、快照捕获、新数据目录发现与按需恢复、覆盖恢复、恢复后重启验证通过 |

可复用命令（在上述 Linux 环境中）：

```sh
go test -mod=readonly ./... -timeout 120s
go vet -mod=readonly ./...
go test -mod=readonly -race -p 2 ./... -timeout 300s
go test -mod=readonly ./internal/storage -run '^$' \
  -bench '^BenchmarkIndexHealth10K$' -benchtime=3x -count=1 -benchmem
RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh
scripts/check_release_freeze.sh
scripts/compatibility_v1_0_v1_1.sh
# 按脚本要求配置专用测试桶和 GRAPHDB_TEST_BACKUP_S3_* 环境变量后运行：
scripts/object_backup_gate.sh
```

原始证据保存在本工作区忽略目录 `.workflow/ponytail-20260919/`：`before.log`、`final-benchmark.log`、`final-race.log`、`source-counts.json`、`http/`、`compatibility.log`、`backup/`。测试用 MinIO 容器和网络已移除。

## 仍然存在的边界

GC 限制的是每批删除数量，不是持锁时间上限。单个大文件的校验、首次大目录枚举仍可能耗时；本轮未引入新的索引服务或后台目录缓存。上述测试覆盖本轮改动的行为和关键并发边界，不构成所有负载下无缺陷的保证。
