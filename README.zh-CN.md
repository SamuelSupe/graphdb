# GGraphDB 2.3.8

**多租户属性图数据库，内置中英文管理控制台。**

[English](README.md) · [下载发行版](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.8) · [文档中心](docs/README.zh-CN.md) · [发行说明](release/local-disk.md)

管理实体、关系与来源治理，通过 JSON DSL 或 GraphQL 查询图数据。从一个进程、
一块本地磁盘开始；需要高可用时增加独立 Raft 副本，需要横向扩展时使用租户分片。
运行时无需对象存储或 PostgreSQL；S3 兼容存储可用于可选的快照备份。

[快速启动](#启动) · [写入与查询](#写入与查询) · [选择部署](#部署方式) · [验证范围](#验证与限制)

## 当前版本

**[2.3.8](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.8)** 已发布为正确性补丁；确切标签门禁、独立下载的 Linux/macOS 恢复、旧 WAL 升级与 catalog 故障检查通过，证据及边界见验证记录。

| 领域 | 2.3.8 提供的能力 |
| --- | --- |
| 管理 UI | `/ui/` 内嵌英文／简体中文控制台，无需独立前端服务 |
| 日常操作 | 图表概览、结构化表单与详情、查询工作台、数据接入和维护 |
| 大量租户 | 服务端租户 ID 搜索，租户切换和集群归属使用有界分页 |
| 正确性 | 索引有限重试与可取消等待、WAL 缓冲及剩余 tail 容量推进 |
| 部署 | 单机 direct/WAL、独立 Raft 副本、按租户分片的 Raft 组 |

验证证据见[发布验证](docs/validation-v2.3.8.md)，兼容规则见[版本边界](docs/naming-and-compatibility.zh-CN.md)。

## 启动

### 1. 运行服务

在仓库根目录执行，Go 版本以 [go.mod](go.mod) 为准：

```sh
go build -o bin/graphdb ./cmd/graphdb
GRAPHDB_DATA_DIR=./.graphdb bin/graphdb serve
```

使用预编译程序时，按[发行版部署](docs/user/release-deployment.zh-CN.md)下载并校验。

<details>
<summary>使用容器启动</summary>

在仓库根目录执行。默认 Compose 部署包含一个服务和一个持久化卷。

```sh
docker compose up -d --build
```

</details>

### 2. 打开控制台

访问 **[http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/)**。顶部选择租户，
侧栏底部切换语言。分离监听器时使用管理端口，详见[控制台指南](docs/management-ui.zh-CN.md)。

### 3. 检查服务

在另一个终端执行：

```sh
curl -fsS http://127.0.0.1:8080/v1/health
```

Health 检查进程存活。接入生产流量前，执行[就绪检查](docs/local-disk.zh-CN.md#就绪与内存预算)。

## 写入与查询

保持服务运行，在仓库根目录执行以下命令，以便正确读取[示例文件](examples/)。
这些示例使用默认的合并监听器。

### 1. 创建租户

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

### 2. 写入示例图数据

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json
```

### 3. 使用 JSON DSL 查询

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json
```

GraphQL 的完整示例见 [GraphQL 指南](docs/graphql.zh-CN.md)。控制台查询工作台
也支持表格、JSON 和关系图结果视图。

| 契约 | 含义 |
| --- | --- |
| `X-Tenant-ID` | 指定数据 API 操作的租户 |
| `version` | 查询必须观察某次写入时，将写入响应的版本作为 `min_version` |
| `data_hash` | 提交摘要，算法标识为 `sha256-shards-v2:<hex>` |
| WAL 受理 | `GRAPHDB_INGEST_MODE=wal` 启用 WAL；HTTP 202 表示受理，`Prefer: wait=committed` 等待终态结果 |

## 核心能力

| 使用场景 | 支持的操作 |
| --- | --- |
| 建模与治理 | 实体／关系类型、字段约束、身份键、来源优先级、幂等写入和采集游标 |
| 查询 | 匹配、路径模式、邻居、遍历、影响分析、最短路径、聚合、分页和流式查询 |
| 存储与接入 | direct 提交、持久化 WAL、Parquet 提交、快照和持久化索引 |
| 运维 | 导入导出、保存查询、租户生命周期、备份恢复、修复、压实、GC 和持久化任务 |
| 集成与观测 | Go/Python SDK、Prometheus 指标、JSON 日志和可选 OTLP 链路 |

## 部署方式

| 正确性 | 索引有限重试与可取消等待、WAL 缓冲及剩余 tail 容量推进 |
| 部署 | 适用需求 | 从这里开始 |
| --- | --- | --- |
| 单机 | 一个进程，同步持久化本地 direct/WAL 写入 | [本地磁盘指南](docs/local-disk.zh-CN.md) · [Compose](docker-compose.yml) |
| Raft | 多数派持久化与 Leader 接管；每个副本在独立磁盘保留完整数据 | [三副本指南](docs/raft-ha.zh-CN.md) · [Compose](docker-compose.raft.yml) |
| 分片 Raft | 独立目录组／数据分片、租户归属与显式迁移 | [分片指南](docs/sharding.zh-CN.md) · [Compose](docker-compose.sharded.yml) |

三种部署均支持 direct 和 WAL。单机不设置 `GRAPHDB_RAFT_*`；Raft 必须完整配置
节点参数；分片增加目录组／数据分片角色及 `serve-router`。独立部署使用不同端口和目录。

**目录独占。** 每个数据目录只能由一个进程使用。对该目录执行离线 CLI 前必须停止
服务，在线管理使用 HTTP API。支持 Linux/macOS 本地文件系统；不支持独立读写进程
或共享网络文件系统。

**升级与恢复。** 单机 2.0/2.1/2.2 停止旧进程后可沿用原目录。Raft 副本目录不能
通过删除配置切换成单机，单机数据也不能直接作为副本。跨部署迁移使用 API 导入或
备份恢复到新目录。Raft 升级遵循[已验收的滚动窗口](docs/raft-rolling-upgrade.zh-CN.md)。不提供 1.x 迁移。

**备份与访问。** 单机支持内置 S3 调度，Raft 使用外部调度调用集群备份 API。
[自动备份 worker](docs/backup-automation.zh-CN.md)为两者提供重试、核验、隔离演练
及 Compose/systemd 示例。对外开放前，在[网关](docs/security-deployment.zh-CN.md)
配置认证、租户授权和 TLS。

## 验证与限制

2.3.6 解除本地维护索引工作对 WAL 确认的前台阻塞，统一并发重建的发布顺序，
减少 Raft 屏障内重复文件系统操作，并在原有共享编码限额内准备 compact 两种表示。

相同输入的候选对照中，单机写入 P99 **9.470 → 3.948 秒**；原始 30 分钟增长计划下，
Raft 查询最大 **13.132 → 5.584 秒**。比较对象是工作区候选 C3 与 C7，不能当作已发布
2.3.4 与 2.3.6 二进制的对照。单机 stale 频率**未改善（131/327 → 134/327）**，
全部 stale 样本均在更新中，静默后索引追平。单机容器内存峰值增加 **6.4%**；
Raft 基线资源计数缺失。查询和接入仍有秒级长尾。两种部署各自保持输入一致，不能跨部署直接比较。

另行完成的确切标签单机 30 分钟门禁中，写入 P99 **5.627 秒**、最大 **7.652 秒**，索引 **53/59 次 stale**，最后采样仍为 **stale**。门禁通过不表示 stale 已解决；它的增长型负载与固定数据候选对照不同。

另行完成的确切标签 Raft 30 分钟门禁中，查询最大 **16.042 秒**、写入最大 **10.968 秒**，索引 **0/59 次 stale**。秒级长尾仍在；这组发行门禁负载与候选固定计划对照分开记录。

详见[确切发行证据](docs/validation-v2.3.6.md)与[前后对照报告](docs/performance-tail-validation-2026-10-20.zh-CN.md)。
确切标签门禁与下载程序的 Linux/macOS 运行核验均通过；跨宿主机可靠性、日级稳定性、生产容量和统一低延迟 SLO 仍未验收。

<details>
<summary>历史性能证据</summary>

此前本机候选记录 71,140 次操作、90 次预期写入 429、最大等待 40.154 秒。
其维护长测还有四次 HAProxy 503 查询错误，结论仍为历史 FAIL，与上面的确切标签结果
分开记录。[2.1.2 写入长尾](docs/performance-write-tail.md)和 [2.0 结果](docs/performance-v2.0.md)
仅适用于各自记录的构建。

</details>

<details>
<summary>存储与发布设计</summary>

先同步数据文件，再发布 manifest/catalog。四个有界工作线程按文件、字节和时间
预算合并目录同步；Parquet 随机读取选择列和行组；有界缓存随本地发布失效。
GC 保护活跃读视图，破坏性的生命周期操作等待读视图结束。

Raft 在协议允许时把维护准备移出应用屏障，仅暂停维护租户的冲突写入；控制消息
使用独立有界队列，迁移按块落盘。发布、图解码和回滚仍消耗资源。详见[整体架构](docs/architecture.md)。

</details>

## 文档与验证

| 我想要…… | 阅读入口 |
| --- | --- |
| 浏览全部指南 | [文档中心](docs/README.zh-CN.md) · [用户指南](docs/user/README.zh-CN.md) |
| 使用控制台 | [管理 UI](docs/management-ui.zh-CN.md) |
| 编写查询或客户端 | [查询能力](docs/query_capabilities.md) · [GraphQL](docs/graphql.zh-CN.md) · [OpenAPI](docs/openapi.yaml) · [Go SDK](sdk/go/graphdb/README.md) · [Python SDK](sdk/python/README.md) |
| 维护服务 | [产品运维](docs/product-operations.zh-CN.md) · [Raft 运维](docs/raft-operations.zh-CN.md) · [诊断指标](docs/diagnostics-metrics.zh-CN.md) |
| 核对发行证据 | [2.3.8 验证](docs/validation-v2.3.8.md) · [兼容规则](docs/naming-and-compatibility.zh-CN.md) |

用户指南与控制台文档提供中英文版本；部分集群运维专题目前仅有中文。

## 开发

后端运行验证使用 OrbStack Linux，并将数据库数据放在 Linux 本地卷。

| 检查 | 命令 |
| --- | --- |
| 单元测试 | `go test -mod=readonly ./...` |
| 静态分析 | `go vet -mod=readonly ./...` |
| 完整静态发行门禁 | `RELEASE_GATE_VERIFY_ONLY=1 scripts/release_gate.sh` |
| 运行集成验证 | `RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh` |
| 三十分钟维护长测 | `GRAPHDB_GATE_SOAK=1 RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh` |

运行门禁覆盖 direct/WAL、图 API、维护和重启一致性。脚本只管理自己创建的进程，
在输出目录保留验证证据。

[贡献指南](CONTRIBUTING.md) · [安全说明](SECURITY.md) · [许可证：保留所有权利](LICENSE)
