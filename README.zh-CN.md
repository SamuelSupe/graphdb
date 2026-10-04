# GGraphDB 2.2

[English](README.md)

GGraphDB 是多租户属性图数据库，提供实体关系管理、来源治理、写入接入、图查询
和运维功能。默认部署为单机单进程；可选 [三副本 Raft 高可用](docs/raft-ha.zh-CN.md) 使用独立磁盘的 Share-Nothing 结构。所有数据持久化在本地磁盘，运行时无需
对象存储或 PostgreSQL。可选的 [S3 兼容快照备份](docs/object-backup.zh-CN.md) 支持恢复到新的本地磁盘。

## 当前版本

[2.2.4](https://github.com/SamuelSupe/graphdb/releases/tag/v2.2.4) 已从 `main` 发布为稳定 Latest，全部确切标签门禁及下载资产核验通过。
同一二进制支持单机 direct/WAL、使用独立磁盘的 Raft 副本，以及按租户分片的多个 Raft 组。
本版增加受保护的集群管理、可续传迁移和恢复、兼容窗口内的滚动升级及本地诊断。
二进制、契约和验证范围见[发行说明](release/local-disk.md)和[版本边界](docs/naming-and-compatibility.zh-CN.md)。

单机 2.0/2.1 用户停止旧进程后可沿用原目录；Raft 目录具有独立的角色和协议要求，
升级遵循[已验收的滚动兼容窗口](docs/raft-rolling-upgrade.zh-CN.md)。单机继续支持自动 S3 备份，
Raft 由外部调度调用集群备份 API。不提供 1.x 迁移。

跨宿主机与生产容量仍待验收；维护会产生可重试 429 和写入长尾，不承诺统一吞吐增幅或低延迟 SLO。
本版已通过确切标签的全部发行门禁。此前性能候选的三十分钟长测仍有四次入口 503，记录为 FAIL；不承诺整体吞吐收益。范围见 [2.2.4 验证说明](docs/validation-v2.2.4.md)。

## 核心能力

- JSON DSL 与 GraphQL：匹配、路径模式、邻居、遍历、影响分析、最短路径、聚合、分页与流式查询。
- 实体/关系类型、字段约束、身份键、来源优先级、幂等写入与采集游标。
- direct 和同步 WAL 接入；使用 Parquet 提交、快照和索引；提交返回带算法标识的 `data_hash`。
- 导入导出、保存查询、租户生命周期、备份恢复、修复、压实、GC 与持久化后台任务。
- Go/Python SDK、Prometheus 指标、JSON 日志与可选 OTLP 链路。

## 启动

```sh
go build -o bin/graphdb ./cmd/graphdb
GRAPHDB_DATA_DIR=./.graphdb bin/graphdb serve
```

容器部署只包含一个服务和一个持久化卷：

```sh
docker compose up -d --build
```

同一二进制同时支持单机、单组 Raft 和分片 Raft：

本轮磁盘保护、流式快照、维护隔离、完整灾备和诊断告警见 [产品运维说明](docs/product-operations.zh-CN.md)。

Raft 验收范围与剩余部署验收见 [发布验收状态](docs/raft-release-readiness.zh-CN.md)。

[自动备份 worker](docs/backup-automation.zh-CN.md)支持单机与 Raft 外部定时调度、持久化重试、校验和隔离演练，并提供 Compose/systemd 示例；单机内置策略继续支持。

| 部署方式 | 启用方式 | 数据持久化 | 运维 |
| --- | --- | --- | --- |
| 单机（默认） | 不设置 `GRAPHDB_RAFT_*`；使用 `docker-compose.yml` | 本机同步 direct / WAL | 定时维护、自动 S3 备份、停机后离线 CLI |
| Raft（默认三副本） | 完整配置 `GRAPHDB_RAFT_NODE_ID` 等参数；使用 `docker-compose.raft.yml` | 多数派持久化，节点各持完整副本 | Leader 接管、集群 API 维护；首版自动备份由外部调度 |
| 分片 Raft | 配置目录组、数据分片角色及 `serve-router`；使用 `docker-compose.sharded.yml` | 每个分片及目录组分别多数派持久化 | 新增分片、分配新租户、迁移已有租户，见 [分片运行说明](docs/sharding.zh-CN.md) |

所有部署均支持 direct 和 WAL 接入，可在同一环境中使用不同端口、不同数据目录独立运行。已有单机目录保持兼容；Raft 副本目录不能通过删除配置切换成单机，单机数据也不能直接作为新 Raft 副本。跨部署迁移使用 API 导入或备份恢复到新目录。

数据目录由一个进程独占。离线 CLI 必须在服务停止后使用，在线管理使用 HTTP API。
支持 Linux/macOS 本地文件系统；不提供独立读写进程或共享网络文件系统。跨机器复制使用可选 [Raft 部署](docs/raft-ha.zh-CN.md)，每个节点拥有独立目录。

## 写入与查询

```sh
# 1. Create a tenant
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'

# 2. Write example graph data
curl -fsS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json

# 3. Query with the JSON Query DSL
curl -fsS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json

# 4. Query the generic graph with GraphQL
curl -fsS -X POST http://127.0.0.1:8080/v1/query/graphql \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"query":"query Find($request: QueryRequest!) { graph(request: $request) { version results stats } }","operationName":"Find","variables":{"request":{"op":"match","kind":"person","where":[{"field":"name","op":"eq","value":"Alice"}],"project":["id","name"],"limit":10}}}'
```


查询需要观察某次写入时，将响应的 `version` 作为 `min_version`。
启用 WAL 使用 `GRAPHDB_INGEST_MODE=wal`：202 表示 WAL 已接受请求，
`Prefer: wait=committed` 表示等待终态结果。

## 本地存储实现

数据文件同步后才发布 manifest/catalog；四个有界工作线程批量合并目录同步。
Parquet 直接通过文件随机读取选择列和行组，读缓存按本地发布通知失效。
GC 延迟回收被活跃读视图保护的文件；清理提交、清空和恢复会等待相关读视图结束。

Raft 在协议允许时把维护准备移出应用屏障，冲突持续时只暂停维护租户的新写入。
控制消息使用独立有界队列，迁移按块落盘；最终发布、图解码和回滚仍有资源开销。

本机候选 30 分钟负载记录 71,140 次操作、零非预期操作错误；写入包含 90 次预期 429，
最大等待 40.154 秒。这些是限定范围的正确性观测，不构成容量或延迟保证，见
[验收范围](docs/validation-v2.2.4.md)。历史 [2.1.2 写入长尾报告](docs/performance-write-tail.md)
仅适用于其记录的构建。

## 文档与验证

- [本地磁盘运行与验证](docs/local-disk.zh-CN.md)
- [2.2.4 验证范围与限制](docs/validation-v2.2.4.md)
- [Raft 运维与滚动升级](docs/raft-operations.zh-CN.md)
- [租户分片与迁移](docs/sharding.zh-CN.md)
- [诊断指标](docs/diagnostics-metrics.zh-CN.md)
- [架构](docs/architecture.md)
- [用户手册](docs/user/README.zh-CN.md)
- [查询能力](docs/query_capabilities.md)、[GraphQL](docs/graphql.zh-CN.md)
- [OpenAPI](docs/openapi.yaml)、[Go SDK](sdk/go/graphdb/README.md)、[Python SDK](sdk/python/README.md)

后端验证默认在 OrbStack 内使用 Linux 本地卷运行：

```sh
go test -mod=readonly ./...
go vet -mod=readonly ./...
RELEASE_GATE_VERIFY_ONLY=1 scripts/release_gate.sh
RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh
GRAPHDB_GATE_SOAK=1 RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh
```

验证流程覆盖 direct/WAL、图 API、维护与重启一致性，可选持续负载为 30 分钟。
脚本只管理自己创建的进程，并在输出目录保留证据。

参见 [贡献指南](CONTRIBUTING.md)、[安全说明](SECURITY.md) 和 [许可证](LICENSE)。
