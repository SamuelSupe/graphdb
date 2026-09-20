# GGraphDB — 本地磁盘版

[English](README.md)

GGraphDB 是多租户属性图数据库，提供实体关系管理、来源治理、写入接入、图查询
和运维功能。这个分支采用单机单进程架构，所有数据持久化在本地磁盘，运行时无需
对象存储或 PostgreSQL。可选的 [S3 兼容快照备份](docs/object-backup.zh-CN.md) 支持恢复到新的本地磁盘。

## 独立版本

[`v1.3.4-local.4`](https://github.com/SamuelSupe/graphdb/releases/tag/v1.3.4-local.4)
从独立分支 [`codex/local-disk-v2`](https://github.com/SamuelSupe/graphdb/tree/codex/local-disk-v2)
发布为本地磁盘预发布版。默认 `main` 分支和稳定版 Latest 保持原样。
二进制下载、兼容边界和验证证据见[发行说明](release/local-disk.md)。

## 核心能力

- JSON DSL 与 GraphQL：匹配、路径模式、邻居、遍历、影响分析、最短路径、聚合、分页与流式查询。
- 实体/关系类型、字段约束、身份键、来源优先级、幂等写入与采集游标。
- direct 和同步 WAL 接入；沿用 Parquet 提交、快照和索引格式。
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

数据目录由一个进程独占。离线 CLI 必须在服务停止后使用，在线管理使用 HTTP API。
支持 Linux/macOS 本地文件系统；本版不提供独立读写进程、共享网络文件系统或跨机器复制。

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
GC、清理提交、清空和恢复会等待活跃读视图结束。

性能工具支持原对象存储和原本地模式的完整对比。本次仅做本地模式之间的单轮重点抽查，
范围和结果见下方验证报告；历史版本报告不代表本分支性能。

## 文档与验证

- [本地磁盘运行与验证](docs/local-disk.zh-CN.md)
- [本地磁盘 v2 验证与性能抽查](docs/performance-local-disk-v2.md)
- [本地磁盘第二轮优化：分页、JSON 编码与 Parquet 布局](docs/performance-local-disk-optimization-2.md)
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
