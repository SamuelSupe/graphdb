# GGraphDB 2.1

[English](README.md)

GGraphDB 是多租户属性图数据库，提供实体关系管理、来源治理、写入接入、图查询
和运维功能。2.0 主版本采用单机单进程架构，所有数据持久化在本地磁盘，运行时无需
对象存储或 PostgreSQL。可选的 [S3 兼容快照备份](docs/object-backup.zh-CN.md) 支持恢复到新的本地磁盘。

## 当前版本

[2.1.2](https://github.com/SamuelSupe/graphdb/releases/tag/v2.1.2) 是主版本，由 `main` 发布。
在线图数据存放在本地盘，S3 兼容对象存储用于快照备份与按需恢复。
不提供 1.x 迁移或旧摘要兼容层；从 1.x 升级需要新目录。2.0 用户停止旧进程后可沿用原目录。
2.1 新增[定时 S3 备份、重试、保留清理和恢复演练](docs/object-backup.zh-CN.md#自动备份)，默认关闭。
二进制、契约变化及验证证据见[发行说明](release/local-disk.md)和[版本边界](docs/naming-and-compatibility.zh-CN.md)。

2.1.2 延后校验仍被活跃查询保护的孤儿索引文件，减少 GC 在租户锁内的无效工作。
磁盘格式、API 和同步持久化默认值保持不变。单元/race、HTTP 和 S3 检查通过；
本次按发布要求提前停止 30 分钟持续负载，该项未完成，不计为通过。

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
GC 延迟回收被活跃读视图保护的文件；清理提交、清空和恢复会等待相关读视图结束。

在 OrbStack 的 4 写入、16 查询客户端并行运行后台维护的重点负载中，写入 P95 从
11.77–12.34 秒降至 8.08 秒。这是有限范围的实测结果，不是生产延迟保证：仍有秒级写入
长尾，压实耗时也出现尚未稳定归因的退化信号。方法与限制见[写入长尾报告](docs/performance-write-tail.md)，
发布检查见 [2.1.2 验证记录](docs/validation-v2.1.2.md)。历史报告仅代表各自版本。

## 文档与验证

- [本地磁盘运行与验证](docs/local-disk.zh-CN.md)
- [2.1.2 写入长尾实测与限制](docs/performance-write-tail.md)
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
