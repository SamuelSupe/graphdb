<div align="center">

# GGraphDB

**面向实体、关系与拓扑的通用对象存储图数据库**

[![Latest Release](https://img.shields.io/github/v/release/SamuelSupe/graphdb?display_name=tag)](https://github.com/SamuelSupe/graphdb/releases)
[![Release Build](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml/badge.svg)](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml)
[![Public Repository](https://img.shields.io/badge/repository-public-2ea44f)](https://github.com/SamuelSupe/graphdb)

[English README](README.md) · [最新发行版](https://github.com/SamuelSupe/graphdb/releases/latest)

</div>

GGraphDB 1.2 是一个 Go 实现的通用当前态属性知识图谱，面向实体关系数据。
知识库、CMDB、资产关系、服务依赖、IT 拓扑和影响分析都是它支持的应用场景。
它把租户数据持久化到本地磁盘或 S3 兼容对象存储，使用 Parquet、manifest
CAS、快照和提交回放，提供可追踪的写入版本与可控的新鲜度。它不是 RDF/OWL、
SPARQL、本体推理或历史图引擎。

## 核心能力

| 能力 | 说明 |
| --- | --- |
| 多租户图数据 | 通过 `X-Tenant-ID` 隔离租户前缀、实体、边和索引。 |
| 可选领域建模 | 实体类型、标签、关系属性 schema、identity reconciliation、source priority 和人工合并/拆分。 |
| 图查询 | GraphQL、JSON Query DSL、有界 pattern match、双向遍历、impact、shortest path。 |
| 批量导入 | task 驱动、可恢复的 JSONL 和 CSV ingest。 |
| 对象存储持久化 | Parquet manifest、commit、snapshot、entity page、edge shard 和 index object。 |
| 读写分离 | 同一二进制支持 `all`、`writer`、`reader`，通过部署拓扑分流。 |
| 可选多写协调 | PostgreSQL head CAS 支持每租户 2–8 个乐观并发 writer，本地协调仍为默认。 |
| 运维能力 | compact、GC、backup/restore、repair、integrity audit、index health 和 metrics。 |

## 架构一览

```mermaid
flowchart LR
  A[采集器 / API] --> W[Writer\\nGRAPHDB_MODE=writer]
  W -. 可选 head CAS .-> P[(PostgreSQL\\n协调状态)]
  W --> O[(S3 / RustFS\\nParquet + Manifest)]
  O --> R[Reader 集群\\nGRAPHDB_MODE=reader]
  R --> Q[GraphQL / JSON DSL 查询]
  A --> A1[all 模式\\n本地开发]
  A1 --> O
```

## 快速开始

### 本地文件存储

需要 Go 1.25 或更高版本：

```sh
go run ./cmd/graphdb serve
curl -fsS http://127.0.0.1:8080/v1/health
```

### Docker Compose + MinIO

```sh
docker compose up --build
curl -fsS http://127.0.0.1:8080/v1/health
```

### RustFS Writer/Reader 拓扑

```sh
docker compose -f docker-compose.rustfs.yml up --build
curl -fsS http://127.0.0.1:38080/v1/health  # writer
curl -fsS http://127.0.0.1:38081/v1/health  # reader
```

### 创建租户、写入数据并查询

```sh
# 1. 创建租户
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'

# 2. 写入示例图数据
curl -fsS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json

# 3. 使用 JSON Query DSL 查询
curl -fsS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json

# 4. 使用 GraphQL 查询通用图数据
curl -fsS -X POST http://127.0.0.1:8080/v1/query/graphql \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"query":"query Find($request: QueryRequest!) { graph(request: $request) { version results stats } }","operationName":"Find","variables":{"request":{"op":"match","kind":"person","where":[{"field":"name","op":"eq","value":"Alice"}],"project":["id","name"],"limit":10}}}'
```

写入响应中的 `version` 可以作为 reader 查询的 `min_version`，保证读到指定
版本；只有在明确接受最终一致性时才使用 `allow_stale=true`。

## 通用图场景：使用 GraphQL 查询

`examples/commit.json` 包含一个非 CMDB 的通用图：`person:alice` 通过
`works_at` 关系连接到 `company:acme`。GraphQL 通过 `QueryRequest` 变量接收
通用 JSON Query DSL，因此应用自定义的实体类型、字段和关系类型同样适用于
组织关系、项目依赖、数据血缘和其他图结构数据。

GraphQL document：

```graphql
query FindPerson($request: QueryRequest!) {
  graph(request: $request) {
    version
    results
    stats
  }
}
```

变量使用 `{"op":"match","kind":"person",...}`；要沿关系查询一跳邻居，可改为
`{"op":"neighbors","id":"person:alice","relation_types":["works_at"]}`。

schema、错误、alias、fragment 和 1.1 边界见
[GraphQL 文档](docs/graphql.zh-CN.md)。旧 `FIND`/`MATCH` 文本 DSL 仅在
`/v1/query/gql` 保留 1.0 兼容，不是 GraphQL。

## 部署模式

| 模式 | 适用场景 | 行为 |
| --- | --- | --- |
| `all` | 本地开发、小规模单进程部署 | 同一进程处理写入和查询。 |
| `writer` | 生产写入入口 | 写入和控制 API；本地单 writer 或 PostgreSQL 协调的 writer 集群。 |
| `reader` | 查询集群 | 从共享对象存储加载数据，提供查询和导出。 |

生产部署建议使用共享 S3/RustFS 和多个 reader。默认
`GRAPHDB_COORDINATION=local` 时每租户保持一个 writer；需要 2–8 个乐观并发
writer 时，对 generic S3/RustFS 使用 `GRAPHDB_COORDINATION=postgres`。仍以
进程 readiness 主动探测对象存储，PostgreSQL 模式按保留窗口清理已完成的
协调记录；租户流量仍以 reader fleet readiness 作为接入条件。
`X-Tenant-ID` 是租户路由标识，不是认证机制；认证、授权、TLS 和限流应由
网关或服务网格提供。

## 发行版

最新已发布版本为 GGraphDB 1.2：
[**v1.2.0**](https://github.com/SamuelSupe/graphdb/releases/tag/v1.2.0)。
发布工作流只有在该 tag 的发行 checklist、30 分钟 PostgreSQL CAS 门禁和
正式回滚演练全部通过后才会发布。

发行包包含：

- Linux amd64、Linux arm64、macOS arm64 静态二进制；
- Dockerfile、MinIO/RustFS/PostgreSQL Compose 文件和 examples；
- 部署、安全、容量、API、查询、SDK、changelog 和构建元数据；
- `.sha256` 校验文件。

详见[发行版部署文档](docs/user/release-deployment.zh-CN.md)，也可查看
[英文版本](docs/user/release-deployment.md)。推送类似 `v1.2.0` 的语义化版本
标签会触发 [GitHub Actions](.github/workflows/release.yml)，自动构建并发布
归档包。为兼容旧部署流程，`release_*` 标签仍然受支持。

## 文档入口

| 文档 | 内容 |
| --- | --- |
| [数据库简介](docs/database-introduction.zh-CN.md) · [English](docs/database-introduction.md) | 产品定位、数据模型、架构和当前边界。 |
| [使用手册](docs/user/usage-manual.zh-CN.md) · [English](docs/user/usage-manual.md) | 租户、写入、查询、可选的 CMDB 场景能力、索引、维护和 SDK。 |
| [部署与运维](docs/user/deploy-ops.zh-CN.md) · [English](docs/user/deploy-ops.md) | `all`/`writer`/`reader`、S3、RustFS、健康检查和生产规则。 |
| [安全边界](docs/security-deployment.zh-CN.md) · [English](docs/security-deployment.md) | 数据/管理 listener、网关认证、租户绑定、RBAC 与 TLS。 |
| [容量边界](docs/capacity.zh-CN.md) · [English](docs/capacity.md) | 发行 CAS 门禁、可复现基线和推荐拓扑。 |
| [发行版部署](docs/user/release-deployment.zh-CN.md) · [English](docs/user/release-deployment.md) | Release 下载、校验、升级、回滚和安全边界。 |
| [读与查询](docs/user/read-query.zh-CN.md) · [English](docs/user/read-query.md) | GraphQL、JSON DSL、分页、流式、explain 和 profile。 |
| [写入与采集](docs/user/write-ingest.zh-CN.md) · [English](docs/user/write-ingest.md) | commit、ingest、幂等、删除、source policy 和背压。 |
| [数据模型](docs/user/data-model.zh-CN.md) · [English](docs/user/data-model.md) | tenant、可选 CI type、entity、relation、edge 和数据治理。 |
| [API Map](docs/user/api-map.zh-CN.md) · [English](docs/user/api-map.md) | 按领域整理的 HTTP endpoint 清单。 |
| [OpenAPI](docs/openapi.yaml) | HTTP API 合同，也可通过 `GET /openapi.yaml` 获取。 |
| [Go/Python SDK](docs/user/sdk.zh-CN.md) · [English](docs/user/sdk.md) | SDK 初始化、读写、流式访问和重试指导。 |
| [全部用户指南](docs/user/README.zh-CN.md) · [English](docs/user/README.md) | 完整 API、部署、运维和故障排查入口。 |

## 当前边界

GGraphDB v1 目前明确保持以下边界：

- 通用实体关系图内核，CMDB 数据治理作为可选的领域 profile；
- 本地协调默认每租户一个活跃 writer；可选 PostgreSQL 协调提供乐观多 writer head CAS；
- 对象存储是生产持久化的推荐来源；
- 强读场景通过显式 reader freshness 控制；
- 认证和授权由部署边界负责。

详细限制和成熟度缺口见[功能缺口跟踪](docs/product_function_gaps.md)。

## 开发

```sh
# 单元测试和包测试
go test -mod=readonly ./...

# 校验两种部署拓扑
docker compose config
docker compose -f docker-compose.rustfs.yml config
```

仓库还包含位于 `tools/` 和 `scripts/` 下的黑盒 e2e、负载、soak、
reader freshness、恢复和发行版门禁工具。运行长时间或有影响的检查前，
请先阅读对应的运维文档。

## 贡献与许可证

详见 [CONTRIBUTING.md](CONTRIBUTING.md)、[SECURITY.md](SECURITY.md) 和
[LICENSE](LICENSE)。当前许可证为 rights-reserved；源码公开不代表授予
生产使用或再分发权利。
