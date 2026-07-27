# 旧文本查询 DSL 兼容性测试报告

日期：2026-07-23

## 目标

验证 1.0 `FIND`/`MATCH` 文本 DSL 兼容入口的可靠性。该入口过去称为
`GQL`，但不是 GraphQL。测试数据使用 CMDB 典型的实体、关系和来源治理作为
一个覆盖场景；测试覆盖数据写入、旧文本 DSL 解析、JSON DSL 映射、查询执行、
HTTP/CLI 入口、stream、分页、一致性和错误边界。

## 测试数据集

主测试 fixture 通过 `POST /v1/commits` 写入，覆盖：

- 实体：`service`、`host`、`database`
- 关系：`depends_on`、`runs_on`
- relation impact：`impact_direction=reverse`
- entity source：`manual`、`agent`、`cloud`
- source priority：`1000`、`100`、`50`
- host 字段类型：string、bool、float、negative number、null
- edge 字段：`status`、`protocol`
- edge source/source_priority
- CMDB 典型链路：
  - `service:checkout -> service:api -> database:orders`
  - `service:checkout -> host:app-01`
  - `service:checkout -> host:stale-01`
  - `service:api -> host:app-02`

## 覆盖点

核心 旧文本 DSL 操作：

- `FIND`
- `MATCH`（1-8 步有界 pattern，每步独立方向/关系/节点/边条件）
- `NEIGHBORS`
- `TRAVERSE`
- `IMPACT`
- `SHORTEST`
- `EXPLAIN`
- `PROFILE`

过滤和组合：

- `=`
- `!=`
- `IN`
- `EXISTS`
- `EXISTS <field>`
- `>`
- `<=`
- keyword operators：`EQ`、`GTE`、`LT`、`LTE`
- `PREFIX`
- `CONTAINS`
- `FUZZY`
- `AND`
- `OR`
- `NOT`
- 括号组合
- literal values：single quoted string、escaped string、bool、null、float、negative number

图查询增强：

- `EDGE WHERE`
- `IN` / `BOTH` direction
- `REL` / `RELATION` / `RELATIONS` alias
- `PATH RELATIONS ... NODES ...`
- `PATH STEP REL ... NODE ...`
- path step 上的节点字段 `WHERE`
- path step 上的 `EDGE WHERE`
- `END KIND`
- `END WHERE`
- reverse impact traversal
- shortest path depth 限制
- shortest path path-step pruning

结果控制：

- `PROJECT`
- `ORDER BY`
- `SORT` / `SORT BY`
- `AGG count(), avg(...)`
- `AGGREGATE count() AS total, max(...) AS max_cpu`
- `GROUP BY`
- `HAVING`
- HAVING keyword operators
- `LIMIT`
- `cursor` 分页
- `profile=true`
- 旧文本 DSL stream meta 中的 `groups`

入口和边界：

- `POST /v1/query/gql`
- `POST /v1/query/gql/stream`
- `Content-Type: application/json`
- `Content-Type: text/plain`
- `Content-Type: application/gql`
- CLI `graphdb gql <tenant-id> <query.gql>`
- `min_version` reader freshness
- invalid 旧文本 DSL syntax
- `cost_limit`
- invalid `timeout_ms`

## 相关测试文件

- `internal/query/gql_test.go`
- `internal/query/gql_operator_matrix_test.go`
- `internal/query/dsl_advanced_test.go`
- `internal/httpapi/gql_test.go`
- `internal/httpapi/gql_fullstack_test.go`
- `cmd/graphdb/query_commands_test.go`

## 本轮发现和修复

1. 旧文本 DSL HTTP body 原来没有 `cursor` 字段，无法完整验证 旧文本 DSL 分页。
   - 修复：`GQLQueryRequest` 增加 `cursor`，并映射到 `query.Request.Cursor`。
   - 文档和 OpenAPI 已同步。

2. 旧文本 DSL stream 需要覆盖分组结果。
   - 修复：新增 `POST /v1/query/gql/stream`。
   - stream meta 现在携带 `aggregates` 和 `groups`。

3. 完成后审计发现语法矩阵还缺少部分组合验证。
   - 补充：literal/operator 矩阵、direction 矩阵、PATH node/end filter、shortest path pruning、aggregate alias、HTTP `profile=true` 和 invalid `timeout_ms`。

## 验证命令

```bash
go test ./...
go test -race ./internal/graph ./internal/query ./internal/storage ./internal/httpapi
```

当前结果：

- `go test ./...` 通过。
- `go test -race ./internal/graph ./internal/query ./internal/storage ./internal/httpapi` 通过。

## 结论

当前 旧文本 DSL 已覆盖通用图查询能力，并以内部 CMDB 常用查询作为一组验证样例：

- 当前态实体查询
- 一跳邻居查询
- 多跳路径查询
- 影响分析
- 最短路径
- 复杂布尔过滤
- edge 字段过滤
- 分步路径约束
- 有界 `MATCH`、标签条件和逐步 `OUT`/`IN`/`BOTH`
- 分组聚合和 having
- 分页和 stream
- HTTP/CLI 双入口
- reader freshness 和错误边界

剩余不在当前边界内的能力：

- 完整 Cypher/Gremlin（当前只承诺 1-8 步有界 `MATCH`）
- 子查询
- join
- 用户自定义函数
- 历史版本查询
- 跨租户查询
