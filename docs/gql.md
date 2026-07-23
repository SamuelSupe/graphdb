# 旧文本查询 DSL（1.0 兼容入口）

1.0 文档曾把这套 `FIND`/`MATCH` 语法称为 `GQL`。GGraphDB 1.1 不再把
这个名称作为产品查询语言名称：它不是 GraphQL，也不是 ISO GQL。该入口仅为
1.0 客户端兼容而保留，并编译成现有 JSON Query DSL，继续走同一套 planner、
索引下推、lazy read、admission、timeout 和错误码。

新接入优先使用 [GraphQL](graphql.zh-CN.md) 或 JSON Query DSL。

## 入口

HTTP：

```bash
curl -sS http://127.0.0.1:8080/v1/query/gql \
  -H 'X-Tenant-ID: tenant-a' \
  -H 'Content-Type: application/json' \
  -d '{"query":"FIND host WHERE hostname PREFIX \"app-\" LIMIT 100"}'
```

也可以直接提交文本：

```bash
curl -sS http://127.0.0.1:8080/v1/query/gql \
  -H 'X-Tenant-ID: tenant-a' \
  -H 'Content-Type: text/plain' \
  --data-binary 'FIND host WHERE hostname PREFIX "app-" LIMIT 100'
```

流式旧文本 DSL：

```bash
curl -sS http://127.0.0.1:8080/v1/query/gql/stream \
  -H 'X-Tenant-ID: tenant-a' \
  -H 'Content-Type: text/plain' \
  --data-binary 'FIND host WHERE hostname PREFIX "app-" LIMIT 100'
```

CLI：

```bash
graphdb gql tenant-a query.gql
```

## 基本语法

```sql
FIND host
WHERE cpu >= 8 AND region IN ["us-east-1", "eu-west-1"]
PROJECT id, hostname, cpu, region
ORDER BY cpu DESC
LIMIT 100
```

关键字大小写不敏感。字符串支持双引号或单引号。未加引号的值会按 identifier 字符串处理，数字会按数值处理。

## 查询类型

### FIND

查询实体。

```sql
FIND host
WHERE hostname PREFIX "app-"
PROJECT id, hostname, cpu
ORDER BY cpu DESC
LIMIT 100
```

编译为 `op=match`。

### MATCH

按起点类型和字段筛选实体，再执行固定步数的图模式匹配：

```sql
MATCH document
WHERE labels CONTAINS "article"
PATH
STEP OUT REL cites NODE document WHERE status = "published"
STEP IN REL authored_by NODE person
LIMIT 20
```

编译为 `op=pattern`。`PATH` 必须包含 1 到 8 个 `STEP`；每一步都可以独立
指定 `OUT`、`IN` 或 `BOTH`、关系类型、目标实体类型、目标实体条件和边条件。
未写方向时使用 `OUT`。匹配的是精确步数，不会执行无界展开。

### NEIGHBORS

查询一跳邻居。

```sql
NEIGHBORS service:checkout OUT
REL depends_on, runs_on
WHERE kind IN ["host", "database"]
PROJECT id, name
LIMIT 100
```

编译为 `op=neighbors`。

### TRAVERSE

有界路径遍历。

```sql
TRAVERSE service:checkout OUT
REL depends_on
DEPTH 3
PATH NODES service, host, database
END KIND database
LIMIT 50
```

编译为 `op=traverse`。

### IMPACT

影响分析。

```sql
IMPACT database:orders
DEPTH 4
END KIND service
LIMIT 100
```

编译为 `op=impact`，执行时按 relation type 的 `impact_direction` 判断传播方向。

### SHORTEST

最短路径。

```sql
SHORTEST service:checkout TO database:orders
OUT
REL depends_on
DEPTH 6
```

编译为 `op=shortest_path`。

### EXPLAIN 和 PROFILE

```sql
EXPLAIN FIND host WHERE hostname PREFIX "app-"
PROFILE FIND host WHERE hostname = "app-01" LIMIT 10
```

也可以在 HTTP JSON body 里传 `profile=true`。

分页时复用同一条 `query`，并在 JSON body 里传上一页返回的 `cursor`。

## 条件

```sql
WHERE field = "value"
WHERE cpu >= 8 AND owner EXISTS
WHERE region IN ["us-east-1", "eu-west-1"]
WHERE hostname PREFIX "app-"
WHERE name CONTAINS "checkout"
WHERE name FUZZY "chckout"
WHERE (cpu >= 16 OR hostname = "db-01") AND NOT owner EXISTS
```

支持：

- `=` / `EQ`
- `!=` / `NEQ`
- `>` / `GT`
- `>=` / `GTE`
- `<` / `LT`
- `<=` / `LTE`
- `IN`
- `EXISTS`
- `PREFIX`
- `CONTAINS`
- `FUZZY`

字段引用规则和 JSON DSL 一致：元字段用 `id/kind/source/...`，schemaless 字段可直接写字段名，冲突时使用 `fields.<name>`。

布尔组合支持 `AND`、`OR`、`NOT` 和括号。

## 子句

方向：

```sql
OUT
IN
BOTH
```

关系类型：

```sql
REL depends_on, runs_on
```

边字段过滤：

```sql
EDGE WHERE status = "active"
EDGE WHERE confidence >= 0.8 AND source = "manual"
```

路径过滤：

```sql
PATH NODES service, host, database
END KIND database
END WHERE engine = "mysql"
```

分步路径约束：

```sql
PATH
STEP REL depends_on NODE service
STEP REL runs_on NODE host EDGE WHERE status = "active"
END KIND host
```

投影：

```sql
PROJECT id, kind, hostname, fields.id, identity.provider
```

排序：

```sql
ORDER BY cpu DESC, hostname ASC
```

聚合：

```sql
AGG count(), count_by(region), avg(cpu) AS avg_cpu, max(cpu)
```

分组和 having：

```sql
GROUP BY owner, region
AGG count(), avg(cpu) AS avg_cpu
HAVING count >= 2 AND avg_cpu > 8
```

限制：

```sql
LIMIT 100
```

`LIMIT` 默认和 JSON DSL 一致为 100，最大 1000。

## 当前限制

- `MATCH` 只支持 1 到 8 步的有界模式；不支持无界重复、变量绑定、
  `OPTIONAL`、跨模式 join 或完整 Cypher/Gremlin 语法。
- 不支持子查询、join、表达式计算和用户自定义函数。
- 旧文本 DSL 只查询当前可见快照，不查询历史版本。
- `POST /v1/query/gql/stream` 支持 NDJSON 流式返回。带全局排序、聚合或路径结果时仍会先构建当前结果页或聚合结果；超大当前态导出仍优先使用 scan/export API。
