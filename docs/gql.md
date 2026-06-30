# GQL 查询语言

GQL 是 GraphDB 的文本查询语言。它不是新的执行引擎，而是编译成现有 JSON Query DSL 后继续走同一套 planner、索引下推、lazy read、admission、timeout 和错误码。

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

流式 GQL：

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

- GQL 不支持 Cypher/Gremlin 图模式语法。
- 不支持子查询、join、表达式计算和用户自定义函数。
- GQL 只查询当前可见快照，不查询历史版本。
- `POST /v1/query/gql/stream` 支持 NDJSON 流式返回。带全局排序、聚合或路径结果时仍会先构建当前结果页或聚合结果；超大当前态导出仍优先使用 scan/export API。
