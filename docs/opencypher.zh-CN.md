# 3.0 开发分支：只读 openCypher

状态：开发实现，尚非 3.0 正式发行资格声明。当前 HTTP `/v1` 与 Go module
`/v2` 合同保持独立版本，不因本功能自动升级。

本实现以 openCypher 9、grammar/TCK `1.0.0-M23` 为固定基线，提供有界的只读
查询 profile，不宣称完整 openCypher、Neo4j 或 Bolt 兼容。写入子句在 AST
检查阶段拒绝，包括 EXPLAIN 和嵌套 EXISTS 内的更新；执行器仅调用读取接口。

## 请求与结果

入口为 `POST /v1/query/cypher` 和 `POST /v1/query/cypher/stream`。两者均使用
现有认证、`X-Tenant-ID`、读新鲜度、并发准入和取消机制。分区图通过 router
接入相同执行器，复用现有分布式读取租约和分区请求/字节预算。

```sh
curl -sS "$READER/v1/query/cypher" \
  -H 'Content-Type: application/json' -H 'X-Tenant-ID: demo' \
  --data-binary @examples/query-cypher.json
```

```json
{
  "query": "MATCH (n:host) WHERE n.hostname = $name RETURN n.hostname AS hostname LIMIT $limit",
  "parameters": {"name": "app-01", "limit": 100},
  "min_version": 123,
  "allow_stale": false,
  "timeout_ms": 3000,
  "cost_limit": 100000,
  "profile": false
}
```

参数是独立的值，不进行文本拼接；JSON 整数参数保持 signed int64 精度，越界
返回错误。查询观察整条语句执行时的一份快照；`min_version` 是最低新鲜度，
不是历史版本选择。已有持久化 float64 值继续作为浮点值读取，无法恢复其过去
已经丢失的整数精度。`timeout_ms` 未设置时沿用请求上下文与成本预算。

```json
{"version":123,"columns":["hostname"],"rows":[["app-01"]],"stats":{"scanned":1,"visited":0,"returned":1,"cost":8}}
```

上面的 stats 仅为结构示例。节点使用 `{"$type":"node","id":...,"labels":[],
"properties":{}}`；关系使用 `$type=relationship` 并带 `type/from/to`；路径
使用 `$type=path` 并带有序 `nodes` 与 `relationships`。

本地/普通租户查询注册到现有 running-query registry，响应带
`X-GraphDB-Query-ID`。分区图沿用其现有请求期限和连接取消机制，不提供新的
跨进程 running-query registry。

语法、语义、参数和类型错误返回 HTTP 422、外层 `invalid_query`，`detail`
包含 `code/message/line/column`。细分 code 包括 `cypher_syntax_error`、
`cypher_semantic_error`、`cypher_parameter_missing`、`cypher_type_error`、
`cypher_unsupported`、`cypher_read_only`。成本、内存或结果上限返回 429。

## 支持矩阵

| 能力 | 当前实现 |
| --- | --- |
| 模式 | 单/多个 MATCH、逗号模式、OPTIONAL MATCH、共享变量、节点/关系/路径绑定 |
| 标签 | 节点模式、多标签 AND、表达式标签谓词，区分大小写 |
| 路径 | 固定长度、显式有界变长路径，单个模式最大总长度 16，允许零长度 |
| 最短路径 | 单独 MATCH 中的 shortestPath/allShortestPaths；两端先绑定；恰好一段显式有界变长关系 |
| 过滤 | 比较、AND/OR/XOR/NOT、IS NULL/IS NOT NULL、IN、STARTS WITH/ENDS WITH/CONTAINS |
| 流水线 | WITH、UNWIND、RETURN、别名、星号、DISTINCT、ORDER BY、SKIP/LIMIT、UNION/UNION ALL。同一语句使用一种 UNION 模式 |
| 聚合 | count、sum、avg、min、max、collect，含 DISTINCT 和隐式分组 |
| 表达式 | 参数、算术、CASE、列表/Map、属性访问、下标、切片、列表推导与 all/any/none/single |
| 模式表达式 | 模式谓词、模式列表推导、`EXISTS { pattern [WHERE ...] }` |
| 诊断 | EXPLAIN、PROFILE，物理访问路径 trace、语句成本与 operator profile |
| 过程与子查询 | 不支持 CALL；EXISTS 内的 RegularQuery、通用子查询暂不支持 |
| 扩展边界 | 只接收有限数值；不支持 temporal/spatial 类型、NaN/Infinity 及未列出的函数 |

内置函数白名单：`coalesce`、`labels`、`type`、`properties`、`keys`、`size`、
`length`、`head`、`last`、`reverse`、`nodes`、`relationships`、`startNode`、
`endNode`、`toString`、`toInteger`、`toFloat`、`toBoolean`、`abs`、`ceil`、
`ceiling`、`floor`、`round`、`sign`、`range`、`toLower`、`toUpper`、`trim`、
`ltrim`、`rtrim`、`split`、`replace`、`substring`、`left`、`right`、`exists`，
以及扩展函数 `graphdb.id`。`toInteger()` 接收越界的整数字符串时返回 null。

## 图模型与语义

- Node 对应 Entity；可见标签为 `kind` 与现有 `labels` 的去重并集，不自动
  展开类型继承。任何标签匹配都必须考虑两种来源。
- Relationship 对应 Edge，类型为 `Edge.Type`，方向按 From→To 解释；无向
  模式匹配两侧，自环只枚举一次。
- `n.foo`/`r.foo` 读取业务 Fields，不自动映射系统元字段；内部 labels 保留
  字段不作为普通属性暴露。普通 Map 的 `keys()` 和 `properties()` 保留所有用户键。
  嵌套 JSON 对象作为 Map 值读取，是存储映射扩展。
- `graphdb.id(n)` 返回字符串内部 ID；`n.id` 是业务属性。标准整数 `id()`
  暂不支持。现有同端点同类型边仍按规范身份合并，查询不改变该模型。
- 缺失属性与显式空值读取为 null；WHERE 只保留 true。字符串谓词区分大小写，
  不沿用旧 DSL 的宽松转换。不同类型比较遵循查询 profile 的空值语义。
- 默认保留重复行；DISTINCT 才去重。聚合使用完整输入，随后应用结果分页。
  同一个 MATCH 限制关系重复使用，多个 MATCH 分别管理关系唯一性。
- DISTINCT 或聚合之后，ORDER BY 和 WITH 的 WHERE 可使用已投影的值。
  推导式和模式表达式的局部绑定在各自的表达式内有效。
  包含模式表达式的聚合用投影别名排序。
- SKIP/LIMIT 是查询运算，LIMIT 0 返回零行。服务端资源限制不会隐式追加 LIMIT。

## 执行与资源边界

语句共享快照、索引目录、成本预算和 deadline。复用字段索引、双向邻接与按需
字段读取；索引覆盖的 kind 先产生候选，其余 kind 仍需扫描，保证附加标签
匹配完整。可以从模式另一端选择起点；命名路径保持源查询中的节点顺序。
索引点查遇到缺失的候选页时，普通查询返回 `index_stale`，流式查询发送带相同错误码的失败结束帧。
索引执行失败后不会切换到另一个版本重跑。

单分支且无排序/聚合/去重时逐行执行，可以提前停止 LIMIT 并传播输出背压。
阻塞算子和 UNION 使用有界缓冲；目前排序缓冲全部候选，不承诺 Top-K 或
磁盘 spill。查询可能因预算不足失败，不能将该结果视为完整聚合。

| 限制 | 值 |
| --- | ---: |
| 查询文本 | 64 KiB |
| token / 结构嵌套 / 变量槽位 | 8192 / 32 / 1024 |
| UNION 分支 / 每分支子句 | 32 / 64 |
| 成本默认 / 最大 | 100000 / 1000000 |
| 执行器累计分配预算 | 64 MiB，保守计算，包含行、值、输出与中间结构 |
| 中间缓冲行 / 列表或 range 元素 / 流式输出行 | 100000 |
| 聚合分组 | 10000 |
| JSON 输出行 | 1000，超限报错 |

列表元素上限也适用于参数列表、列表拼接、`split()` 和 `keys()` 的结果。

流式协议依次为 meta、零到多行、terminal：

```json
{"stream":true,"version":123,"columns":["hostname"]}
{"row":["app-01"]}
{"done":true,"success":true,"version":123,"stats":{}}
```

执行失败发送 `{"done":true,"success":false,"error":{...}}`。断开连接或
写入期限耗尽可能导致缺少 terminal；客户端必须确认 `done=true` 且
`success=true`。HTTP 200 和已经收到的部分行均不代表查询成功。

Go SDK 提供 `Cypher`/`CypherStream`；动态数值单元格使用 json.Number，
每次成功调用 `Next` 都用当前流帧替换接收对象的内容。
Python SDK 提供 `cypher`/`stream_cypher`。消费流到 EOF 时，两个 SDK 都会
检查完成标记与失败 terminal；提前 Close 属于取消。

## 验证与兼容声明

`internal/query/testdata/cypher-tck.json` 固定上游 M23 revision，并保留场景来源。
它覆盖空图表达式及部分只读子句场景，不是完整 TCK。测试分别报告通过、
不支持、适配器排除和资源限制，不把跳过计入通过；图夹具和错误分类的完整
TCK 接入尚未完成。独立行为测试覆盖真实图绑定、路径、两个读路径、只读
拒绝、流式终止、跨分区并发提交时的快照一致性。

重现命令及本次实际结果见[开发验证记录](opencypher-validation.zh-CN.md)。
