# 3.0 只读 openCypher 开发验证

日期：2026-11-11。分支：`codex/3.0-opencypher`。基线提交：
`75ec1e886b6693239627962224fdf64beea720ea`。本记录验证开发 worktree，
不构成正式版本发布或完整 openCypher 兼容声明。

Linux 检查在 OrbStack 的 `golang:1.27.3-bookworm` arm64 容器运行；
Python SDK 使用 `python:3.13-alpine`。本机日志位于
`.workflow/opencypher-3.0/`。

| 检查 | 结果 | 日志 |
| --- | --- | --- |
| 最终 Cypher 行为、HTTP、分区图、router 与 Go SDK 定向 race | 通过 | `current-race.log` |
| query、httpapi、partition、sharding、Go SDK 完整包 race | 通过 | `affected-race-final.log` |
| 全仓 `go vet -mod=readonly ./...` | 通过 | `current-vet.log` |
| 修复前全仓 Go 普通测试 | 仅 `cmd/graphdb` 的 `TestReceiptCheckCLI/damaged` 失败，其余包通过 | `go-test.log` |
| 修复前在同一 HEAD 的原始 checkout 复跑 `TestReceiptCheckCLI` | 同样失败，损坏夹具返回 `receipts_checked: 0` 与成功退出 | `baseline-receipt.log` |
| 解析器 fuzz | 初次 30 秒 82,642 次；最终 10 秒 33,565 次，均通过 | `fuzz.log`、`current-fuzz.log` |
| Python SDK 客户端测试 | 14 项通过 | `python-sdk.log` |
| Linux 服务进程 HTTP smoke | 11 项通过 | `smoke.json`、`server.log` |
| Parser 再生成 | lexer、parser 两个 Go 文件 SHA-256 均与生成前一致 | `parser-before.sha256` |
| OpenAPI YAML 与新请求/响应 schema 引用 | 通过静态检查 | — |

进程 smoke 使用新建的临时本地数据目录，通过现有 commit API 装载测试图。
验证服务健康、参数化关系查询、附加标签、OPTIONAL MATCH、聚合、整数精度、
有界最短路径、只读拒绝且图版本不变、读新鲜度、EXPLAIN/PROFILE、NDJSON
成功及预算耗尽终止帧。临时服务已清理。分区图快照测试在两组存储间装载跨分区
边，并在取得读租约后提交新版本；旧查询返回两个旧值，下一次查询返回两个新值。

## 回执检查复测

旧版 bbolt v1.4.3 在页结构检查协程 panic 时关闭结果通道，CLI 可能提前输出成功。
原用例重复 100 次，有 10 次把损坏库报告为成功。升级至 bbolt v1.5.0 后，页结构断言
通过检查通道返回错误，CLI 以状态 1 退出，stdout 为空，stderr 包含租户和页诊断。

回归用例先确认含一条实际回执的库可读，再修改回执桶之外的 bbolt 页 ID。
增强后的用例在旧依赖下 10 次均失败，其中 4 次出现成功退出。升级后所有
`TestReceiptCheck*` 用例重复 100 次均通过，回执检查与损坏隔离定向 race 通过。
使用真实服务生成 v1.4.3 回执库，新 CLI 读取后文件 SHA-256 一致。在同一库中用
v1.5.0 追加一条回执，两个版本的 CLI 均读到两条回执。

复测在 OrbStack 的 Linux arm64 容器执行。`go test -mod=readonly -p 4 ./... -count=1`
退出状态为 0，21 个测试包通过，另有 9 个包无测试文件。全仓 `go vet`、Python SDK
单元测试与真实服务端到端测试、控制台测试、备份与滚动恢复脚本测试均通过。
复测日志位于 `.workflow/receipt-check-3.0/`，全量 Go 测试见 `go-test.log`。

## 固定 TCK 子集

上游 `1.0.0-M23`，commit `007895aff5f33097d67b2e48a0a2babd6bd18590`。
从 expressions 与 WITH/RETURN/UNWIND/UNION 子句抽取空图/任意图场景，
展开 Scenario Outline，按列名及声明的行顺序比较上游结果。

| 分类 | 场景数 |
| --- | ---: |
| 提取总数 | 3103 |
| 通过 | 1113 |
| 语言特性未支持 | 1033 |
| 适配器排除：图夹具、错误类别、非只读或预期值格式等 | 956 |
| 命中资源限制 | 1 |
| 声明运行的场景失败 | 0 |

资源限制场景生成超出 100000 元素的 range。未支持及排除项不计为通过；完整
图夹具和错误类别适配仍未接入。图语义由独立用例验证，包括重复节点/关系规则、
自环、逆向扩展的关系顺序、所有等长最短路径、附加标签索引覆盖和租户隔离。

## 查询优化复测

解析采样显示，构建编译器语法树时会为每一层递归拼接文本。改为在编译时按需取文本，
并移除投影中未被读取的作用域。执行器只为 EXPLAIN 和 PROFILE 构建诊断计划。

基准在同一 OrbStack Linux arm64 容器顺序运行优化前后的二进制。20,000 实体的
本地物化图通过索引等值定位并返回一条属性值，`GOMAXPROCS=2`，每项取 7 次中位数。

| 路径 | µs/op（前 → 后） | B/op（前 → 后） | allocs/op（前 → 后） |
| --- | ---: | ---: | ---: |
| JSON DSL | 3.938 → 3.966 | 9032 → 9032 | 86 → 86 |
| 已编译 Cypher 执行 | 3.019 → 2.932 | 5448 → 5096 | 85 → 80 |
| 每次解析并执行 Cypher | 58.003 → 31.787 | 56993 → 48523 | 1542 → 738 |

前后逐项比较固定 TCK 子集，场景通过及跳过状态一致。新增回归用例覆盖带转义的
标识符、未起别名的表达式列名和 Unicode 文本。query、httpapi、partition、sharding
和 Go SDK 的 race 测试通过。解析器 fuzz 运行 30 秒，执行 45,399 次并通过。
真实服务进程验证了普通查询、EXPLAIN、PROFILE、`profile: true` 和 NDJSON 的计划与结果。
日志位于 `.workflow/cypher-optimize/`。
微基准的测量范围是进程内解析与执行，不含 HTTP 编解码、持久化对象读取和并发负载。

## 第二轮查询优化复测

编译器只为实际求值的表达式分配节点，同一父节点的语法子节点共用一个切片。
嵌套深度检查只读取单字符 token 的文本，HTTP 请求从原始字节解码。

同一 OrbStack Linux arm64 容器重跑上一轮的 20,000 实体等值查询微基准，
`GOMAXPROCS=2`，每项取 7 次中位数。

| 解析并执行 | 本轮优化前 | 本轮优化后 | 变化 |
| --- | ---: | ---: | ---: |
| µs/op | 32.230 | 30.231 | −6.2% |
| B/op | 48523 | 40060 | −17.4% |
| allocs/op | 738 | 547 | −25.9% |

真实 HTTP 压测通过 `/v1/query/cypher` 查询同一份 20,000 实体的本地数据，
预热后用 4 条长连接读取，每轮 2 秒，交替顺序跑 5 对，并验证每条响应的版本和结果。
吞吐中位数从 13,215.5 升到 13,305.5 QPS，p95 从 0.943 ms 降到 0.917 ms。
前后轮次的测量范围重叠。日志和本机压测脚本位于 `.workflow/cypher-optimize-2/`。

固定 TCK 子集的通过和跳过状态逐项一致。嵌套限制用例验证了第 33 层的错误位置，
以及字符串中的括号不计入深度。五个相关 Go 包的 race 测试和 30 秒解析器 fuzz 均通过。

## 正确性复查

对编译、聚合、模式匹配和持久化点查添加行为回归，先记录失败，再验证修复。
日志位于 `.workflow/cypher-correctness/`。

| 已复现问题 | 修复后的行为 |
| --- | --- |
| `graphdb.id` 点查把缺失的索引页当成节点不存在 | 缺页返回 `index_stale`，NDJSON 发送失败结束帧。普通页和范围页均覆盖 |
| 重复标签与等值索引组合产生重复行 | 同一节点只生成一次候选 |
| 聚合忽略模式表达式的外层变量，列表推导局部变量参与分组 | 按实际外层依赖分组，空输入上的常量表达式与 `count(*)` 返回一行 |
| 聚合排序把不同模式视为等价 | 包含模式表达式的聚合通过投影别名排序，未投影的不同聚合报错 |
| `toInteger('-9223372036854775809')` 返回最小整数 | 越界整数字符串返回 null，科学计数法转换继续按其浮点值处理 |
| 普通 Map 的 `keys()` 和 `properties()` 删除用户同名键 | 普通 Map 保留 `__graphdb_labels`，节点和关系仍按图属性契约输出 |
| 混用 UNION 模式、包含 NUL 的不同列名被接受 | 校验统一 UNION 模式，逐列比较列名 |

固定 TCK 子集保持 1,113 个场景通过，其余场景的统计与前述记录一致。
OrbStack Linux arm64 上的全仓 `go test -mod=readonly -p 4 ./... -count=1`
退出状态为 0，21 个测试包通过，另有 9 个包无测试文件。相关六个包的定向 race、
全仓 `go vet` 和 30 秒解析器 fuzz 通过。真实 Linux 服务验证 JSON/NDJSON
结果、非法组合的 HTTP 422 和查询前后的版本一致性。全量结果见 `go-test.log`，
回归初始失败见 `before.log`、`pattern-order-before.log` 和 `value-before.log`。

## 第二轮正确性复查

| 已复现问题 | 修复后的行为 |
| --- | --- |
| 反向遍历提前计算引用外层节点的模式属性，多返回一行 | 根据属性表达式的外层依赖选择遍历方向 |
| DISTINCT 后误拒绝表达式局部变量，或在模式中接受已离开作用域的变量 | 保留局部绑定，校验模式和属性中的外层引用，并支持用已投影的算术表达式排序 |
| 聚合排序把整数与浮点除法视为同一个表达式 | 按字面量类型区分排序键 |
| `split()`、列表拼接和 Go 的字符串切片参数绕过列表大小限制 | 拒绝超过 100,000 项的列表，恰好达到上限的列表仍可使用 |
| Go SDK 复用接收对象时，结束帧残留上一行 | 每个成功读取的流帧独立解码 |

在 OrbStack Linux arm64 上，查询、HTTP API、分区、分片和 Go SDK 的全包 race 测试
及全仓 `go vet` 通过。固定 TCK 子集的 1,113 个通过项和其余场景状态与上一轮逐项一致。
实际服务验证了相关遍历、作用域和排序的 JSON/NDJSON 结果、非法绑定的 HTTP 422，
以及列表超限的 HTTP 429 和 NDJSON 失败结束帧。本轮未重跑全仓单元测试。
初始失败和修复后日志位于 `.workflow/cypher-correctness-2/`。

## 重现

在上述 Linux/Go 环境中执行：

```sh
go test -mod=readonly -p 4 ./... -count=1
go test -mod=readonly ./cmd/graphdb -run '^TestReceiptCheck' -count=100
go test -mod=readonly -run TestCypherTCKReadOnly -count=1 -v ./internal/query
go test -mod=readonly -race -p 2 ./internal/query ./internal/httpapi ./internal/partition ./internal/sharding ./sdk/go/graphdb
go vet -mod=readonly ./...
go test -mod=readonly ./internal/query -run '^$' -fuzz FuzzParseCypher -fuzztime=30s -parallel=2
GOMAXPROCS=2 go test -mod=readonly ./internal/query -run '^$' -bench BenchmarkReadOnlyCypher -benchmem -count=7 -benchtime=300ms
python3 -m unittest discover -s sdk/python/tests -p test_client.py -v
```

查询兼容范围、协议及资源上限见[只读 openCypher 查询](opencypher.zh-CN.md)。
