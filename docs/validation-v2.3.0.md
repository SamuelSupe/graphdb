# GGraphDB 2.3.0 发布验证 / Release validation

## 当前状态 / Current status

**PUBLISHED_AND_VERIFIED**：[v2.3.0](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.0)
于 2026-10-11 08:03:59 UTC 发布为稳定 Latest，非草稿、非预发行，无门禁豁免。
标签指向 `91ffe5033e58d49ca38ef9dbb40efd414264538e`；标签工作流
[38120462949](https://github.com/SamuelSupe/graphdb/actions/runs/38120462949) 全部成功。
本记录在发布包核验后更新于 main，不移动标签；包内保留发行前的验证状态记录。

Published as stable Latest, with every exact-tag gate passing and no waiver.
The release source is the commit above. This post-publication main record does
not rewrite the tag or the pre-publication record embedded in the archive.

发行后的文档／官网提交 `692ac94009ed971eccef74f5ffa0c10a63ef3e33` 的
[CI](https://github.com/SamuelSupe/graphdb/actions/runs/38123988348) 和
[Pages 构建／部署](https://github.com/SamuelSupe/graphdb/actions/runs/38123988172)
共 8 项检查全部成功。官网已显示 2.3.0；此文档提交不替换发行二进制或标签。

The post-publication documentation/site commit passed all eight CI and Pages
checks. The live website shows 2.3.0; release binaries and the tag remain unchanged.

## GitHub 发行门禁 / Exact-tag qualification

| 范围 / Scope | 结果 / Result |
| --- | --- |
| 全量 unit / vet / race、版本冻结、Go/Python SDK、12 项 Node 控制台回归、11 项备份 worker 回归 | PASS |
| 单机 direct/WAL HTTP、负载、备份恢复、快照重启一致性 | PASS |
| MinIO/S3 备份与真实 TLS 网关 | PASS |
| Raft 协议 1、2、3，磁盘、恢复、流式、维护与租户归属分页 | PASS |
| 已发布 v2.2.4 到本版的协议 1 滚动 | PASS，两端二进制已与各自发布包绑定 |
| 单机及 Raft 各 30 分钟维护长测 | PASS，见下表中的实际边界 |
| 三平台打包、解包、配置及容器启动 | PASS |

发行提交的 main CI
[38120462973](https://github.com/SamuelSupe/graphdb/actions/runs/38120462973)
第 2 次运行成功。第 1 次在 race 测试
`TestRestoreDrillCleanupFailureFailsTaskAndRetriesCleanup` 的 2 秒任务等待处超时
（2.11 秒），不隐去原始失败。同一测试在 OrbStack Go 1.26.7 下分别以默认设置和
`GOMAXPROCS=2` 连续运行 30 次均通过；未修改代码、断言或超时再重跑 CI。
这不能证明首次失败根因已经修复；2.2.4 记录过同类失败。

Main CI passed on attempt 2. Attempt 1 timed out while waiting for restore-drill
cleanup under the race detector. Two unchanged 30-repeat runs passed locally;
the cause of the original timeout remains unproven.

## 长测结果 / Maintenance soaks

| 指标 / Metric | 单机 / Standalone | Raft 协议 3 / Protocol 3 |
| --- | --- | --- |
| 持续时间 / Duration | 30m0s | 30m0s |
| 操作数 / Operations | 130,126 | 66,222 |
| 操作错误 / Operation errors | 0 | 0 |
| Reader 样本 / Samples | 59，未就绪 0，最大版本落后 0 | 59，未就绪 0，最大版本落后 0 |
| Index 样本 / Samples | 59，unhealthy 0，stale 31，最后 stale | 59，unhealthy 0，stale 0，最后 ready |
| compact / GC / index rebuild | 5 / 2 / 2 | 5 / 2 / 2 |
| ingest P99 | 17,200 ms | 9,500 ms |

操作数包含查询、控制、写入和维护，不是 QPS 或容量结论。单机索引并非始终 ready，
维护仍有明显长尾，不承诺低延迟 SLO。历史性能候选的四次入口 503 仍为 FAIL，
本轮通过不能改写其结论。原始记录位于发行包 `release/evidence/local-gate/`
和 `release/evidence/raft-gate/protocol3/dual/`。

Operation totals mix query, control, ingestion and maintenance. They do not
establish QPS, production capacity or a latency SLO. Standalone indexes had stale
samples; the earlier performance candidate's four 503s remain a historical FAIL.

## 发布包核验 / Downloaded artifact verification

从 GitHub 重新下载两个资产并核对 API 的大小、摘要、外部及包内 SHA256：

| 资产 / Asset | SHA256 |
| --- | --- |
| `graphdb-v2.3.0.tar.gz`（57,560,379 bytes） | `8fca98251d098010ec7bdc12049de3eadc38a472cc7c70eb4270529cb64c3a9a` |
| `bin/graphdb-darwin-arm64` | `f0ce31d82f2d4d5cc1f8bbb17d5ce2375a4530dee88ac09d55fd7e75d08bff24` |
| `bin/graphdb-linux-amd64` | `18ee1a1574decd40cb0d82841406230b4e9d695d04ebbf94525e64d0dd4e0307` |
| `bin/graphdb-linux-arm64` | `236411ec0a272f023c1b4be46248dcb8673df72657c0ea1c55a9b174dcf30499` |

构建身份为 v2.3.0 / 上述提交 / Go 1.26.7，构建时间
`2026-10-11T15:10:05+08:00`。包内 1,030 个标签跟踪文件逐字节一致，必需源码、
SDK、工具与文档齐全。amd64 摘要与 Raft 三协议、滚动和网关的 PASS metadata
一致；滚动来源与独立下载的 v2.2.4 二进制摘要
`224df57d837aa6cd475881681c286bfae1c2670da245c24a5399820ec22fb86a` 一致。

下载的 macOS arm64 在本机、Linux arm64 在 OrbStack 分别执行 direct 和 WAL：
真实服务 Python SDK 各 12 项无跳过、HTTP E2E（含备份恢复）、4 writer / 16 reader
负载、重启前后快照完全一致及内嵌 `/ui/` 返回均 PASS。amd64 的运行证据来自
GitHub Linux 门禁，未在本机模拟器重复做完整运行验证。

Downloaded checksums, build identities, 1,030 tagged files and qualified binary
bindings passed. Downloaded ARM binaries also passed direct/WAL SDK, HTTP E2E,
load and snapshot-restart checks on native macOS and OrbStack Linux. amd64 runtime
qualification comes from GitHub Linux gates, without a local emulated rerun.

## 本机发行准备 / Local preflight

Go 1.26.7、OrbStack Linux arm64 的完整 `RELEASE_GATE_VERIFY_ONLY=1` 门禁通过：
全仓 unit、vet、race、版本冻结与 SDK 检查 PASS。独立 direct/WAL HTTP 集成、负载、
快照重启一致性及备份恢复通过；真实服务的 Python SDK 12 项检查无跳过，
独立备份 worker 11 项回归通过。无真实服务的 SDK 单测运行明确跳过 E2E，
该场景已由真实 HTTP 门禁补充，不将跳过标为通过。

The full Go 1.26.7 Linux arm64 unit/vet/race/version gate passed locally. Separate
direct/WAL HTTP integration, load, recovery and restart checks passed. Real-server
Python SDK checks ran 12 tests without skips; 11 backup-worker regressions passed.
The SDK-only run skips E2E when no server is configured; real HTTP testing covers it.
The exact-tag GitHub qualification subsequently passed as recorded above.

## 管理 UI / Management console

- 本机 Chrome 验证中英文、390px 窄屏、图表概览、结构化编辑和详情、草稿保护、查询／任务交互。
- 一万租户实际注册表样本：管理页默认 25 行、切换器 20 条、搜索末尾租户、翻页与每页 100 条通过；整页无窄屏横向溢出。
- 集群归属页面使用一万条模拟目录记录验证分页、搜索和双语窄屏；真实 Raft 分页由独立集成测试验证。
- 12 项原生 Node 控制台行为回归通过；JavaScript 语法、OpenAPI YAML 与差异检查通过。
- 先前 Go 1.25.0 的完整迁移测试两次在 Leader 重选阶段失败；Go 1.26.7 的同一测试重跑通过（33.338 秒）。不据此断言旧失败根因已修复。
- HTTP／路由与存储分页回归通过；真实 Raft 分页含租户隔离和归属代次请求头验证通过。
- 发布后使用下载的 Linux arm64 二进制在本机 Chrome 复验：图表概览、中英文切换、390px 导航和租户 ID 搜索通过；文档宽度 390px，无框架错误、控制台 warn/error。

Native Chrome validates bilingual/responsive interaction with 10,000 tenant
fixtures. Cluster UI uses a simulated 10,000-placement catalog; dedicated
integration tests cover real Raft ownership headers and paging. Earlier Go 1.25.0
migration election failures remain recorded; an unchanged Go 1.26.7 run passed,
without establishing the original failure's root cause.

## 边界 / Boundaries

ID 注册表／集群目录仍整体读取；详情 I/O 和浏览器渲染受页大小约束。搜索匹配 ID，
不匹配名称。历史租户发现会扫描存储对象。分页读取实时数据，不提供快照隔离。
浏览器优化不等于数据库吞吐、生产容量或跨宿主机稳定性认证。

The registry/catalog still reads in full; pages bound detail I/O and rendering.
Search matches IDs, legacy discovery scans objects and pages are live. Frontend
optimizations do not certify database throughput, production capacity, cross-host
or day-scale reliability. Standalone retains 2.0/2.1 formats; Raft roles/protocols
and the documented rolling qualification window still apply.
