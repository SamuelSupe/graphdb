# GGraphDB 2.3.0 发布验证 / Release validation

## 当前状态 / Current status

候选源码正在发行验证，尚未声明标签或下载资产已通过核验。后续状态只更新本文，
不重写已发布的标签。精确标签工作流需要通过全量 unit/vet/race、SDK、单机 direct/WAL、
S3、TLS 网关、Raft、已发布 2.2.4 到本版的协议 1 滚动及两个三十分钟维护长测。

The candidate is undergoing qualification. Publication, exact-tag workflows and
downloaded assets are not yet certified. The final main record will report actual
conclusions without moving published tags.

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
These results do not substitute for the exact-tag GitHub qualification.

## 管理 UI / Management console

- 本机 Chrome 验证中英文、390px 窄屏、图表概览、结构化编辑和详情、草稿保护、查询／任务交互。
- 一万租户实际注册表样本：管理页默认 25 行、切换器 20 条、搜索末尾租户、翻页与每页 100 条通过；整页无窄屏横向溢出。
- 集群归属页面使用一万条模拟目录记录验证分页、搜索和双语窄屏；真实 Raft 分页由独立集成测试验证。
- 12 项原生 Node 控制台行为回归通过；JavaScript 语法、OpenAPI YAML 与差异检查通过。
- 先前 Go 1.25.0 的完整迁移测试两次在 Leader 重选阶段失败；Go 1.26.7 的同一测试重跑通过（33.338 秒）。不据此断言旧失败根因已修复。
- HTTP／路由与存储分页回归通过；真实 Raft 分页含租户隔离和归属代次请求头验证通过。

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
