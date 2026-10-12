# GGraphDB v2.3.0

## 中文

本版新增内置中英文管理控制台。通过管理或组合监听器的 `/ui/` 访问；路由入口也支持。
控制台覆盖图数据、查询、模型、租户、索引、任务与备份、接入、治理和集群运维。
概览采用图表，详情和配置采用结构化展示与有类型的编辑器，原始 JSON 按需展开。
支持独立查询草稿、未完成编辑保护、键盘标签切换、可滚动对话框和窄屏布局。
运行时无需 Node、npm 或独立前端服务；沿用部署网关的认证与授权，令牌仅保存在页面内存。

大量租户场景使用服务端 ID 搜索和分页。切换器每次加载 20 条，管理表格默认 25 条，
集群归属也按页加载。只读取当前页的租户详情，历史对象扫描改为手动启用。
Go/Python SDK 增加对应分页能力；不传分页参数的 HTTP 请求与原 SDK 全量方法保持兼容。
ID 注册表／集群目录仍整体读取，本次优化不表示注册表可无限扩展。

页面性能优化延迟展开详情与原始数据，合并字段验证，减少重复列表请求和隐藏结果渲染。
清理已失效的历史代码及 CI 门禁，不更改 Parquet/WAL 数据格式、Raft 协议、多数派或 fsync 契约。

版本、SDK 和 OpenAPI 为 2.3.0；Go 模块 `/v2`、HTTP `/v1` 不变。
单机升级先备份、停止旧进程，再以相同数据目录和前缀启动。Raft 升级遵循
[滚动资格窗口](https://github.com/SamuelSupe/graphdb/blob/v2.3.0/docs/raft-rolling-upgrade.zh-CN.md)。
协议激活和混部限制仍适用，不提供 1.x 迁移或任意版本降级保证。

本版已发布为稳定 Latest，确切标签的全量 unit/vet/race、SDK、direct/WAL、S3、TLS 网关、Raft、
已发布 2.2.4 到本版的协议 1 滚动和两个三十分钟维护长测全部通过，无门禁豁免。
下载包、包内二进制及标签源码核验通过。门禁证据在压缩包的 `release/evidence/`，
发布与下载验证见 [2.3.0 验证](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.0.md)。
本机 Chrome 已验证双语、390px 和一万租户样本。浏览器局部性能收益不代表数据库吞吐收益；
跨宿主机、天级稳定性和生产容量仍未认证。历史失败记录不因本版发行而变为通过。

## English

This release embeds an English / Simplified Chinese management console at `/ui/`
on the management/combined listener and sharding router. It covers graph data,
queries, schemas, tenants, indexes, tasks/backups, ingestion, governance and cluster
operations. Charts replace overview lists; typed editors and structured details
retain optional raw JSON. Draft protection, keyboard tabs, scrollable dialogs and
responsive layouts improve routine operation. No Node, npm or separate frontend
service is needed at runtime. Existing gateway authorization remains in force;
bearer tokens stay in page memory.

Server-side tenant ID search and cursor pages bound switching, tenant tables and
cluster placements. The picker loads 20 matches; the tenant table defaults to 25.
Only page metadata/manifests are loaded; legacy prefix scans are opt-in. Go/Python
SDKs support paging while original unpaged requests remain compatible. The complete
ID registry/catalog is still read: this is not an unlimited registry-capacity claim.

Collapsed details render on demand, input validation is coalesced, and redundant
list requests/hidden query rendering are avoided. Obsolete source helpers and CI
jobs are removed. Parquet/WAL formats, Raft protocols, majority and fsync contracts
remain unchanged. SDK/OpenAPI are 2.3.0; module `/v2` and HTTP `/v1` remain unchanged.

The stable Latest release passed exact-tag unit/vet/race, SDK, standalone direct/WAL,
S3, TLS gateway, Raft, protocol-1 rolling from published v2.2.4 and two 30-minute
maintenance soak gates without a waiver. Downloaded archives, binaries and tagged
source files were verified. Evidence is packaged under `release/evidence/`; the
main validation record reports publication/download results. Native Chrome checks
cover both languages, 390px and 10,000 fixture tenants.
Frontend measurements do not establish database throughput, production capacity,
cross-host or day-scale reliability. Earlier failures remain recorded.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.0.tar.gz.sha256
tar -xzf graphdb-v2.3.0.tar.gz
cd v2.3.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, console sources, matching
SDKs/OpenAPI, bilingual guides, deployment/backup examples, build identity and gate
evidence. No published container image or production fault-domain certification
is implied.
