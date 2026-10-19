# GGraphDB 2.3.2 Documentation

[简体中文](README.zh-CN.md) · [Project home](../README.md) · [Download](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.2)

The release candidate is **2.3.2**; see its validation record for final status. Choose a task below or follow a suggested reading path.
User guides and console documentation have English and Simplified Chinese versions;
some cluster operations guides are currently Chinese-only.

## Suggested reading paths

| Your goal | Suggested order |
| --- | --- |
| Try the database | [Quick start](user/quickstart.md) → [Console](management-ui.md) → [Usage manual](user/usage-manual.md) |
| Integrate business data | [Data model](user/data-model.md) → [Ingestion](user/write-ingest.md) → [Queries](user/read-query.md) → [SDKs](user/sdk.md) |
| Prepare a deployment | [Release deployment](user/release-deployment.md) → [Security](security-deployment.md) → [Backup/restore](object-backup.md) → [Validation](validation-v2.3.2.md) |

## User guides

### Get started

| Task | English guide | 中文 |
| --- | --- | --- |
| Run a first example | [Quick start](user/quickstart.md) | [快速开始](user/quickstart.zh-CN.md) |
| Install a published binary | [Release deployment](user/release-deployment.md) | [发行版部署](user/release-deployment.zh-CN.md) |
| Explore in the browser | [Management console](management-ui.md) | [管理控制台](management-ui.zh-CN.md) |
| Follow the full workflow | [Usage manual](user/usage-manual.md) | [使用手册](user/usage-manual.zh-CN.md) |
| Find API conventions | [User guide](user/README.md) | [用户指南](user/README.zh-CN.md) |

### Model, ingest and query

| Task | English guide | 中文 |
| --- | --- | --- |
| Design entities and relationships | [Data model](user/data-model.md) | [数据模型](user/data-model.zh-CN.md) |
| Write commits and collector batches | [Write and ingest](user/write-ingest.md) | [写入与采集](user/write-ingest.zh-CN.md) |
| Read, traverse and aggregate | [Read and query](user/read-query.md) | [读取与查询](user/read-query.zh-CN.md) |
| Scan or export data | [Scan and export](user/scan-export.md) | [扫描与导出](user/scan-export.zh-CN.md) |
| Build a Go or Python client | [SDK guide](user/sdk.md) | [SDK 指南](user/sdk.zh-CN.md) |

### Operate and recover

| Task | English guide | 中文 |
| --- | --- | --- |
| Configure tenant lifecycle and paging | [Tenants and configuration](user/tenant-config.md) | [租户与配置](user/tenant-config.zh-CN.md) |
| Plan service operations | [Deployment and operations](user/deploy-ops.md) | [部署与运维](user/deploy-ops.zh-CN.md) |
| Manage tasks and maintenance | [Tasks and maintenance](user/tasks-maintenance.md) | [任务与维护](user/tasks-maintenance.zh-CN.md) |
| Diagnose API failures | [Troubleshooting](user/errors-troubleshooting.md) | [故障排查](user/errors-troubleshooting.zh-CN.md) |
| Understand disk durability | [Local disk guide](local-disk.md) | [本地磁盘指南](local-disk.zh-CN.md) |
| Back up and restore snapshots | [Object backups](object-backup.md) | [对象备份](object-backup.zh-CN.md) |
| Schedule standalone or Raft backups | [Backup automation](backup-automation.md) | [自动备份](backup-automation.zh-CN.md) |

## Cluster and production operations

| Task | Documentation |
| --- | --- |
| Configure replicas or shards | [Three-replica Raft](raft-ha.zh-CN.md) · [Tenant sharding and migration](sharding.zh-CN.md) |
| Operate or upgrade a cluster | [Raft operations](raft-operations.zh-CN.md) · [Rolling upgrade window](raft-rolling-upgrade.zh-CN.md) |
| Protect and diagnose the service | [Production security](security-deployment.md) · [Product operations](product-operations.zh-CN.md) · [Diagnostic metrics](diagnostics-metrics.zh-CN.md) |
| Check deployment qualification | [Release and deployment readiness](raft-release-readiness.zh-CN.md) |

## Reference documents

| Topic | Documentation |
| --- | --- |
| Positioning and architecture | [Database introduction](database-introduction.md) · [中文](database-introduction.zh-CN.md) · [Architecture](architecture.en.md) |
| API contracts | [API map](user/api-map.md) · [中文](user/api-map.zh-CN.md) · [OpenAPI](openapi.yaml) · [Error codes](error_codes.md) |
| Query languages | [Query capabilities](query_capabilities.md) · [GraphQL](graphql.md) · [中文](graphql.zh-CN.md) · [Legacy text DSL](gql.md) |
| Versions and gaps | [Naming and compatibility](naming-and-compatibility.md) · [中文](naming-and-compatibility.zh-CN.md) · [Product gaps](product_function_gaps.md) |
| 2.3.2 release evidence | [Release notes](../release/local-disk.md) · [Validation](validation-v2.3.2.md) · [Machine-readable JSON](validation-v2.3.2.json) |

SDK/OpenAPI versions are 2.3.2. Module `/v2`, HTTP `/v1` and separate Raft protocol
identifiers follow their own contracts; do not substitute the product version.

## Historical designs and validation

<details>
<summary>Browse historical designs, reviews and validation</summary>

These records retain their own dates, builds and PASS/FAIL/skipped/unpublished
conclusions. [2.3.2 validation](validation-v2.3.2.md) qualifies the current release;
historical performance measurements do not establish current production capacity.

- [Product P0/P1 review after 2.2.2](product-p0-p1-review-2026-10-03.zh-CN.md)
- [Follow-up review: missing heads, graph digests and source identities](product-p0-p1-review2-2026-10-03.zh-CN.md)
- [Follow-up review: migration validation and atomic local replacement](product-p0-p1-review3-2026-10-03.zh-CN.md)
- [Follow-up review: migration chunks, Raft snapshots and data prefixes](product-p0-p1-review4-2026-10-03.zh-CN.md)
- [Follow-up review: runtime restore destinations and auxiliary directory ownership](product-p0-p1-review5-2026-10-03.zh-CN.md)
- [Full test and fixes for the unpublished candidate](full-validation-2026-10-03.zh-CN.md)
- [Availability and rolling-upgrade fixes for the unpublished candidate](availability-rolling-validation-2026-10-04.zh-CN.md)
- [Three-replica Raft 30-minute soak rerun: FAIL](raft-long-validation-2026-10-04.zh-CN.md)
- [Raft availability fixes and 30-minute soak: PASS](raft-availability-fixes-2026-10-04.zh-CN.md)
- [Raft snapshot and maintenance transfer optimization and qualification](performance-raft-batching-2026-10-04.zh-CN.md)
- [2.2.4 release verification](validation-v2.2.4.md) · [JSON](validation-v2.2.4.json)
- [Historical 2.1.2 write-tail measurements](performance-write-tail.md)
- [Deep backup fault tests and fixes](backup-deep-validation-2026-10-04.zh-CN.md)
- [Historical local disk v2 validation and focused benchmark (Chinese)](performance-local-disk-v2.md)
- [Historical local disk performance: pagination, JSON encoding and Parquet layout (Chinese)](performance-local-disk-optimization-2.md)
- [Historical HA design](high-availability-design.zh-CN.md) · [HA validation](raft-ha-validation.zh-CN.md) · [Sharding validation](sharding-validation.zh-CN.md)

</details>
