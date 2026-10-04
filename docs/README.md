# GGraphDB 2.2.3 Documentation

[中文](README.zh-CN.md)

User-facing guides live under [user/](user/README.md). They describe how to
start, write, query, deploy, and operate GGraphDB through the API and CLI.
Every user guide has an English default file and a matching `.zh-CN.md` file
with a language-switch link at the top.

- [三副本 Raft 高可用运行说明](raft-ha.zh-CN.md) · [设计](high-availability-design.zh-CN.md) · [验收记录](raft-ha-validation.zh-CN.md)

- [Tenant sharding and migration](sharding.zh-CN.md) · [Raft operations](raft-operations.zh-CN.md) · [Diagnostic metrics](diagnostics-metrics.zh-CN.md)
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

## User guides

- [2.2.3 release notes](../release/local-disk.md) · [Validation](validation-v2.2.3.md) · [JSON](validation-v2.2.3.json) · [Historical 2.1.2 write-tail measurements](performance-write-tail.md)

- [User Guide](user/README.md) · [中文](user/README.zh-CN.md)
- [Local disk deployment and durability](local-disk.md) · [中文](local-disk.zh-CN.md)
- [Object-storage snapshots, automation and restore](object-backup.md) · [中文](object-backup.zh-CN.md)
- [Standalone and Raft backup automation](backup-automation.md) · [中文](backup-automation.zh-CN.md)
- [Deep backup fault tests and fixes](backup-deep-validation-2026-10-04.zh-CN.md)
- [Historical local disk v2 validation and focused benchmark (Chinese)](performance-local-disk-v2.md)
- [Historical local disk performance: pagination, JSON encoding and Parquet layout (Chinese)](performance-local-disk-optimization-2.md)
- [Quick Start](user/quickstart.md) · [中文](user/quickstart.zh-CN.md)
- [Usage Manual](user/usage-manual.md) · [中文](user/usage-manual.zh-CN.md)
- [Release Deployment](user/release-deployment.md) · [中文](user/release-deployment.zh-CN.md)
- [Deployment And Operations](user/deploy-ops.md) · [中文](user/deploy-ops.zh-CN.md)
- [Data Model](user/data-model.md) · [中文](user/data-model.zh-CN.md)
- [Write And Ingest](user/write-ingest.md) · [中文](user/write-ingest.zh-CN.md)
- [Read And Query](user/read-query.md) · [中文](user/read-query.zh-CN.md)
- [Scan And Export](user/scan-export.md) · [中文](user/scan-export.zh-CN.md)
- [Tenant And Config](user/tenant-config.md) · [中文](user/tenant-config.zh-CN.md)
- [Tasks And Maintenance](user/tasks-maintenance.md) · [中文](user/tasks-maintenance.zh-CN.md)
- [Errors And Troubleshooting](user/errors-troubleshooting.md) · [中文](user/errors-troubleshooting.zh-CN.md)
- [Go And Python SDK](user/sdk.md) · [中文](user/sdk.zh-CN.md)
- [API Map](user/api-map.md) · [中文](user/api-map.zh-CN.md)

## Reference documents

- [Database Introduction](database-introduction.md) · [中文](database-introduction.zh-CN.md)
- [GraphQL](graphql.md) · [中文](graphql.zh-CN.md)
- [Legacy Text DSL compatibility](gql.md)
- [Naming and compatibility](naming-and-compatibility.md) · [中文](naming-and-compatibility.zh-CN.md)
- [Query capabilities](query_capabilities.md)
- [Error codes](error_codes.md)
- [Architecture](architecture.en.md)
- [OpenAPI](openapi.yaml)
- [Product function gaps](product_function_gaps.md)
