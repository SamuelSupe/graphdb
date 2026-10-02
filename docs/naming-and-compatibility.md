# GGraphDB version and compatibility contracts

[中文](naming-and-compatibility.zh-CN.md)

The current release is **2.2.0**, developed on `main`. GGraphDB runs one process
per local data directory, with default standalone and optional independent Raft
replicas/tenant sharding; S3-compatible storage is optional snapshot backup
storage. Remote primary storage, PostgreSQL coordination, separate reader/writer
modes and shared network filesystems are unsupported.

## Version identifiers

| Identifier | Current contract |
| --- | --- |
| Product and release tag | `VERSION`: `2.2.0`; tag: `v2.2.0` |
| Go/Python SDKs and OpenAPI document version | `2.2.0` |
| Go module | `github.com/SamuelSupe/graphdb/v2` |
| HTTP route namespace | `/v1/...`; not the product major version |
| Persisted Parquet/WAL and snapshot format | Standalone retains 2.0/2.1; Raft has separate protocol and directory-role markers |

Import the Go SDK from `github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`.
Existing extension directory names such as `extensions/v1.1/` are layout
identifiers, not product versions or a cross-major compatibility promise.
Security support is described in [SECURITY.md](../SECURITY.md).

## Installation and upgrades

- New installations and replacements of 1.x require a fresh data directory.
  No automatic 1.x migration or legacy `data_md5` response is provided. Keep 1.x
  installations and backups separate; never open 2.x data with a 1.x binary.
- Standalone 2.2 retains the 2.0/2.1 local data and snapshot formats. Stop the old process before
  reusing a 2.0/2.1 directory with the same data root and prefix. Directory reuse
  does not permit concurrent processes or versions.
- Upgrade compatibility does not promise direct binary downgrade or cross-major
  rollback. Keep a verified pre-upgrade backup and follow the
  [upgrade instructions](user/release-deployment.md#upgrade-from-20).
- 2.1 adds opt-in S3 scheduling, retries, retention and restore drills. A logical
  graph snapshot does not contain all operational state; consult the
  [backup scope](object-backup.md) before planning disaster recovery.

## Data and API contracts

Commit results use `data_hash`: `sha256-shards-v2:` followed by 64 lowercase hex
characters. It identifies logical graph content, excluding commit version and
timestamps. The algorithm is specified in [content-hash-v2.md](content-hash-v2.md).
It is a different contract from the former MD5 of the complete logical JSON.
No-op writes retain the current version and hash; idempotent retries return the
recorded result. `expected_version`, `min_version`, cursor version checks and
WAL accepted/published/terminal distinctions remain supported.

GraphQL is served by `POST /v1/query/graphql`. The deprecated text DSL aliases
remain text DSL endpoints. Compatibility control routes named `reader`, `writer`
or `fleet` describe local state, not Raft membership. Distributed deployments use
the [Raft administration APIs](raft-ha.zh-CN.md).

## Raft protocols and upgrades

Product versions, Raft protocols and snapshot formats are separate identifiers.
Raft defaults to protocol 1 and supports up to 3. Activate protocol-2 streaming
snapshots/prepared maintenance or protocol-3 prepared GC only after all voters
support them. A protocol-2-only binary cannot open a directory whose protocol-3
minimum has been persisted. Rolling upgrades cover only the
[qualified source/target/protocol window](raft-rolling-upgrade.zh-CN.md).
Standalone and replica directories cannot be interchanged; migrate through API
imports or backup restoration into a fresh directory.

## Release status and performance claims

See [2.2.0 validation](validation-v2.2.0.md), actual workflow conclusions and
packaged evidence for the specific release binary. The local candidate passed a
thirty-minute Raft maintenance workload, with expected 429s and long write waits.
Cross-host, capacity and day-scale stability remain unqualified. A stable label
does not certify every workload size or latency target. The capacity envelope
retains the historical 2.1.2 `performance_unqualified` record and does not qualify
2.2.0 or Raft capacity. Older reports describe their own builds.
