# GGraphDB version and compatibility contracts

[中文](naming-and-compatibility.zh-CN.md)

The current release is **2.1.2**, developed on `main`. GGraphDB runs one process
per local data directory; S3-compatible storage is optional snapshot backup
storage. Remote primary storage, PostgreSQL coordination, separate reader/writer
modes and shared network filesystems are unsupported.

## Version identifiers

| Identifier | Current contract |
| --- | --- |
| Product and release tag | `VERSION`: `2.1.2`; tag: `v2.1.2` |
| Go/Python SDKs and OpenAPI document version | `2.1.2` |
| Go module | `github.com/SamuelSupe/graphdb/v2` |
| HTTP route namespace | `/v1/...`; not the product major version |
| Persisted Parquet/WAL and snapshot format | Introduced in 2.0 and retained by 2.1 |

Import the Go SDK from `github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`.
Existing extension directory names such as `extensions/v1.1/` are layout
identifiers, not product versions or a cross-major compatibility promise.
Security support is described in [SECURITY.md](../SECURITY.md).

## Installation and upgrades

- New installations and replacements of 1.x require a fresh data directory.
  No automatic 1.x migration or legacy `data_md5` response is provided. Keep 1.x
  installations and backups separate; never open 2.x data with a 1.x binary.
- 2.1 retains the 2.0 local data and snapshot formats. Stop the old process before
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
or `fleet` describe local state, not supported distributed deployment modes.

## Release status and performance claims

2.1.2 is published. Unit/vet/race, SDK, HTTP/restart, S3 backup/restore and artifact
checks passed. Its 30-minute mixed workload was stopped by explicit release
decision and is **not completed**, not a passing endurance check. The default tag
workflow retains that gate for future releases.

See [2.1.2 validation](validation-v2.1.2.md) and the
[write-tail measurements](performance-write-tail.md). The capacity envelope
remains `performance_unqualified`; a stable release label does not certify all
workload sizes or latency targets. Older reports describe their own builds.
