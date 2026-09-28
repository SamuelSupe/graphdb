# GGraphDB 2.0 contracts

The product is GGraphDB. Version 2.0 is the main local-disk release; S3-compatible
storage is optional snapshot backup storage. Run one process per local directory.
Remote primary storage, PostgreSQL coordination, separate reader/writer modes
and shared network filesystems are unsupported.

## Version boundary

2.0 replaces the 1.x release line. It does not provide automatic data migration,
a legacy `data_md5` response, or a cross-version rollback guarantee. Start with a
new data directory. Keep any 1.x installation and backups separate; 2.0 backup
and restore operate within the 2.0 format. Never point an older binary at 2.0 data.

Commit results use `data_hash`: `sha256-shards-v2:` followed by 64 lowercase hex
characters. It identifies logical graph content, excluding commit version and
timestamps. The algorithm is specified in [content-hash-v2.md](content-hash-v2.md).
It is a different contract from the former MD5 of the complete logical JSON.
No-op writes retain the current version and hash; idempotent retries return the
recorded result. `expected_version`, `min_version`, cursor version checks and
WAL accepted/published/terminal distinctions remain supported.

The HTTP routes remain `/v1/...`; that is the API route namespace, not the product
major version. GraphQL is served by `POST /v1/query/graphql`. The legacy text DSL
aliases still refer to the text DSL, not GraphQL. Existing extension directory
names such as `extensions/v1.1/` are layout identifiers, not a compatibility promise.

Both SDKs are version 2.1.1. The Go module is
`github.com/SamuelSupe/graphdb/v2`; import
`github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`.

2.1 retains the 2.0 local data format and adds opt-in S3 automation. See [upgrade instructions](user/release-deployment.md#upgrade-from-20).
