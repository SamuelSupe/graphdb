# GGraphDB 2.0: release-deployment

[中文](release-deployment.zh-CN.md)

GGraphDB 2.x uses one process and a persistent local directory. Upgrades from 1.x need a fresh directory; no 1.x data migration is provided.

- [Start, write, and query](../../README.md)
- [Configuration, directory ownership, recovery, and validation](../local-disk.md)
- [API user guide](README.md)

Start the container:

```sh
docker compose up -d --build
```

Stop the service before using offline tools on its directory. Use HTTP for live administration.

## Release archive

Download the archive and checksum from the [2.1 release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.1.0).
Verify the outer archive and inner `SHA256SUMS`, then choose the binary for your platform:

```sh
sha256sum -c graphdb-v2.1.0.tar.gz.sha256
tar -xzf graphdb-v2.1.0.tar.gz
cd v2.1.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

Use `graphdb-linux-arm64` on ARM Linux or `graphdb-darwin-arm64` on Apple Silicon.
Configure authentication, tenant authorization and TLS at the gateway before
exposing the API. For off-machine recovery, configure [object snapshots](../object-backup.md).
Commit responses use `data_hash`; update clients to the 2.0 SDKs.

## Upgrade from 2.0

Create and verify a snapshot, stop the old process, replace the binary, then start
2.1.0 with the same `GRAPHDB_DATA_DIR` and prefix. The directory remains exclusive;
never run both versions against it. Validate readiness, representative queries,
and WAL status before reopening traffic. Preserve the pre-upgrade backup for
rollback; downgrading after enabling new automation is not a supported workflow.
S3 automation is opt-in; see [schedules and retention](../object-backup.md#automatic-backups).
