# GGraphDB 2.x: release-deployment

[中文](release-deployment.zh-CN.md)

GGraphDB 2.x uses one process and a persistent local directory. Upgrades from 1.x need a fresh directory; no 1.x data migration is provided.

The current branch also supports optional single-group and tenant-sharded Raft,
with an independent local directory per replica. The packaged examples are
`docker-compose.raft.yml` and `docker-compose.sharded.yml`. See the
[Raft operations guide](../raft-operations.zh-CN.md) and
[release qualification status](../raft-release-readiness.zh-CN.md). The upgrade
instructions below concern standalone deployments. Raft rolling upgrades are limited
to the [qualified compatibility window](../raft-rolling-upgrade.zh-CN.md); untested
versions require maintenance or migration. After protocol 3 activation, a binary
whose maximum supported protocol is 2 cannot reopen the same replica directory.

- [Start, write, and query](../../README.md)
- [Configuration, directory ownership, recovery, and validation](../local-disk.md)
- [API user guide](README.md)

Start the container:

```sh
docker compose up -d --build
```

Stop the service before using offline tools on its directory. Use HTTP for live administration.

## Release archive

Download the archive and checksum from the [2.2 release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.2.0).
Verify the outer archive and inner `SHA256SUMS`, then choose the binary for your platform:

```sh
sha256sum -c graphdb-v2.2.0.tar.gz.sha256
tar -xzf graphdb-v2.2.0.tar.gz
cd v2.2.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

Use `graphdb-linux-arm64` on ARM Linux or `graphdb-darwin-arm64` on Apple Silicon.
Configure authentication, tenant authorization and TLS at the gateway before
exposing the API. For off-machine recovery, configure [object snapshots](../object-backup.md).
Commit responses use `data_hash`; use the matching SDK release listed in [version boundaries](../naming-and-compatibility.md).

## Upgrade from 2.0

Use this process to upgrade standalone 2.0/2.1 installations to 2.2.0.

Create and verify a snapshot, stop the old process, replace the binary, then start
2.2.0 with the same `GRAPHDB_DATA_DIR` and prefix. The directory remains exclusive;
never run both versions against it. Validate readiness, representative queries,
and WAL status before reopening traffic. Preserve the pre-upgrade backup for
rollback; downgrading after enabling new automation is not a supported workflow.
S3 automation is opt-in; see [schedules and retention](../object-backup.md#automatic-backups).
