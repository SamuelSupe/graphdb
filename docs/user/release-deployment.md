# GGraphDB 2.4.0 Release Deployment

[简体中文](release-deployment.zh-CN.md) · [User guide](README.md) · [Download](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.0)

Choose a deployment, verify the package, then start the service. Standalone uses
one process and one persistent local directory; each Raft replica owns a separate directory.

## Choose your deployment

| Mode | Packaged example | Operations guide |
| --- | --- | --- |
| Standalone | [docker-compose.yml](../../docker-compose.yml) | [Local disk, durability and recovery](../local-disk.md) |
| Single-group Raft | [docker-compose.raft.yml](../../docker-compose.raft.yml) | [Raft operations](../raft-operations.zh-CN.md) |
| Tenant-sharded Raft | [docker-compose.sharded.yml](../../docker-compose.sharded.yml) | [Sharding and migration](../sharding.zh-CN.md) |

The steps below start standalone. Cluster deployment and qualification follow the
linked guides and [readiness report](../raft-release-readiness.zh-CN.md).

## Install a release archive

### 1. Download and verify

Download `graphdb-v2.4.0.tar.gz` and `graphdb-v2.4.0.tar.gz.sha256` from the
[2.4.0 release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.0) into the same directory.

On Linux:

```sh
sha256sum -c graphdb-v2.4.0.tar.gz.sha256
tar -xzf graphdb-v2.4.0.tar.gz
cd v2.4.0
sha256sum -c SHA256SUMS
```

On macOS, use `shasum -a 256 -c` in place of `sha256sum -c` for both checks.

### 2. Select the binary

| Platform | Binary |
| --- | --- |
| Linux amd64 | `bin/graphdb-linux-amd64` |
| Linux arm64 | `bin/graphdb-linux-arm64` |
| macOS Apple Silicon | `bin/graphdb-darwin-arm64` |

### 3. Start the service

For Linux amd64, after unpacking:

```sh
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

The service user must be able to write to the data directory. Use the matching
binary from the table and a writable directory on other platforms.

### 4. Verify and open

In another terminal, check liveness:

```sh
curl -fsS http://127.0.0.1:8080/v1/health
```

Open **[http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/)** and follow the
[write/query example](../../README.md#write-and-query). Check
[readiness](../local-disk.md#readiness-and-memory-budgets) before reopening traffic.

## Container alternative

From the repository root, or the unpacked archive's `v2.4.0/` directory:

```sh
docker compose up -d --build
```

Stop the service before using offline CLI tools on its directory. Use HTTP for live administration.

<a id="upgrade-from-20"></a>

## Upgrade an existing installation

| Existing installation | Upgrade rule |
| --- | --- |
| Standalone 2.0 / 2.1 / 2.2 | Stop the old process; reuse the compatible directory with 2.4.0 |
| Raft | Follow the [qualified rolling window](../raft-rolling-upgrade.zh-CN.md); untested versions require maintenance or migration |
| 1.x | Start with a fresh directory; no 1.x data migration is provided |

### Standalone upgrade checklist

1. Create and verify a snapshot; retain the pre-upgrade backup for rollback.
2. Stop the old process. Never run two versions against the same directory.
3. Replace the binary and start 2.4.0 with the same `GRAPHDB_DATA_DIR` and prefix.
4. Check readiness, representative queries and WAL status before reopening traffic.

Downgrading after enabling new automation is unsupported. After Raft protocol 3
activation, a binary with maximum protocol 2 cannot reopen the same replica directory.
See [version boundaries](../naming-and-compatibility.md).

## Before exposing the service

| Need | Configuration |
| --- | --- |
| Authentication, tenant authorization, TLS | Configure the [gateway](../security-deployment.md) |
| Off-machine recovery | Configure [object snapshots](../object-backup.md) |
| Scheduled backups | Opt in to [standalone schedules and retention](../object-backup.md#automatic-backups), or use the [backup worker](../backup-automation.md) |
| Client compatibility | Use matching SDKs and the `data_hash` contract in [version boundaries](../naming-and-compatibility.md) |
