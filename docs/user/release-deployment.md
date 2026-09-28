# GGraphDB 2.0: release-deployment

[中文](release-deployment.zh-CN.md)

GGraphDB 2.0 uses one process and a persistent local directory. Start with a fresh directory; no 1.x data migration is provided.

- [Start, write, and query](../../README.md)
- [Configuration, directory ownership, recovery, and validation](../local-disk.md)
- [API user guide](README.md)

Start the container:

```sh
docker compose up -d --build
```

Stop the service before using offline tools on its directory. Use HTTP for live administration.

## Release archive

Download the archive and checksum from the [2.0 release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.0.0).
Verify the outer archive and inner `SHA256SUMS`, then choose the binary for your platform:

```sh
sha256sum -c graphdb-v2.0.0.tar.gz.sha256
tar -xzf graphdb-v2.0.0.tar.gz
cd v2.0.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

Use `graphdb-linux-arm64` on ARM Linux or `graphdb-darwin-arm64` on Apple Silicon.
Configure authentication, tenant authorization and TLS at the gateway before
exposing the API. For off-machine recovery, configure [object snapshots](../object-backup.md).
Commit responses use `data_hash`; update clients to the 2.0 SDKs.
