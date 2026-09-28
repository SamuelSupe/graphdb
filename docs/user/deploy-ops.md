# GGraphDB 2.0: deploy-ops

[中文](deploy-ops.zh-CN.md)

GGraphDB 2.0 uses one process and a persistent local directory. Start with a fresh directory; no 1.x data migration is provided.

- [Start, write, and query](../../README.md)
- [Configuration, directory ownership, recovery, and validation](../local-disk.md)
- [Object-storage snapshots and on-demand restore](../object-backup.md)
- [API user guide](README.md)

Start the container:

```sh
docker compose up -d --build
```

Stop the service before using offline tools on its directory. Use HTTP for live administration.
