# Local disk edition: quickstart

This edition uses one process and a persistent local directory.

- [Start, write, and query](../../README.md)
- [Configuration, directory ownership, recovery, and validation](../local-disk.md)
- [API user guide](README.md)

Start the container:

```sh
docker compose up -d --build
```

Stop the service before using offline tools on its directory. Use HTTP for live administration.
