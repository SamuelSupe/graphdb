# GGraphDB 2.x: quickstart

[中文](quickstart.zh-CN.md)

GGraphDB 2.x uses one process and a persistent local directory. New installations
and replacements of 1.x need a fresh directory; existing 2.0/2.1 directories can
be reused after stopping the old process. See [version boundaries](../naming-and-compatibility.md).

- [Start, write, and query](../../README.md)
- [Configuration, directory ownership, recovery, and validation](../local-disk.md)
- [API user guide](README.md)

Start the container:

```sh
docker compose up -d --build
```

Stop the service before using offline tools on its directory. Use HTTP for live administration.
