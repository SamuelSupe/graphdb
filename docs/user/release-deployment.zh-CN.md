# 本地磁盘版：release-deployment

本版使用单机单进程和持久化本地目录。

- [启动、写入和查询](../../README.zh-CN.md)
- [配置、目录独占、备份恢复与验证](../local-disk.zh-CN.md)
- [API 用户手册](README.zh-CN.md)

容器启动：

```sh
docker compose up -d --build
```

停止服务后才能对同一目录执行离线 CLI。在线操作使用 HTTP API。
