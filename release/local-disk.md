# GGraphDB v2.6.1

2.6.1 修复 Raft 维护取消慢读时可能出现的网关 502 或连接提前关闭。

- 维护取消读取时，如果响应尚未开始写入，服务返回完整的 JSON `503 raft_unavailable`，其中 `retryable` 为 `true`，并附带 `Retry-After: 1`。
- 已经开始写入的响应在取消后最多还有一秒写入时间，流式处理器无法续长维护设定的期限。

三节点 Raft 与 HAProxy 的定向回归在竞态检测下连续通过五轮。GC 与慢速租户用量读取、普通查询同时运行时，两类读取均收到完整的 503，GC 在 1.14 到 1.33 秒内完成。维护后两类查询均返回 200。另有阻塞流式连接按期限释放的回归用例。[Raft 读排空语义](https://github.com/SamuelSupe/graphdb/blob/v2.6.1/docs/raft-ha.zh-CN.md#243-读排空与转发修正) · [标签工作流](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.1)

## 升级

替换二进制时使用各 Raft 组已配置的协议。协议激活按独立的维护窗口流程执行。[Raft 升级说明](https://github.com/SamuelSupe/graphdb/blob/v2.6.1/docs/raft-rolling-upgrade.zh-CN.md) · [2.6.0 发行说明](https://github.com/SamuelSupe/graphdb/releases/tag/v2.6.0)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.1-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.1-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.6.1.tar.gz` |

将归档与对应的 `.sha256` 放在同一目录。以下命令校验 Linux AMD64 归档并检查版本。

```sh
archive=graphdb-v2.6.1-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.1-linux-amd64/graphdb version
```

完整版包含源码、SDK、部署示例和发布门禁证据。`BUILD-METADATA.json` 记录版本和构建提交。
