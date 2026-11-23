# GGraphDB v2.6.2

2.6.2 修复 Raft 维护取消已开始的 NDJSON 流时，客户端可能把部分结果当成完整结果的问题。

- 已开始的 NDJSON 响应被维护打断时，服务显式中止 HTTP 响应。客户端应检查响应体读取错误和最终的 `done: true`，丢弃未完成的流并重试。
- 尚未开始写入的响应仍返回完整的 JSON `503 raft_unavailable`，其中 `retryable` 为 `true`，并附带 `Retry-After: 1`。阻塞写入在取消后至多有一秒，流式处理器无法续长这个期限。

四组定向用例在 Linux x86_64 和 OrbStack Linux ARM64 上分别通过三轮和五轮竞态检测。三节点 Raft 与 HAProxy 同时运行 GC、慢速租户用量读取和普通查询时，两类读取均收到完整的 503，GC 在 1.39 到 1.64 秒内完成。维护后两类读取均返回 200。截断错误通过 HAProxy 单独验证三轮。[Raft 读排空语义](https://github.com/SamuelSupe/graphdb/blob/v2.6.2/docs/raft-ha.zh-CN.md#243-读排空与转发修正) · [标签工作流](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.2)

## 升级

替换二进制时使用各 Raft 组已配置的协议。协议激活按独立的维护窗口流程执行。[Raft 升级说明](https://github.com/SamuelSupe/graphdb/blob/v2.6.2/docs/raft-rolling-upgrade.zh-CN.md) · [2.6.1 发行说明](https://github.com/SamuelSupe/graphdb/releases/tag/v2.6.1)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.2-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.2-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.6.2.tar.gz` |

将归档与对应的 `.sha256` 放在同一目录。以下命令校验 Linux AMD64 归档并检查版本。

```sh
archive=graphdb-v2.6.2-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.2-linux-amd64/graphdb version
```

完整版包含源码、SDK、部署示例和发布门禁证据。`BUILD-METADATA.json` 记录版本和构建提交。
