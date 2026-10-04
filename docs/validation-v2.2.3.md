# GGraphDB 2.2.3 发布门禁失败 / Unpublished tag

v2.2.3 标签指向 `f687cf213b227e2984979f3895e1b9e6407ad7a3`。[正式发布工作流 37204045846](https://github.com/SamuelSupe/graphdb/actions/runs/37204045846)为 **FAIL**，发布作业跳过，未创建 GitHub Release 或发行资产；标签不删除、不重写。最新成功发行仍为 v2.2.2，后续发行状态见 [2.2.4](validation-v2.2.4.md)。

- 全仓 unit/vet/race、SDK/备份 worker、单机集成、MinIO/S3、TLS 网关与单机三十分钟长测通过。
- 协议 1/2/3、限定旧开发基线的混部滚动、故障恢复和串行重启通过。
- 协议 3 的三十分钟维护长测失败：`saved-service-impact` 的 2,569 次执行有一次 HTTP 504。2026-10-04 13:39:22 UTC 服务端记录 `context deadline exceeded`，五秒预算耗尽，图遍历未开始。当时 readiness 正常；没有据此排除服务端问题。
- Raft 长测完成 compact 五次、GC 两次和 index rebuild 两次；读就绪及索引健康采样未报错。这些局部结果不能覆盖查询失败。
- 单机三十分钟完成 125,770 次指标操作、零操作错误；ingest P99 28.959 秒。操作数包含查询及维护，不是业务吞吐容量认证。
- 同提交 main CI 首次有 membership 竞争场景的失主与 Docker 启动退出 125；Linux arm64 的 membership race 二十轮未复现，原失败仍保留。同提交失败作业重跑通过，不证明首轮故障根因已排除。

证据保存于 `/private/tmp/graphdb-release-v223-20261004/`，包括 `tag-raft-evidence/protocol3/dual/{soak.ndjson,soak-report.txt,raft.log}`、失败工作流日志和 main 首次失败。v2.2.4 增加读缓存保留回归与修复；历史 504 唯一根因尚未证明，必须重新运行全部正式门禁。没有延长请求超时、忽略错误或申请豁免。

This tag was not published. The exact-tag workflow failed with one saved-query HTTP 504 in its Raft maintenance soak. Other passing gates do not override that failure. The tag and failed results remain immutable; the next candidate must independently pass the complete release workflow.
