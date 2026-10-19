# GGraphDB v2.3.5 未发布记录 / Unreleased validation

**NOT_RELEASED**。保留原标签 `v2.3.5`，指向 `84d269daebf1e7440341239a0dc297f4e96edd60`；不移动标签、不创建发行资产。
[标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39307798093) 的增强恢复阶段在
`docker compose up -d --no-build` 返回 1，尚未执行该阶段业务检查。原始命令 stderr 被捕获但没有打印，
具体 CI 触发原因 **UNCONFIRMED**。不能将此失败认定为后端业务错误，也不能将其算作 PASS。

此前 direct/WAL 集成、S3、TLS 网关、Raft 基础检查、已发布 v2.3.0 的单组及分片滚动均通过；
失败时全仓 race 和单机长测仍运行，后续取消本次发布流水线，未完成的门禁不记通过。
最终流水线为 CANCELLED；Raft 作业为 FAILURE，unit/race、单机长测和发布作业均为 CANCELLED。
失败时间点与最终作业结论见[JSON](validation-v2.3.5.json)。

调查发现普通/增强门禁的默认端口（如 36082、39082、46080）仍在常见 Linux 动态端口范围中；
先前 2.3.4 只移动了滚动脚本的默认端口。OrbStack 实际复现：出站客户端占用 36082 时，
Docker 绑定该端口返回 125（address already in use）；同一连接仍存续时，16082 绑定成功。
这证明该门禁风险存在，不能补证原 CI 的确切 errno。

v2.3.6 将完整门禁默认端口统一按夹具减去 20000，并保留显式偏移语义；
Compose 启动/恢复失败直接输出诊断，分片包装也打印捕获的输出。没有为本失败修改
`internal/`、`cmd/` 或 `tools/` 的优化实现，也没有更改重试、强一致、持久化和超时契约。
新标签必须独立完成全部门禁，无豁免；结果见[2.3.6 验证](validation-v2.3.6.md)。

C3/C7 对照保持原始构建身份，见[性能报告](performance-tail-validation-2026-10-20.zh-CN.md)；
不把未发布标签或本次 Compose 启动失败当作性能结论。
