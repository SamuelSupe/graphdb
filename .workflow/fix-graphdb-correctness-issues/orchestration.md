# Orchestration: Fix GraphDB correctness issues

## Execution Rules

- Keep the original objective intact.
- Ask for approval before risky, expensive, external, or destructive actions.
- Keep immediate blocking work local.
- Delegate only bounded, disjoint, materially useful packets.
- Integrate packet results before final verification.
- 用户未授权子代理；所有 packet 由主代理按隔离 pass 执行并记录结果。
- 不把测试失败通过重试隐藏；所有竞态失败必须解释并修复。

## Branching Rules

- 若修复需要改变公开 API，优先保持兼容；无法兼容时暂停并说明影响。
- 若 OrbStack 集成环境不可用，完成 MemoryStore/故障注入验证并将集成项明确标为未完成。
- 若发现新的 P1 数据损坏路径，将其加入当前目标，不缩小成功条件。

## Packet Prompts

- C1 owns `internal/storage/gc*` and matching HTTP validation tests. Preserve current checkpoint pagination semantics while binding destructive resume to the current manifest.
- C2 owns tenant lifecycle, purge and mutation guards. Avoid recursive tenant locking; status recheck must occur while the mutation lock is held.
- C3 owns direct commit idempotency and object-key existence caching. Recovery must be derivable from durable commit/manifest state and reject mismatched bodies.
- C4 owns restore drill target validation/ownership. Never purge a pre-existing target.
- C5 owns index task terminal-state publication and race tests.
- C6 owns only integration fixes and verification artifacts; it must not weaken earlier invariants.

## Completion Audit

- Review accepted/rejected packet results.
- Confirm no current snapshot deletion path remains.
- Confirm lifecycle transitions and purge share the mutation serialization boundary.
- Confirm direct commit retry after metadata failure returns the original result.
- Confirm restore drill cleanup proves ownership.
- Run targeted race repetitions and full repository checks.
