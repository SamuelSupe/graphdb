# Result 06: Tenant generation fencing

Status: completed

- Purge/recreate retains a monotonic fence epoch outside the deleted tenant prefix.
- Store-scoped fence contexts propagate into ordinary tasks and index rebuild workers; delayed tasks and heartbeats cannot adopt a new generation.
- Online metadata, task, backup, dead-letter, restore, repair, GC, and migration writes use fence-aware CAS. Overwrite restore has one explicit generation rebind after its owned purge.
- Native S3 conditional delete uses `If-Match`; conflicts never fall through to unconditional deletion.

Verification: late config/task/index-task/heartbeat/commit regressions, overwrite restore, takeover contention tests, and RustFS integration passed.
