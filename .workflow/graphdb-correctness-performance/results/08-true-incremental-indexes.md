# Result 08: True incremental indexes

Status: completed

- Mutation reports carry affected edge IDs and drive affected-only secondary, edge-shard, entity-page, and entity-record writes.
- Unchanged catalog objects are reused; new split prefixes cannot shadow an existing secondary shard.
- The final 10k single-entity benchmark was 25.2–26.1 ms/op, 38.11–38.17 MB/op, and about 337k allocs/op versus the recorded `main` range of 36.6–38.3 ms/op, 56.27–56.34 MB/op, and about 506k allocs/op.

Verification: index correctness/takeover tests, package race, and three benchmark runs passed.
