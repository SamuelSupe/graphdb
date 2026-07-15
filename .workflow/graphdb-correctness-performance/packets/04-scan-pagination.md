# Packet 04: Cursor and catalog scan efficiency

Objective: resume entity/edge scans at the cursor shard and avoid repeated linear catalog searches.

Context: every page starts at the first shard; `entityPageSpec` and `edgeShardSpec` linearly search catalog slices.

Ownership: scan cursor helpers, entity/edge scan loops and focused performance tests.

Do: binary-search the sorted target list using cursor shard, build small per-call spec maps, and preserve pinned catalog/query hashes.

Do not: change cursor encoding or result ordering.

Expected output: later pages do not read/decode earlier shards and spec lookup is O(1).

Verification: multi-shard cursor tests with read counters and existing scan correctness suite.
