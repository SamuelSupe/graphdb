# Result 04: Cursor and catalog scan efficiency

Status: completed

- Entity and edge cursor scans binary-search the first relevant shard instead of restarting at shard zero.
- Scan loops build one catalog spec map and use O(1) lookups, including prefetch selection.
- Incremental index writes and persisted field/entity lookup also use catalog maps instead of repeated linear searches.
- Cursor encoding, query hash validation, pinned catalog version, and result ordering are unchanged.

Verification: read-counter tests prove later cursors do not fetch earlier entity pages or edge shards; repeated scan tests and the storage suite passed.
