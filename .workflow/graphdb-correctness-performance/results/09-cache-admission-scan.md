# Result 09: Cache, admission, and scan hot paths

Status: completed

- Cache weight includes canonical logical bytes and a conservative structural floor, including variable-length fields.
- Commit and ingest admission coalesce authoritative index/GC marker reads and never scan task history.
- Cursor scans reuse a bounded compiled catalog with binary-search starts, O(1) maps, and a trusted content hash for pinned pages.

Verification: large-field eviction, object-operation counts, cursor snapshot/read counters, hot-commit reads, and race tests passed.
