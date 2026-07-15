# Result 07: Fingerprint and `data_md5` compatibility

Status: completed

- The graph maintains a deterministic incremental content fingerprint under a mutex and clones it by value.
- Public `data_md5` remains the exact legacy canonical logical-graph MD5; an optional manifest field avoids recomputation without changing legacy decoding.
- Changed commits compute canonical MD5 and logical bytes once, while no-op detection avoids repeated full-graph sorting and encoding.

Verification: exact legacy equality, cold replay, concurrent first read/commit, clone, and race tests passed.
