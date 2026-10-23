# GGraphDB 2.4.2 validation

Candidate qualification is in progress; **not published and no tag created**. Every new compile and qualification uses **Go1.27.3**.

The immutable [2.4.1 tag qualification failed](validation-v2.4.1.md) with 23 query errors in the original three-replica thirty-minute soak. Its full log and workload evidence were read before remediation.

- A completing read revoked by a control operation: baseline **3/3 FAIL**. Lost POST-query responses reported as unknown writes: baseline **3/3 FAIL**.
- After the focused repairs, both regressions plus original long-read cancellation, slow blocked-socket control deadlines and nonretryable lost-write response assertions: **3 race repetitions PASS** in OrbStack Go1.27.3 (37.856s).
- Current full unit/vet/race, exact-source direct/WAL recovery, original full protocol3 thirty-minute soak and all seven main CI jobs: **PENDING**.
- New annotated tag, all original exact-tag release jobs, actual assets, independent source/checksum/three-binary identity checks, and downloaded LinuxARM64 fresh-directory replay/backup restore: **PENDING**.

The read-drain change allows at most one second for ordinary in-flight reads to finish before revoking blocked readers. Original two/three-second regression deadlines, thirty-minute durations, workload, five-second query budget and zero-error rules remain unchanged. It adds no storage I/O, changes no index layout or cursor, and does not retry unknown mutations.

Range and adjacency pages remain off by default. Upgrade every reader/writer/GC/replica before enabling; rebuild the old layout with the new binary before index downgrade. WAL v1 durable markers migrate to two-slot v2, which old binaries cannot read: drain WAL, preserve the whole directory and verified pre-upgrade backups. Existing public tags and assets remain immutable.

[Machine-readable status](validation-v2.4.2.json). Final CI for this record's eventual commit must be checked separately; no document can certify its own later run.
