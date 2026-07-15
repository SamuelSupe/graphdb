# Packet 03: Mutation-sized no-op detection

Objective: remove full-graph MD5 calculation from every commit while preserving no-op skipping and deterministic result identity.

Context: current commit hashes and sorts the complete graph after each mutation.

Ownership: graph apply report/change tracking, commit no-op path, write-cache metadata and benchmarks.

Do: make mutation application report whether logical content changed and compute a full digest only when explicitly required or when loading legacy cache state.

Do not: skip commits based only on request emptiness; suppressed or canonicalized mutations must retain current semantics.

Expected output: O(mutation-size) no-op decision and materially lower single-entity commit allocations/latency.

Verification: no-op correctness tests, hash compatibility tests and before/after benchmarks.
