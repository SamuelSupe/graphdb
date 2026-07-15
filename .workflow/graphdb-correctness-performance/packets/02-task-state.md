# Packet 02: Durable task state

Objective: make cancel/progress transitions concurrency-safe, recover stale GC state, and remove historical task scans from write admission.

Context: task objects use unconditional Put; GC backpressure scans all task objects and trusts any persisted `running` state forever.

Ownership: unified task persistence/runtime, GC active marker, backpressure lookup and related tests.

Do: use ETag CAS state transitions, create a small authoritative GC marker with owner/expiry, and clear/recover stale markers deterministically.

Do not: auto-resume destructive work without validated checkpoints.

Expected output: O(1) GC-running lookup and deterministic stale-task terminalization/cancellation behavior.

Verification: race tests, crash/takeover simulations, and a no-history-scan test.
