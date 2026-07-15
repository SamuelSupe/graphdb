# Result 02: Durable task state

Status: completed

- Task persistence, progress, cancellation, and terminal transitions use ETag CAS instead of unconditional overwrite.
- GC has one authoritative `_active/gc.parquet` marker with heartbeat expiry and ownership-preserving CAS updates.
- Stale GC markers terminalize their historical task; a crash between marker claim and task persistence reconstructs failed history.
- Write backpressure reads only the GC marker and never lists historical task objects.
- Progress observes concurrent cancellation or stale-task failure and cannot revive terminal state.

Verification: cancellation/progress race, stale marker recovery, missing-history recovery, no-list backpressure, package tests, and race suite passed.
