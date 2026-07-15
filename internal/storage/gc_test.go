package storage

import (
	"context"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestRunGCCleansOldSnapshotsAndOrphanCommits(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	first, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 1: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	second, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 2: %v", err)
	}
	if first.SnapshotKey == "" || second.SnapshotKey == "" || first.SnapshotKey == second.SnapshotKey {
		t.Fatalf("snapshots: first=%q second=%q", first.SnapshotKey, second.SnapshotKey)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.DeletedSnapshots != 1 || report.CommitCleanup.Deleted != 2 {
		t.Fatalf("report = %#v, want one snapshot and two orphan commits deleted", report)
	}
	if _, err := store.Objects.Get(ctx, first.SnapshotKey); err == nil {
		t.Fatal("old snapshot still exists")
	}
	if _, err := store.Objects.Get(ctx, second.SnapshotKey); err != nil {
		t.Fatalf("current snapshot missing: %v", err)
	}
}

func TestRunGCProtectsSnapshotAndCommitsForActiveReaderWatermark(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	first, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 1: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	second, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 2: %v", err)
	}
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{
		ReaderID:        "reader-slow",
		Status:          "fresh",
		VisibleVersion:  first.Version,
		ManifestVersion: first.Version,
		Consistent:      true,
		LastSeenAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put reader heartbeat: %v", err)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.ReaderWatermarkVersion != first.Version || report.ReaderWatermarkReaders != 1 {
		t.Fatalf("reader watermark report = %#v", report)
	}
	if report.DeletedSnapshots != 0 {
		t.Fatalf("report = %#v, want active reader to protect old snapshot", report)
	}
	if report.CommitCleanupSkippedReason == "" || report.IndexCleanupSkippedReason == "" {
		t.Fatalf("report = %#v, want commit and index cleanup skipped", report)
	}
	for _, key := range []string{first.SnapshotKey, first.SnapshotCatalogKey, second.SnapshotKey, second.SnapshotCatalogKey} {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("protected snapshot object %q missing: %v", key, err)
		}
	}
}

func TestRunGCIgnoresStaleReaderHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	first, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 1: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact 2: %v", err)
	}
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{
		ReaderID:        "reader-stale",
		Status:          "fresh",
		VisibleVersion:  first.Version,
		ManifestVersion: first.Version,
		Consistent:      true,
		LastSeenAt:      time.Now().UTC().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("put reader heartbeat: %v", err)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, ReaderMaxAge: time.Minute})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.ReaderWatermarkReaders != 0 || report.ReaderWatermarkIgnored != 0 {
		t.Fatalf("reader watermark report = %#v", report)
	}
	if _, err := store.Objects.Get(ctx, store.readerHeartbeatKey("tenant-a", "reader-stale")); err != ErrNotFound {
		t.Fatalf("stale reader heartbeat get err = %v, want ErrNotFound", err)
	}
	if report.DeletedSnapshots != 1 {
		t.Fatalf("report = %#v, want stale heartbeat ignored", report)
	}
	if _, err := store.Objects.Get(ctx, first.SnapshotKey); err == nil {
		t.Fatal("old snapshot still exists")
	}
	if _, err := store.Objects.Get(ctx, first.SnapshotCatalogKey); err == nil {
		t.Fatal("old sharded snapshot catalog still exists")
	}
}

func TestRunGCProtectsReaderSnapshotVersionWhenVisibleIsCurrent(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	first, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 1: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	second, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 2: %v", err)
	}
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{
		ReaderID:        "reader-current",
		Status:          "fresh",
		VisibleVersion:  second.Version,
		SnapshotVersion: first.Version,
		ManifestVersion: second.Version,
		Consistent:      true,
		LastSeenAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put reader heartbeat: %v", err)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.DeletedSnapshots != 0 || report.CommitCleanupSkippedReason != "" {
		t.Fatalf("report = %#v, want snapshot protected without skipping current commit cleanup", report)
	}
	if _, err := store.Objects.Get(ctx, first.SnapshotKey); err != nil {
		t.Fatalf("reader base snapshot missing: %v", err)
	}
}

func TestRunGCCleansExpiredDeadLetters(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	old := time.Now().UTC().Add(-2 * time.Hour)
	letter := DeadLetter{
		ID:        "collector-a/batch-old",
		TenantID:  "tenant-a",
		Source:    "agent",
		BatchID:   "batch-old",
		Status:    "pending",
		CreatedAt: old,
		UpdatedAt: old,
	}
	key := store.deadLetterKey("tenant-a", "agent", letter.ID)
	if _, err := store.putDeadLetterWithMeta(ctx, "tenant-a", key, letter, ObjectMeta{Key: key}); err != nil {
		t.Fatalf("put deadletter: %v", err)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{DeadLetterMaxAge: time.Hour})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.DeletedDeadLetters != 1 {
		t.Fatalf("report = %#v, want one deadletter deleted", report)
	}
	if _, err := store.Objects.Get(ctx, key); err == nil {
		t.Fatal("expired deadletter still exists")
	}
}

func TestRunGCDryRunReportsCheckpointWithoutDeleting(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	key := putExpiredDeadLetter(t, ctx, store, "tenant-a", "agent", "batch-dry-run")
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{DeadLetterMaxAge: time.Hour, DryRun: true})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.DeletedDeadLetters != 0 || report.Checkpoint.Planned != 1 || len(report.Checkpoint.PlannedKeys) != 1 {
		t.Fatalf("report = %#v, want planned deadletter without deletion", report)
	}
	if report.Checkpoint.PlannedKeys[0] != key || !report.Checkpoint.DryRun || !report.Checkpoint.Completed {
		t.Fatalf("checkpoint = %#v, want dry-run completed checkpoint for %q", report.Checkpoint, key)
	}
	if _, err := store.Objects.Get(ctx, key); err != nil {
		t.Fatalf("dry-run deleted %q: %v", key, err)
	}
}

func TestRunGCCheckpointCursorResumesDeleteBudget(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	keys := []string{
		putExpiredDeadLetter(t, ctx, store, "tenant-a", "agent", "batch-a"),
		putExpiredDeadLetter(t, ctx, store, "tenant-a", "agent", "batch-b"),
		putExpiredDeadLetter(t, ctx, store, "tenant-a", "agent", "batch-c"),
	}
	first, err := store.RunGC(ctx, "tenant-a", GCOptions{DeadLetterMaxAge: time.Hour, MaxDeletes: 1})
	if err != nil {
		t.Fatalf("first gc: %v", err)
	}
	if first.DeletedDeadLetters != 1 || !first.Checkpoint.Paused || first.Checkpoint.NextCursor == "" {
		t.Fatalf("first report = %#v, want one delete and paused cursor", first)
	}
	if _, err := store.Objects.Get(ctx, first.Checkpoint.DeletedKeys[0]); err == nil {
		t.Fatalf("first deleted key still exists: %q", first.Checkpoint.DeletedKeys[0])
	}
	second, err := store.RunGC(ctx, "tenant-a", GCOptions{
		DeadLetterMaxAge: time.Hour,
		CheckpointCursor: first.Checkpoint.NextCursor,
		MaxDeletes:       10,
	})
	if err != nil {
		t.Fatalf("second gc: %v", err)
	}
	if second.DeletedDeadLetters != 2 || second.Checkpoint.Paused || !second.Checkpoint.Completed {
		t.Fatalf("second report = %#v, want remaining deadletters deleted", second)
	}
	for _, key := range keys {
		if _, err := store.Objects.Get(ctx, key); err == nil {
			t.Fatalf("expired deadletter %q still exists", key)
		}
	}
}

func TestRunGCRejectsCheckpointCursorInsideCurrentSnapshot(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	manifest, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}

	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{CheckpointCursor: manifest.SnapshotCatalogKey}); err == nil {
		t.Fatal("gc accepted checkpoint cursor inside current snapshot")
	}
	if _, err := objects.Get(ctx, manifest.SnapshotKey); err != nil {
		t.Fatalf("current snapshot deleted: %v", err)
	}
	if _, err := objects.Get(ctx, manifest.SnapshotCatalogKey); err != nil {
		t.Fatalf("current snapshot catalog deleted: %v", err)
	}
	cold := NewTenantStore(objects, "test")
	graphData, loaded, err := cold.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cold load after rejected gc: %v", err)
	}
	if loaded.Version != manifest.Version {
		t.Fatalf("loaded version = %d, want %d", loaded.Version, manifest.Version)
	}
	if _, ok := graphData.GetEntity("host:1"); !ok {
		t.Fatal("current snapshot entity missing")
	}
}

func TestRunGCCleansExpiredTasksAndResults(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	old := time.Now().UTC().Add(-2 * time.Hour)
	task := Task{
		ID:         "task-old",
		TenantID:   "tenant-a",
		Type:       TaskTypeExportSnapshot,
		Status:     "succeeded",
		Phase:      "done",
		ResultKey:  store.taskResultKey("tenant-a", "task-old"),
		StartedAt:  old,
		UpdatedAt:  old,
		FinishedAt: old,
	}
	if err := store.putTaskResult(ctx, "tenant-a", task.ID, map[string]any{"ok": true}); err != nil {
		t.Fatalf("put task result: %v", err)
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("put task: %v", err)
	}
	indexTask := IndexTask{
		ID:         "index-old",
		TenantID:   "tenant-a",
		Type:       "rebuild",
		Status:     "succeeded",
		Phase:      "done",
		StartedAt:  old,
		UpdatedAt:  old,
		FinishedAt: old,
	}
	if err := store.saveIndexTask(ctx, indexTask); err != nil {
		t.Fatalf("put index task: %v", err)
	}

	report, err := store.RunGC(ctx, "tenant-a", GCOptions{TaskMaxAge: time.Hour})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.DeletedTasks != 1 || report.DeletedTaskResults != 1 || report.DeletedIndexTasks != 1 {
		t.Fatalf("report = %#v, want task/result/index task deleted", report)
	}
	for _, key := range []string{store.taskKey("tenant-a", task.ID), task.ResultKey, store.indexTaskKey("tenant-a", indexTask.ID)} {
		if _, err := store.Objects.Get(ctx, key); err == nil {
			t.Fatalf("expired object %q still exists", key)
		}
	}
}

func putExpiredDeadLetter(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, source string, batchID string) string {
	t.Helper()
	old := time.Now().UTC().Add(-2 * time.Hour)
	letter := DeadLetter{
		ID:        source + "/" + batchID,
		TenantID:  tenantID,
		Source:    source,
		BatchID:   batchID,
		Status:    "pending",
		CreatedAt: old,
		UpdatedAt: old,
	}
	key := store.deadLetterKey(tenantID, source, letter.ID)
	if _, err := store.putDeadLetterWithMeta(ctx, tenantID, key, letter, ObjectMeta{Key: key}); err != nil {
		t.Fatalf("put deadletter %q: %v", batchID, err)
	}
	return key
}
