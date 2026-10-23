package storage

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
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

func TestRunGCProtectsSnapshotAndCommitsForActiveReadView(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
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
	release, err := store.PinReadView(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.DeletedSnapshots != 0 || report.Checkpoint.DeferredFiles == 0 {
		t.Fatalf("pinned view was not protected: %+v", report)
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

func TestRunGCProtectsPinnedSnapshotFiles(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
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
	_, err = store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact 2: %v", err)
	}
	release, err := store.PinReadView(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
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

func TestTaskRetentionPreservesLocalBackup(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	task, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded {
		t.Fatalf("backup: %+v", task)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	task.StartedAt, task.UpdatedAt, task.FinishedAt = old, old, old
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{TaskMaxAge: time.Hour}); err != nil {
		t.Fatal(err)
	}
	key := task.Result["backup_manifest_key"].(string)
	manifest, err := store.loadBackupManifest(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if report := store.validateBackupManifest(ctx, manifest); report.Status != "ok" {
		t.Fatalf("retention invalidated backup: %+v", report)
	}
}

func TestGCReacquiresParentTaskAdmission(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	admission := &taskExecutionAdmission{tenant: make(chan struct{}, 1), execution: make(chan struct{}, 1)}
	if !admission.acquire(ctx) {
		t.Fatal("initial admission")
	}
	defer admission.release()
	ctx = context.WithValue(ctx, taskIngestAdmissionKey{}, admission)
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{}); err != nil {
		t.Fatal(err)
	}
	if !admission.held || len(admission.execution) != 1 || len(admission.tenant) != 1 {
		t.Fatal("GC returned to its parent without execution admission")
	}
}

func TestGCCatalogFailureDoesNotBlockIndependentRetention(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	key := putExpiredDeadLetter(t, ctx, store, "tenant-a", "agent", "expired")
	if err := base.Put(ctx, store.indexCatalogKey("tenant-a"), []byte("corrupt catalog")); err != nil {
		t.Fatal(err)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{CleanupIndexOrphans: true, DeadLetterMaxAge: time.Hour})
	if err == nil || report.IndexCleanupError == "" {
		t.Fatalf("catalog failure hidden: %+v %v", report, err)
	}
	if _, err := base.Get(ctx, key); err != ErrNotFound {
		t.Fatalf("unrelated retention starved: %v", err)
	}
}
func TestGCCleansShardedSnapshotWithoutFlatAnchor(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.putShardedSnapshot(ctx, "tenant-a", g.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1}); err != nil {
		t.Fatal(err)
	}
	objects, err := store.Objects.List(ctx, store.snapshotVersionPrefix("tenant-a", 1)+"/")
	if err != nil || len(objects) != 0 {
		t.Fatalf("unanchored snapshot leaked: %d %v", len(objects), err)
	}
	g, _, err = store.Load(ctx, "tenant-a")
	if err != nil || g.Entities.Len() != 2 {
		t.Fatalf("current snapshot damaged: %v", err)
	}
}

type gcHistoryReadCounter struct {
	ObjectStore
	historyPrefix string
	histories     atomic.Int64
}

func (s *gcHistoryReadCounter) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if strings.HasPrefix(key, s.historyPrefix) && isVersionedIndexCatalogKey(key) {
		s.histories.Add(1)
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}
func TestGCMarksRetainedAdjacencyWithBoundedHistoryReads(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	store.WriteIndexRangePages = true
	store.WriteIndexAdjacencyPages = true
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}}, UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:b"}}, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	prefix := store.Prefix + "/" + store.adjacencyPrefix("tenant-a", 1, "orphan")
	for i := range 1200 {
		if err = files.Put(ctx, fmt.Sprintf("%s%04d.adj.json", prefix, i), []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	historyPrefix := store.parquetVersionRootPrefix("tenant-a")
	catalogs, err := files.List(ctx, historyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	catalogCount := 0
	for _, object := range catalogs {
		if isVersionedIndexCatalogKey(object.Key) {
			catalogCount++
		}
	}
	if catalogCount == 0 {
		t.Fatal("fixture has no retained catalog history")
	}
	counted := &gcHistoryReadCounter{ObjectStore: files, historyPrefix: historyPrefix}
	store.Objects = counted
	if _, err = store.RunGC(ctx, "tenant-a", GCOptions{CleanupIndexOrphans: true, SkipEntityRecordCleanup: true}); err != nil {
		t.Fatal(err)
	}
	if n := counted.histories.Load(); n > int64(catalogCount) {
		t.Fatalf("redecoded retained catalogs across GC pages: %d", n)
	}
	objects, err := files.List(ctx, prefix)
	if err != nil || len(objects) != 0 {
		t.Fatalf("orphan cleanup: %d %v", len(objects), err)
	}
	graph, _, err := store.Load(ctx, "tenant-a")
	if err != nil || graph.Edges.Len() != 1 {
		t.Fatalf("lost retained adjacency: %v", err)
	}
}

func TestGCOrphanSnapshotCursorRetainsProtectedVersions(t *testing.T) {
	for _, pausedVersion := range []int64{1, 2, 3} {
		t.Run(fmt.Sprint(pausedVersion), func(t *testing.T) {
			ctx := context.Background()
			store := NewTenantStore(NewMemoryStore(), "test")
			for version := 1; version <= 2; version++ {
				if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: fmt.Sprintf("host:%d", version), Kind: "host"}}}, CommitOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := store.Compact(ctx, "tenant-a"); err != nil {
					t.Fatal(err)
				}
			}
			g, manifest, err := store.Load(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			future := g.Snapshot()
			future.Version = 3
			if _, err = store.putShardedSnapshot(ctx, "tenant-a", future); err != nil {
				t.Fatal(err)
			}
			if _, err = store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 2, CheckpointCursor: store.snapshotCatalogKey("tenant-a", 3)}); err == nil {
				t.Fatal("gc accepted a whole-version deletion cursor inside a future snapshot")
			}
			options := GCOptions{MaxDeletes: gcBatchDeletes, listings: make(map[string]gcListing)}
			if pausedVersion > 1 {
				options.CheckpointCursor = store.snapshotVersionPrefix("tenant-a", pausedVersion-1) + "/\xff"
			}
			checkpoint := newGCCheckpointRunner(options)
			// Exhaust the batch before this phase, as a preceding cleanup phase
			// can do on a slow disk. The next object must be revisited on resume.
			checkpoint.visited = 1
			checkpoint.started = time.Now().Add(-gcBatchDuration)
			if _, err = store.cleanupShardedSnapshotOrphansLocked(ctx, "tenant-a", manifest, checkpoint); !gcPaused(err) {
				t.Fatalf("fixture did not pause before version %d: %v", pausedVersion, err)
			}
			if _, err = store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 2, CheckpointCursor: checkpoint.result().NextCursor}); err != nil {
				t.Fatal(err)
			}
			for version := int64(1); version <= 3; version++ {
				if _, err = store.Objects.Get(ctx, store.snapshotCatalogKey("tenant-a", version)); err != nil {
					t.Fatalf("protected snapshot version %d lost on resume: %v", version, err)
				}
			}
			for version := int64(1); version <= 2; version++ {
				if _, err = store.Objects.Get(ctx, store.snapshotKey("tenant-a", version)); err != nil {
					t.Fatalf("retained flat snapshot version %d lost: %v", version, err)
				}
			}
			cold := NewTenantStore(store.Objects, "test")
			loaded, current, err := cold.Load(ctx, "tenant-a")
			if err != nil || current.Version != 2 || loaded.Entities.Len() != 2 {
				t.Fatalf("current snapshot changed on resume: %+v %v", current, err)
			}
		})
	}
}
