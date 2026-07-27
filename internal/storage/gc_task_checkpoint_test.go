package storage

import (
	"context"
	"testing"
	"time"
)

func TestRunGCCheckpointCompletesExpiredTaskObjectGroup(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	keys := putExpiredTaskObjectGroup(t, ctx, store, objects)

	cursor := ""
	reports := make([]GCReport, 0, 5)
	for attempt := 0; attempt < 5; attempt++ {
		report, err := store.RunGC(ctx, "tenant-a", GCOptions{
			TaskMaxAge:       time.Hour,
			CheckpointCursor: cursor,
			MaxDeletes:       1,
		})
		if err != nil {
			t.Fatalf("gc attempt %d: %v", attempt+1, err)
		}
		reports = append(reports, report)
		if report.Checkpoint.Completed {
			break
		}
		cursor = report.Checkpoint.NextCursor
		if cursor == "" {
			t.Fatalf("gc attempt %d paused without a cursor", attempt+1)
		}
	}

	for label, key := range keys {
		if _, err := objects.Get(ctx, key); err == nil {
			t.Fatalf(
				"%s %q survived completed checkpoint GC: reports=%#v",
				label, key, reports,
			)
		}
	}
}

func TestRunGCDryRunCheckpointPlansExpiredTaskObjectGroupOnce(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	keys := putExpiredTaskObjectGroup(t, ctx, store, objects)

	first, err := store.RunGC(ctx, "tenant-a", GCOptions{
		TaskMaxAge: time.Hour,
		DryRun:     true,
		MaxDeletes: 1,
	})
	if err != nil {
		t.Fatalf("first dry-run gc: %v", err)
	}
	if first.Checkpoint.Planned != len(keys) ||
		!first.Checkpoint.Paused ||
		first.Checkpoint.NextCursor == "" {
		t.Fatalf(
			"first dry-run checkpoint = %#v, want one complete task group",
			first.Checkpoint,
		)
	}
	second, err := store.RunGC(ctx, "tenant-a", GCOptions{
		TaskMaxAge:       time.Hour,
		DryRun:           true,
		MaxDeletes:       1,
		CheckpointCursor: first.Checkpoint.NextCursor,
	})
	if err != nil {
		t.Fatalf("resumed dry-run gc: %v", err)
	}
	if !second.Checkpoint.Completed || second.Checkpoint.Planned != 0 {
		t.Fatalf(
			"resumed dry-run checkpoint = %#v, want completed without replanning",
			second.Checkpoint,
		)
	}
	for label, key := range keys {
		if _, err := objects.Get(ctx, key); err != nil {
			t.Fatalf("dry-run removed %s %q: %v", label, key, err)
		}
	}
}

func putExpiredTaskObjectGroup(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	objects *MemoryStore,
) map[string]string {
	t.Helper()
	old := time.Now().UTC().Add(-2 * time.Hour)
	taskID := "00000000-expired-import"
	sourceKey := store.importSourceKey("tenant-a", taskID, "jsonl")
	resultKey := store.taskResultKey("tenant-a", taskID)
	task := Task{
		ID:         taskID,
		TenantID:   "tenant-a",
		Type:       TaskTypeBulkImport,
		Status:     TaskStatusSucceeded,
		Phase:      "done",
		Params:     map[string]any{"source_key": sourceKey},
		ResultKey:  resultKey,
		StartedAt:  old,
		UpdatedAt:  old,
		FinishedAt: old,
	}
	if err := objects.Put(ctx, sourceKey, []byte("source")); err != nil {
		t.Fatalf("put import source: %v", err)
	}
	if err := store.putTaskResult(
		ctx, "tenant-a", taskID, map[string]any{"ok": true},
	); err != nil {
		t.Fatalf("put task result: %v", err)
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	return map[string]string{
		"task":          store.taskKey("tenant-a", taskID),
		"task result":   resultKey,
		"import source": sourceKey,
	}
}
