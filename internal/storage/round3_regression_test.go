package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
)

func TestRound3SaveQueryUsesMutationTime(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	defer store.ShutdownTasks(context.Background())
	fixed := time.Date(2026, 5, 1, 12, 30, 0, 0, time.UTC)
	ctx := ReplicatedContext(context.Background(), "cmd-save-query", fixed)

	saved, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name: "hosts",
		Request: query.Request{
			Op:   "match",
			Kind: "host",
		},
	})
	if err != nil {
		t.Fatalf("SaveQuery failed: %v", err)
	}
	if !saved.CreatedAt.Equal(fixed) || !saved.UpdatedAt.Equal(fixed) {
		t.Fatalf("SaveQuery ignored ReplicatedContext mutationTime: created=%v updated=%v want=%v", saved.CreatedAt, saved.UpdatedAt, fixed)
	}
}

func TestRound3PurgeTenantClearsCollectorStatusCache(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "cmdb",
		CollectorID: "collector-1",
		BatchID:     "batch-1",
		Items:       []IngestItem{{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host"}}},
	}); err != nil {
		t.Fatal(err)
	}
	// Warm collector status cache.
	before, err := store.GetCollectorStatus(ctx, "tenant-a", "cmdb", "collector-1")
	if err != nil || before.LastBatchID != "batch-1" {
		t.Fatalf("GetCollectorStatus before purge failed: status=%+v err=%v", before, err)
	}
	if _, err := store.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("PurgeTenant failed: %v", err)
	}
	after, err := store.GetCollectorStatus(ctx, "tenant-a", "cmdb", "collector-1")
	if err != nil {
		t.Fatalf("GetCollectorStatus after purge failed: %v", err)
	}
	if after.LastBatchID != "" || after.AppliedTotal != 0 {
		t.Fatalf("stale collector status survived purge: %+v", after)
	}
}

func TestRound3IngestWALOSModeRemovesStaleDurableFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	syncCfg := DefaultIngestWALConfig(dir)
	syncCfg.Durability = IngestWALDurabilitySync
	wal, err := OpenIngestWAL(syncCfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte(`{"batch":"b1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ingestWALDurableFile)); err != nil {
		t.Fatalf("expected .durable after sync mode: %v", err)
	}
	// Reopen in OS durability mode; stale .durable must be removed so OS mode recovery never truncates committed frames.
	osCfg := DefaultIngestWALConfig(dir)
	osCfg.Durability = IngestWALDurabilityOS
	wal2, err := OpenIngestWAL(osCfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatalf("reopen in OS mode failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ingestWALDurableFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale .durable was not removed in OS durability mode: err=%v", err)
	}
	if _, err := wal2.Append(context.Background(), IngestWALAccepted, []byte(`{"batch":"b2"}`)); err != nil {
		t.Fatal(err)
	}
	if err := wal2.Close(); err != nil {
		t.Fatal(err)
	}
	replayed := 0
	wal3, err := OpenIngestWAL(osCfg, func(IngestWALRecord) error {
		replayed++
		return nil
	})
	if err != nil {
		t.Fatalf("second reopen in OS mode failed: %v", err)
	}
	defer wal3.Close()
	if replayed != 2 {
		t.Fatalf("expected 2 replayed records, got %d", replayed)
	}
}

func TestRound3CommitBuilderRejectsOrphanFieldEnumAndDefault(t *testing.T) {
	for _, kind := range []string{codec.CommitRowCITypeFieldEnum, codec.CommitRowCITypeFieldDefault} {
		builder := codec.NewCommitBuilder(10)
		err := builder.Apply(codec.ParquetCommitRow{
			Kind:          kind,
			ChildOrdinal:  0,
			NestedOrdinal: 0,
			Value:         parquetio.Value{Kind: "string", StringValue: "v"},
		})
		if err == nil {
			t.Fatalf("expected error for orphan %q without parent field, got nil", kind)
		}
	}
}

func TestRound3GCPreservesLiveEntityRecordsWhenEnabled(t *testing.T) {
	ctx := context.Background()
	files := NewMemoryStore()
	store := NewTenantStoreWithOptions(files, "test", TenantStoreOptions{
		WriteEntityRecords:      true,
		UseEntityRecordsForRead: true,
	})
	defer store.ShutdownTasks(ctx)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:live", Kind: "host", Fields: map[string]any{"name": "alpha"}}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("RebuildIndexes failed: %v", err)
	}
	before, err := files.List(ctx, store.entityRecordPrefix("tenant-a"))
	if err != nil || len(before) == 0 {
		t.Fatalf("expected entity records to be written, count=%d err=%v", len(before), err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{}); err != nil {
		t.Fatalf("RunGC failed: %v", err)
	}
	after, err := files.List(ctx, store.entityRecordPrefix("tenant-a"))
	if err != nil || len(after) == 0 {
		t.Fatalf("GC deleted live entity records while WriteEntityRecords && UseEntityRecordsForRead are enabled: count=%d err=%v", len(after), err)
	}
}

func TestRound3CrossTenantBackupRejectedBeforeRead(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.Commit(ctx, "victim", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:secret", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	backup, err := store.StartTask(ctx, "victim", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	for {
		cur, err := store.GetTask(ctx, "victim", backup.ID)
		if err != nil {
			t.Fatal(err)
		}
		if cur.Status == TaskStatusSucceeded {
			backup = cur
			break
		}
		if cur.Status == TaskStatusFailed {
			t.Fatalf("backup failed: %s", cur.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	existingKey := backup.ResultKey
	missingKey := store.taskResultKey("victim", "nonexistent-task-id")
	_, errExisting := store.loadTenantBackupInputForTenant(ctx, existingKey, "attacker")
	_, errMissing := store.loadTenantBackupInputForTenant(ctx, missingKey, "attacker")
	if !errors.Is(errExisting, ErrInvalidArgument) || !errors.Is(errMissing, ErrInvalidArgument) {
		t.Fatalf("expected uniform ErrInvalidArgument for cross-tenant keys: existing=%v missing=%v", errExisting, errMissing)
	}
	if errExisting.Error() != errMissing.Error() {
		t.Fatalf("existence oracle detected: existing=%q missing=%q", errExisting.Error(), errMissing.Error())
	}
}
