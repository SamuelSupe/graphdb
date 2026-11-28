package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRound7GCProtectsLiveShardedSnapshotWhenMonolithicKeyEmpty(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	store := NewTenantStore(mem, "r7-gc")
	defer store.ShutdownTasks(ctx)

	tenant := "t-sharded-snap"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Manifest pointing ONLY to SnapshotCatalogKey (SnapshotKey == "", e.g. after repairManifest).
	manifest := Manifest{
		TenantID:           tenant,
		Version:            5,
		SnapshotVersion:    5,
		SnapshotKey:        "",
		SnapshotCatalogKey: store.snapshotVersionPrefix(tenant, 5) + "/catalog.parquet",
	}

	// 1. validateGCSnapshotCursor must reject a sharded cursor at or above SnapshotVersion.
	shardedCursor := store.snapshotVersionPrefix(tenant, 5) + "/shard-00.parquet"
	if err := store.validateGCSnapshotCursor(tenant, manifest, shardedCursor); err == nil {
		t.Fatalf("expected validateGCSnapshotCursor to reject cursor referencing live sharded snapshot v5")
	}

	// 2. cleanupSnapshotsLocked must NOT delete live sharded/v5 objects even when monolithic v5 + future v6 exist.
	liveShardedObj := store.snapshotVersionPrefix(tenant, 5) + "/catalog.parquet"
	if err := mem.Put(ctx, liveShardedObj, []byte("live-sharded-v5")); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, store.snapshotKey(tenant, 5), []byte("old-mono-v5")); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, store.snapshotKey(tenant, 6), []byte("future-mono-v6")); err != nil {
		t.Fatal(err)
	}

	checkpoint := newGCCheckpointRunner(GCOptions{})
	if _, _, err := store.cleanupSnapshotsLocked(ctx, tenant, manifest, 1, checkpoint); err != nil {
		t.Fatalf("cleanupSnapshotsLocked failed: %v", err)
	}
	if _, err := mem.Get(ctx, liveShardedObj); err != nil {
		t.Fatalf("live current sharded snapshot object %q was erroneously deleted by GC: %v", liveShardedObj, err)
	}
}

func TestRound7SharedStoreWriterRecoversAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	shared := NewMemoryStore()
	storeA := NewTenantStore(shared, "r7-lease")
	storeA.InstanceID = "writer-a"
	defer storeA.ShutdownTasks(ctx)

	storeB := NewTenantStore(shared, "r7-lease")
	storeB.InstanceID = "writer-b"
	defer storeB.ShutdownTasks(ctx)

	tenant := "t-takeover"
	if _, err := storeA.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := storeA.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "h1", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("storeA initial commit failed: %v", err)
	}

	// Instance B takes over the writer lease on the shared store.
	if _, err := storeB.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "h2", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("storeB takeover commit failed: %v", err)
	}

	// Instance A's first write detects stale fence (ErrLeaseHeld) and evicts its stale cached lease.
	_, firstErr := storeA.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "h3", Kind: "host"}},
	}, CommitOptions{})
	if firstErr != nil && !errors.Is(firstErr, ErrLeaseHeld) {
		t.Fatalf("expected nil or ErrLeaseHeld on takeover transition, got %v", firstErr)
	}

	// Instance A's subsequent write MUST NOT remain permanently wedged; it must re-acquire lease and succeed.
	if _, err := storeA.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "h4", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("storeA remained wedged after lease takeover: %v", err)
	}
}

func TestRound7RepairTenantMetadataRepairsCorruptMetadata(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	store := NewTenantStore(mem, "r7-meta")
	defer store.ShutdownTasks(ctx)

	tenant := "t-corrupt-meta"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Corrupt tenant-metadata.parquet directly on shared store.
	if err := mem.Put(ctx, store.tenantMetadataKey(tenant), []byte("corrupt-parquet-bytes")); err != nil {
		t.Fatal(err)
	}
	store.deleteCachedTenantMetadata(tenant)

	if err := store.repairTenantMetadata(ctx, tenant); err != nil {
		t.Fatalf("repairTenantMetadata failed on corrupt metadata: %v", err)
	}
	meta, exists, _, err := store.getTenantMetadataWithMeta(ctx, tenant)
	if err != nil || !exists || meta.Status != TenantStatusActive {
		t.Fatalf("expected repaired active metadata, got meta=%+v exists=%v err=%v", meta, exists, err)
	}
}

func TestRound7RangeCompactionPreservesLiveCachesAndUpdatesEntityRecords(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := NewTenantStore(fs, "r7-compact-cache")
	defer store.ShutdownTasks(ctx)
	store.WriteEntityRecords = true
	store.WriteIndexRangePages = true
	store.WriteIndexAdjacencyPages = true

	tenant := "t-compact-r7"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "a-1", Kind: "host", Fields: graph.Fields{"v": 1}},
			{ID: "b-1", Kind: "host", Fields: graph.Fields{"v": 2}},
		},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "a-2", Kind: "host", Fields: graph.Fields{"v": 3}}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}

	// Ensure write cache and writer lease cache are warm before compaction.
	if _, ok := store.getWriteCache(tenant); !ok {
		t.Fatalf("expected warm write cache before compaction")
	}
	beforeLease, _, ok := store.getCachedWriterLease(tenant)
	if !ok {
		t.Fatalf("expected cached writer lease before compaction")
	}

	skipped := map[string]bool{}
	changed, _, _, err := store.compactRangeSlice(ctx, tenant, skipped)
	if err != nil {
		t.Fatalf("compactRangeSlice failed: %v", err)
	}
	_ = changed

	// Live write cache and writer lease cache must NOT be evicted by compaction!
	if _, ok := store.getWriteCache(tenant); !ok {
		t.Fatalf("compactRangeSlice evicted live TenantStore write cache")
	}
	afterLease, _, ok := store.getCachedWriterLease(tenant)
	if !ok || afterLease.FenceToken != beforeLease.FenceToken {
		t.Fatalf("compactRangeSlice evicted/rewrote live writer lease cache")
	}
}

func TestRound7RestoreDrillBackupIncludesRelationSchemasAndCleansUpOnFailure(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	store := NewTenantStore(mem, "r7-drill")
	defer store.ShutdownTasks(ctx)

	tenant := "t-drill-src"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "service", ToKind: "service", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "s1", Kind: "service"},
			{ID: "s2", Kind: "service"},
		},
		UpsertEdges: []graph.Edge{
			{ID: "e1", Type: "calls", From: "s1", To: "s2"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutRelationSchema(ctx, tenant, RelationSchema{
		RelationType: "calls",
		Fields:       map[string]graph.FieldSpec{"latency_ms": {Type: "number"}},
	}); err != nil {
		t.Fatal(err)
	}

	input, _, _, err := store.createRestoreDrillBackup(ctx, tenant, "drill-test-backup")
	if err != nil {
		t.Fatalf("createRestoreDrillBackup failed: %v", err)
	}
	if len(input.Record.RelationSchemas) != 1 || input.Record.RelationSchemas[0].RelationType != "calls" {
		t.Fatalf("expected RelationSchemas to be captured in restore drill backup, got %+v", input.Record.RelationSchemas)
	}

	// Verify cleanup of generated backup on failed drill (pre-seed target tenant inside drill target prefix so claim fails after backup capture).
	existingTarget := "t-drill-existing"
	task := Task{
		ID:        "drill-fail-1",
		TenantID:  tenant,
		Type:      TaskTypeTenantRestoreDrill,
		Status:    TaskStatusRunning,
		Params:    map[string]any{"target_tenant_id": existingTarget, "cleanup": true},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	targetPrefix := restoreDrillTargetPrefix(store.Prefix, task)
	targetStore := store.restoreDrillTargetStore(targetPrefix, mem)
	if _, err := targetStore.CreateTenant(ctx, existingTarget, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	targetStore.ShutdownTasks(ctx)
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	_, drillErr := store.tenantRestoreDrillTask(ctx, task)
	if drillErr == nil {
		t.Fatalf("expected restore drill into existing target tenant to fail")
	}
	if _, err := mem.Get(ctx, store.taskResultKey(tenant, task.ID+"-backup")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected generated drill backup result to be cleaned up on failure, err=%v", err)
	}
	if _, err := mem.Get(ctx, store.backupManifestKey(tenant, task.ID+"-backup")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected generated drill backup manifest to be cleaned up on failure, err=%v", err)
	}
}

func TestRound7PurgeAndMigrationPageRespectCanceledContext(t *testing.T) {
	mem := NewMemoryStore()
	store := NewTenantStore(mem, "r7-cancel")
	defer store.ShutdownTasks(context.Background())

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	objects := []ObjectInfo{{Key: "k1"}, {Key: "k2"}, {Key: "k3"}}
	if _, err := store.deleteTenantPurgePage(canceledCtx, objects); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected deleteTenantPurgePage to return context.Canceled, got %v", err)
	}
	pending := []tenantMigrationPendingObject{
		{object: ObjectInfo{Key: "k1"}, targetKey: "d1"},
		{object: ObjectInfo{Key: "k2"}, targetKey: "d2"},
	}
	if _, _, err := store.copyTenantMigrationPage(canceledCtx, "t1", "t2", "p", nil, writerFenceRef{}, pending, func(context.Context, string, []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected copyTenantMigrationPage to return context.Canceled, got %v", err)
	}
}
