package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
)

func TestDirectorySyncBarrierWaitsForInFlightSync(t *testing.T) {
	var barrier directorySyncBarrier
	dir := t.TempDir()

	barrier.mark(dir)

	// Simulate an in-flight syncDir for generation 1.
	w := &dirSyncWaiter{done: make(chan struct{})}
	barrier.mu.Lock()
	barrier.dirs[dir].inFlight = w
	barrier.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- barrier.drain(nil, false)
	}()

	select {
	case err := <-done:
		t.Fatalf("drain returned early while syncDir was in flight: err=%v", err)
	case <-time.After(30 * time.Millisecond):
	}

	// Mark the directory dirty again while generation 1 is still in flight;
	// release generation 1 and verify drain completes cleanly after syncing gen 2.
	barrier.mark(dir)
	barrier.mu.Lock()
	barrier.dirs[dir].synced = 1
	barrier.dirs[dir].inFlight = nil
	close(w.done)
	barrier.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil drain error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain timed out waiting for in-flight directory sync")
	}

	// Write barrier (allowMissing=false) must surface ENOENT so publications never bypass an unsynced directory.
	missingDir := filepath.Join(dir, "does-not-exist")
	barrier.mark(missingDir)
	if err := barrier.drain(nil, false); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected ENOENT on write barrier, got %v", err)
	}
	if barrier.pendingCount() != 1 {
		t.Fatalf("expected missing write dir to remain pending for retry, pending=%d", barrier.pendingCount())
	}

	// Delete barrier (allowMissing=true) must drain ENOENT cleanly without poisoning subsequent GC passes.
	var deleteBarrier directorySyncBarrier
	removedDir := filepath.Join(dir, "already-removed")
	deleteBarrier.mark(removedDir)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- deleteBarrier.drain(nil, true)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("expected ENOENT delete dir to drain cleanly without poisoning barrier, got %v", err)
		}
	}
	if deleteBarrier.pendingCount() != 0 {
		t.Fatalf("expected delete barrier to be clear after ENOENT drain, pending=%d", deleteBarrier.pendingCount())
	}
}

func TestGCCursorAdvancedHandlesShardedSnapshotTransitions(t *testing.T) {
	cases := []struct {
		name string
		prev string
		next string
		want bool
	}{
		{
			name: "sharded v5 to anchor v5",
			prev: "tenants/t/snapshots/sharded/v5/chunk-0001.parquet",
			next: "tenants/t/snapshots/00000000000000000005.parquet",
			want: true,
		},
		{
			name: "sharded v5 to anchor v6",
			prev: "tenants/t/snapshots/sharded/v5/chunk-0001.parquet",
			next: "tenants/t/snapshots/00000000000000000006.parquet",
			want: true,
		},
		{
			name: "sharded v9 to sharded v10 numeric order",
			prev: "tenants/t/snapshots/sharded/v9/chunk-0001.parquet",
			next: "tenants/t/snapshots/sharded/v10/chunk-0000.parquet",
			want: true,
		},
		{
			name: "sharded v5 to same key stalled",
			prev: "tenants/t/snapshots/sharded/v5/chunk-0001.parquet",
			next: "tenants/t/snapshots/sharded/v5/chunk-0001.parquet",
			want: false,
		},
		{
			name: "sharded v5 back to anchor v4 rejected",
			prev: "tenants/t/snapshots/sharded/v5/chunk-0001.parquet",
			next: "tenants/t/snapshots/00000000000000000004.parquet",
			want: false,
		},
		{
			name: "sharded v5 to sharded orphans phase",
			prev: "tenants/t/snapshots/sharded/v5/chunk-0001.parquet",
			next: "tenants/t/snapshots/~sharded-orphans/v1/chunk-0000.parquet",
			want: true,
		},
	}
	for _, tc := range cases {
		if got := gcCursorAdvanced(tc.prev, tc.next); got != tc.want {
			t.Errorf("%s: gcCursorAdvanced(%q, %q) = %v, want %v", tc.name, tc.prev, tc.next, got, tc.want)
		}
	}
}

type failOnceObjectStore struct {
	ObjectStore
	failPut error
}

func (f *failOnceObjectStore) Put(ctx context.Context, key string, data []byte) error {
	if f.failPut != nil {
		return f.failPut
	}
	return f.ObjectStore.Put(ctx, key, data)
}

func TestWriterObjectCacheInvalidatesOnNonConflictError(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	wrapper := &failOnceObjectStore{ObjectStore: mem}
	cache := NewWriterObjectCache(wrapper, WriterObjectCacheConfig{MaxBytes: 1 << 20})

	if err := cache.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	// Underlying store mutates outside cache, then Put fails with I/O error.
	if err := mem.Put(ctx, "k1", []byte("v2")); err != nil {
		t.Fatalf("direct Put: %v", err)
	}
	wrapper.failPut = errors.New("simulated network timeout")
	if err := cache.Put(ctx, "k1", []byte("v3")); err == nil {
		t.Fatal("expected Put error")
	}
	wrapper.failPut = nil
	got, err := cache.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get after failed Put: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("expected fresh value %q after invalidation, got stale %q", "v2", string(got))
	}
}

func TestReaderCachePublishGraphEvictsStaleEntryWhenOversized(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "")
	cache := NewReaderCache(store, time.Minute)

	g1 := graph.New()
	if err := g1.ApplyCommit(graph.Commit{
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "ci:1", Kind: "host", Fields: graph.Fields{"name": "small"}}},
		},
	}); err != nil {
		t.Fatalf("seed g1: %v", err)
	}
	loaded1 := loadedGraph{Graph: g1, Manifest: Manifest{Version: 1}}
	cache.MaxBytes = readerCacheEntryBytes(loaded1) + 256
	if !cache.publishGraph("tenant-a", loaded1) {
		t.Fatal("expected small graph publish to succeed")
	}
	if v, ok := cache.CachedVersion("tenant-a"); !ok || v != 1 {
		t.Fatalf("expected cached version 1, got %d (%v)", v, ok)
	}

	g2 := g1.Clone()
	bigVal := string(make([]byte, int(cache.MaxBytes)+4096))
	if err := g2.ApplyCommit(graph.Commit{
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "ci:2", Kind: "host", Fields: graph.Fields{"blob": bigVal}}},
		},
	}); err != nil {
		t.Fatalf("seed g2: %v", err)
	}
	if cache.publishGraph("tenant-a", loadedGraph{Graph: g2, Manifest: Manifest{Version: 2}}) {
		t.Fatal("expected oversized graph publish to fail")
	}
	if _, ok := cache.CachedVersion("tenant-a"); ok {
		t.Fatal("expected stale v1 entry to be evicted when v2 publish fails")
	}
	_ = ctx
}

func TestReverseIndexGCRespectsPinnedReadView(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	defer files.Close()
	store := NewTenantStore(files, "")

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "ci:a", Kind: "host"},
			{ID: "ci:b", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{
			{ID: "edge:1", Type: "runs_on", From: "ci:a", To: "ci:b"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild 1: %v", err)
	}
	revV1, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 1)
	if err != nil || len(revV1.EdgeShards) == 0 || len(revV1.EdgeShards[0].Objects) == 0 {
		t.Fatalf("reverse catalog v1 missing: err=%v cat=%+v", err, revV1)
	}
	v1ShardKey := revV1.EdgeShards[0].Objects[0].Key

	// Pin a read view at v1 before v2 is committed and rebuilt.
	unpin, err := store.PinReadView(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("PinReadView: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "ci:c", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{
			{ID: "edge:2", Type: "runs_on", From: "ci:a", To: "ci:c"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild 2: %v", err)
	}

	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true})
	if err != nil {
		t.Fatalf("RunGC while pinned: %v", err)
	}
	if _, err := files.Get(ctx, v1ShardKey); err != nil {
		t.Fatalf("v1 reverse shard %q was deleted while read view was pinned: %v (report=%+v)", v1ShardKey, err, report)
	}
	unpin()

	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("RunGC after unpin: %v", err)
	}
	if _, err := files.Get(ctx, v1ShardKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected v1 reverse shard %q to be deleted after unpin, got err=%v", v1ShardKey, err)
	}
}

func TestStreamingSnapshotRecordContentHashMatchesPayloadJSON(t *testing.T) {
	cases := []struct {
		name string
		rec  snapshotRecord
	}{
		{
			name: "empty snapshot",
			rec: snapshotRecord{
				TenantID: "tenant-empty",
				Snapshot: graph.Snapshot{Version: 1},
			},
		},
		{
			name: "populated snapshot",
			rec: snapshotRecord{
				TenantID: "tenant-pop",
				Snapshot: graph.Snapshot{
					Version: 7,
					Entities: []graph.Entity{
						{ID: "ci:a", Kind: "host", Fields: graph.Fields{"env": "prod", "tags": []any{"web", "canary"}}},
						{ID: "ci:b", Kind: "service", Fields: graph.Fields{"tier": 1}},
					},
					RelationTypes: []graph.RelationType{
						{Name: "runs_on", ImpactDirection: "reverse"},
					},
					Edges: []graph.Edge{
						{ID: "edge:1", Type: "runs_on", From: "ci:b", To: "ci:a", Fields: graph.Fields{"weight": 0.5}},
					},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := snapshotRecordPayloadJSON(tc.rec)
			if err != nil {
				t.Fatalf("snapshotRecordPayloadJSON: %v", err)
			}
			want := codec.ContentHash(payload)
			got, err := snapshotRecordContentHash(tc.rec)
			if err != nil {
				t.Fatalf("snapshotRecordContentHash: %v", err)
			}
			if got != want {
				t.Fatalf("streaming hash mismatch: got %q want %q", got, want)
			}
		})
	}
}

func TestMergeGCReportBoundsKeySamples(t *testing.T) {
	var total GCReport
	chunk := make([]string, 3000)
	for i := range chunk {
		chunk[i] = "tenants/t/snapshots/orphan"
	}
	for i := 0; i < 5; i++ {
		mergeGCReport(&total, GCReport{
			TenantID:         "tenant-a",
			DeletedSnapshots: len(chunk),
			DeletedKeys:      chunk,
			Checkpoint: GCCheckpoint{
				Deleted:     len(chunk),
				Planned:     len(chunk),
				DeletedKeys: chunk,
				PlannedKeys: chunk,
			},
		})
	}
	if total.DeletedSnapshots != 15000 || total.Checkpoint.Deleted != 15000 || total.Checkpoint.Planned != 15000 {
		t.Fatalf("expected exact counts 15000, got snapshots=%d deleted=%d planned=%d", total.DeletedSnapshots, total.Checkpoint.Deleted, total.Checkpoint.Planned)
	}
	if len(total.DeletedKeys) != gcReportKeySampleLimit || len(total.Checkpoint.DeletedKeys) != gcReportKeySampleLimit || len(total.Checkpoint.PlannedKeys) != gcReportKeySampleLimit {
		t.Fatalf("expected key slices capped at %d, got keys=%d cpDeleted=%d cpPlanned=%d", gcReportKeySampleLimit, len(total.DeletedKeys), len(total.Checkpoint.DeletedKeys), len(total.Checkpoint.PlannedKeys))
	}
}

func TestReaderCacheCloseCancelsDetachedStoreLoad(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "")
	cache := NewReaderCache(store, time.Minute)
	loadCtx, cancel := context.WithCancel(context.Background())
	load := &cacheLoad{done: make(chan struct{})}
	cache.mu.Lock()
	cache.loading["tenant-a"] = load
	cache.mu.Unlock()
	cache.setLoadCancel(load, cancel, true)

	store.StopBackground()
	select {
	case <-loadCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected TenantStore.StopBackground -> ReaderCache.Close to cancel active detached store load")
	}

	// Late setLoadCancel after Close must cancel immediately.
	lateCtx, lateCancel := context.WithCancel(context.Background())
	lateLoad := &cacheLoad{done: make(chan struct{})}
	cache.setLoadCancel(lateLoad, lateCancel, true)
	select {
	case <-lateCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected setLoadCancel on closed ReaderCache to cancel immediately")
	}
}

func TestReadViewContextCancelableCanceledByExclusiveLock(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	defer files.Close()
	store := NewTenantStore(files, "")

	streamCtx, releaseStream, err := store.ReadViewContextCancelable(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("ReadViewContextCancelable: %v", err)
	}
	streamExited := make(chan struct{})
	go func() {
		defer close(streamExited)
		<-streamCtx.Done()
		releaseStream()
	}()

	exclCtx, cancelExcl := context.WithTimeout(ctx, 2*time.Second)
	defer cancelExcl()
	releaseExcl, err := store.lockReadViews(exclCtx, "tenant-a", true)
	if err != nil {
		t.Fatalf("expected exclusive lock to cancel streaming read view promptly, got %v", err)
	}
	releaseExcl()
	<-streamExited
}

func TestValidateTenantMigrationSourceRejectsFutureIndexCatalog(t *testing.T) {
	ctx := context.Background()
	files := NewFileStore(t.TempDir())
	defer files.Close()
	store := NewTenantStore(files, "")

	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"}); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host"}},
		UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("Commit v1: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("RebuildIndexes: %v", err)
	}
	// Poison catalog.parquet with a future version > manifest.Version (1).
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("GetIndexCatalog: %v", err)
	}
	catalog.Version = 2
	raw, err := marshalParquetIndexCatalog(ctx, catalog)
	if err != nil {
		t.Fatalf("marshalParquetIndexCatalog: %v", err)
	}
	if err := files.Put(ctx, store.tenantObjectPrefix("tenant-a")+"indexes/catalog.parquet", raw); err != nil {
		t.Fatalf("Put poisoned catalog: %v", err)
	}
	if err := ValidateTenantMigrationSource(ctx, store, "tenant-a", nil); err == nil {
		t.Fatal("expected ValidateTenantMigrationSource to reject index catalog version > manifest version")
	}
	if !tenantMigrationRelativeExceedsVersion("tenants/tenant-a/snapshots/00000000000000000002.parquet", "snapshots/00000000000000000002.parquet", 1) {
		t.Fatal("expected future snapshot key to be flagged as exceeding version")
	}
	if !tenantMigrationRelativeExceedsVersion("tenants/tenant-a/indexes/parquet/versions/v2/catalog.parquet", "indexes/parquet/versions/v2/catalog.parquet", 1) {
		t.Fatal("expected future versioned index key to be flagged as exceeding version")
	}
	if tenantMigrationRelativeExceedsVersion("tenants/tenant-a/snapshots/00000000000000000001.parquet", "snapshots/00000000000000000001.parquet", 1) {
		t.Fatal("expected current version snapshot key to be allowed")
	}
}

func TestValidateMigrationReceiptDBRejectsCorruptDBSafely(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	key := "tenants/tenant-a/" + receiptDatabase
	if err := mem.Put(ctx, key, []byte("not-a-valid-bolt-db")); err != nil {
		t.Fatal(err)
	}
	if err := validateMigrationReceiptDB(ctx, mem, key); err == nil {
		t.Fatal("expected corrupt receipt DB bytes to be rejected")
	}
	if err := mem.Put(ctx, key, []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := validateMigrationReceiptDB(ctx, mem, key); err == nil {
		t.Fatal("expected empty receipt DB to be rejected")
	}
}

func TestPublishGraphOversizedSupersedesInFlightLoad(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "")
	cache := NewReaderCache(store, time.Minute)
	cache.MaxBytes = 128

	loadCtx, cancel := context.WithCancel(context.Background())
	load := &cacheLoad{
		done:        make(chan struct{}),
		phaseCtx:    context.Background(),
		cancelPhase: func() {},
		cancel:      cancel,
		independent: true,
	}
	cache.mu.Lock()
	cache.loading["tenant-a"] = load
	cache.mu.Unlock()

	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:        "c1",
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "ci:1", Kind: "host", Fields: graph.Fields{"blob": string(make([]byte, 4096))}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if cache.publishGraph("tenant-a", loadedGraph{Graph: g, Manifest: Manifest{Version: 1}}) {
		t.Fatal("expected oversized publishGraph to return false")
	}
	select {
	case <-loadCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected oversized publishGraph to cancel in-flight load")
	}
	if err := cache.finishLoad("tenant-a", load, context.Canceled); err != nil {
		t.Fatalf("expected superseded load finishLoad to clear context.Canceled, got %v", err)
	}
	if load.err != nil {
		t.Fatalf("expected load.err == nil for superseded waiters, got %v", load.err)
	}
}

func TestBatchedDeleteLooseReceiptWithPackedReceiptDBDoesNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	defer files.Close()
	files.receiptPrefix = "tenants/"
	store := NewTenantStore(files, "")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"}); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("Commit schema: %v", err)
	}
	// Create a packed ingest receipt so receipts.v1.db exists on disk.
	if _, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "cmdb",
		CollectorID: "c1",
		BatchID:     "b1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// Seed a loose direct-commit idempotency receipt file alongside receipts.v1.db
	// (simulating an uncompacted direct commit receipt).
	loosePath := filepath.Join(root, "tenants", "tenant-a", "idempotency", "commits", "loose.parquet")
	if err := os.MkdirAll(filepath.Dir(loosePath), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(loosePath, []byte("PAR1"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "tenants", "tenant-a", receiptDatabase)); err != nil {
		t.Fatalf("expected receipts.v1.db to exist before purge: %v", err)
	}
	// PurgeTenant deletes loose receipts using gcDeleteBatchKey while receipts.v1.db
	// still exists; must complete cleanly without self-deadlocking on publicationMu.
	if _, err := store.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("PurgeTenant: %v", err)
	}
}

func TestApplySecondaryIndexValueDeltaBatchedMerge(t *testing.T) {
	values := map[string][]string{
		"s:prod": {"a", "c", "e", "g"},
	}
	deltaE, deltaD, err := applySecondaryIndexValueDelta(values, "s:prod", []string{"g", "a"}, []string{"f", "b", "d"}, "host", "env")
	if err != nil {
		t.Fatalf("applySecondaryIndexValueDelta: %v", err)
	}
	if deltaE != 1 || deltaD != 0 {
		t.Fatalf("unexpected deltas: entries=%d distinct=%d", deltaE, deltaD)
	}
	want := []string{"b", "c", "d", "e", "f"}
	got := values["s:prod"]
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	// Duplicate add must fail.
	if _, _, err := applySecondaryIndexValueDelta(values, "s:prod", nil, []string{"c"}, "host", "env"); err == nil {
		t.Fatal("expected duplicate add error")
	}
	// Missing remove must fail.
	if _, _, err := applySecondaryIndexValueDelta(values, "s:prod", []string{"z"}, nil, "host", "env"); err == nil {
		t.Fatal("expected missing remove error")
	}
	// Removing all entries must delete key and decrement distinct count.
	deltaE, deltaD, err = applySecondaryIndexValueDelta(values, "s:prod", append([]string(nil), want...), nil, "host", "env")
	if err != nil || deltaE != -5 || deltaD != -1 {
		t.Fatalf("emptying delta = (%d, %d, %v), want (-5, -1, nil)", deltaE, deltaD, err)
	}
	if _, exists := values["s:prod"]; exists {
		t.Fatal("expected emptied value key to be deleted")
	}
	// Creating new value must increment distinct count.
	deltaE, deltaD, err = applySecondaryIndexValueDelta(values, "s:stage", nil, []string{"z", "a"}, "host", "env")
	if err != nil || deltaE != 2 || deltaD != 1 || len(values["s:stage"]) != 2 || values["s:stage"][0] != "a" || values["s:stage"][1] != "z" {
		t.Fatalf("create delta = (%d, %d, %v) values=%v", deltaE, deltaD, err, values["s:stage"])
	}
}
