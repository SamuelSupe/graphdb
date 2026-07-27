package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCommitBackpressureRejectsHighObjectLatency(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	pressure := NewWritePressure(BackpressureConfig{ObjectLatencyThreshold: time.Millisecond})
	pressure.RecordObjectLatency(2 * time.Millisecond)
	store.Backpressure = pressure

	_, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "object_store_latency_high")
}

func TestCommitBackpressureRejectsManifestConflictSpike(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	pressure := NewWritePressure(BackpressureConfig{CASConflictThreshold: 1})
	pressure.RecordManifestCASConflict("tenant-a")
	store.Backpressure = pressure

	_, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "manifest_cas_conflicts_high")
}

func TestCommitBackpressureRejectsRecentObjectStoreErrors(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	pressure := NewWritePressure(BackpressureConfig{ObjectErrorThreshold: 1, ObjectErrorWindow: time.Minute})
	pressure.RecordObjectOperation(time.Millisecond, ErrObjectStoreUnavailable)
	store.Backpressure = pressure

	_, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "object_store_errors_high")
}

func TestCommitBackpressureRejectsHighTenantObjectCountFromUsageSample(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxObjectsPerTenant: 1})
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.TenantUsage(ctx, "tenant-a"); err != nil {
		t.Fatalf("usage: %v", err)
	}

	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "tenant_object_count_high")
}

func TestCommitBackpressureMapsAdmissionObjectStoreFailure(t *testing.T) {
	store := NewTenantStore(&unavailableGetMetaStore{ObjectStore: NewMemoryStore()}, "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{})

	_, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "object_store_unavailable")
}

func TestCommitBackpressureRecordsManifestCASConflict(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &takeoverOnManifestPutStore{ObjectStore: base, base: base, tenantID: "tenant-a"}
	store := NewTenantStore(objects, "test")
	store.LeaseTTL = time.Hour
	store.MaxRetries = 2
	store.Backpressure = NewWritePressure(BackpressureConfig{CASConflictThreshold: 1})

	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{})
	if err == nil {
		t.Fatal("commit succeeded, want takeover conflict path")
	}
	if !objects.triggered {
		t.Fatal("test store did not trigger takeover")
	}

	_, err = store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:next", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "manifest_cas_conflicts_high")
}

func TestCommitBackpressureRejectsDuringIndexRebuild(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{})
	now := time.Now().UTC()
	if err := store.saveIndexTask(ctx, IndexTask{
		ID: "task-a", TenantID: "tenant-a", Type: "rebuild", Status: "running",
		StartedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("save task: %v", err)
	}

	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "index_rebuild_running")
}

func TestCommitBackpressureIndexRebuildCheckDoesNotScanHistoricalTasks(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newCountingListStore(base)
	store := NewTenantStore(objects, "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{})
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		taskID := fmt.Sprintf("old-index-task-%d", i)
		if err := putIndexTaskFixture(ctx, store, store.indexTaskKey("tenant-a", taskID), IndexTask{
			ID:         taskID,
			TenantID:   "tenant-a",
			Type:       "rebuild",
			Status:     "succeeded",
			StartedAt:  now.Add(-time.Hour),
			UpdatedAt:  now.Add(-time.Hour),
			FinishedAt: now.Add(-time.Hour),
		}); err != nil {
			t.Fatalf("seed historical index task: %v", err)
		}
	}

	objects.reset()
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := objects.count(store.indexTaskPrefix("tenant-a")); got != 0 {
		t.Fatalf("index task list count = %d, want 0", got)
	}
}

func TestCommitBackpressureRejectsDuringGC(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{})
	now := time.Now().UTC()
	if err := store.saveTask(ctx, Task{
		ID: "task-gc", TenantID: "tenant-a", Type: TaskTypeGC, Status: "running",
		StartedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("save task: %v", err)
	}

	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "gc_running")
}

func TestCommitBackpressureRejectsLongCommitTail(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 1})
	for _, id := range []string{"host:a", "host:b"} {
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: id, Kind: "host"}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host"}},
	}, CommitOptions{})
	assertBackpressureReason(t, err, "commit_tail_too_long")
}

func TestCoordinatedBackpressureDoesNotTrustStaleWriteCacheTail(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	current := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       3,
		CommitKeys:    []string{"commit-2", "commit-3"},
	}
	data, err := marshalParquetManifest(ctx, current)
	if err != nil {
		t.Fatalf("marshal current manifest: %v", err)
	}
	key := "test/tenants/tenant-a/coordination/manifests/current.parquet"
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put current manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:     "tenant-a",
		Status:       TenantStatusActive,
		Generation:   1,
		Revision:     3,
		GraphVersion: 3,
		ManifestKey:  key,
		ManifestHash: objectContentHash(data),
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(&mutableHeadCoordinator{head: head})
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 1})
	stale := cachedGraph(1)
	stale.Manifest.TenantID = "tenant-a"
	stale.Manifest.CommitKeys = []string{"commit-1"}
	stale.Meta = coordinatedManifestMeta("stale.parquet", CoordinationHead{
		Generation: 1, Revision: 1,
	})
	store.setWriteCache("tenant-a", stale)

	err = store.CheckWriteBackpressure(ctx, "tenant-a")
	assertBackpressureReason(t, err, "commit_tail_too_long")
}

func TestCoordinatedBackpressureCurrentHeadKeepsCacheHit(t *testing.T) {
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Status:               TenantStatusActive,
		Generation:           2,
		Revision:             7,
		GraphVersion:         5,
		WriteContextRevision: 3,
		ManifestKey:          "current.parquet",
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(&mutableHeadCoordinator{head: head})
	current := cachedGraph(5)
	current.Manifest.TenantID = "tenant-a"
	current.Meta = coordinatedManifestMeta(head.ManifestKey, head)
	store.setWriteCache("tenant-a", current)

	manifest, err := store.currentManifestForWriteAdmission(
		context.Background(), "tenant-a",
	)
	if err != nil {
		t.Fatalf("current manifest: %v", err)
	}
	if manifest.Version != 5 {
		t.Fatalf("manifest version = %d, want 5", manifest.Version)
	}
}

func TestLocalBackpressureDoesNotTrustCacheAfterWriterTakeover(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	original := NewTenantStore(objects, "test")
	if _, err := original.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit original version: %v", err)
	}
	if err := expireWriterLeaseForTakeover(ctx, objects, "tenant-a"); err != nil {
		t.Fatalf("expire original writer lease: %v", err)
	}
	replacement := NewTenantStore(objects, "test")
	if _, err := replacement.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit replacement version: %v", err)
	}
	lease, meta, ok := original.getCachedWriterLeaseAny("tenant-a")
	if !ok {
		t.Fatal("original writer lease was not cached")
	}
	lease.ExpiresAt = time.Now().UTC().Add(-time.Second)
	original.setCachedWriterLease("tenant-a", lease, meta)
	original.Backpressure = NewWritePressure(
		BackpressureConfig{MaxCommitTail: 1},
	)

	err := original.CheckWriteBackpressure(ctx, "tenant-a")
	assertBackpressureReason(t, err, "commit_tail_too_long")
}

func TestCommitBackpressureQuotaBlocksGrowthButAllowsReduction(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxEntitiesPerTenant: 1})

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host"}},
	}, CommitOptions{}); err == nil {
		t.Fatal("growth commit succeeded, want quota backpressure")
	} else {
		assertBackpressureReason(t, err, "tenant_entity_quota_exceeded")
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		DeleteEntities: []string{"host:b"},
	}, CommitOptions{}); err != nil {
		t.Fatalf("reduction commit: %v", err)
	}
}

func TestIngestBackpressureDoesNotCreateDeadLetter(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	pressure := NewWritePressure(BackpressureConfig{ObjectLatencyThreshold: time.Millisecond})
	pressure.RecordObjectLatency(2 * time.Millisecond)
	store.Backpressure = pressure

	_, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source: "agent", CollectorID: "collector-a",
		Items: []IngestItem{{Entity: &graph.Entity{ID: "host:a", Kind: "host"}}},
	})
	assertBackpressureReason(t, err, "object_store_latency_high")
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("deadletters = %d, want 0", len(letters))
	}
}

func assertBackpressureReason(t *testing.T, err error, code string) {
	t.Helper()
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	var pressure *BackpressureError
	if !errors.As(err, &pressure) {
		t.Fatalf("err = %T, want BackpressureError", err)
	}
	for _, reason := range pressure.Reasons {
		if reason.Code == code {
			return
		}
	}
	t.Fatalf("reasons = %#v, want %q", pressure.Reasons, code)
}

type unavailableGetMetaStore struct {
	ObjectStore
}

func (s *unavailableGetMetaStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	return nil, ObjectMeta{Key: key}, ErrObjectStoreUnavailable
}

type countingListStore struct {
	ObjectStore
	mu     sync.Mutex
	counts map[string]int
}

func newCountingListStore(inner ObjectStore) *countingListStore {
	return &countingListStore{ObjectStore: inner, counts: map[string]int{}}
}

func (s *countingListStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	s.mu.Lock()
	s.counts[prefix]++
	s.mu.Unlock()
	return s.ObjectStore.List(ctx, prefix)
}

func (s *countingListStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts = map[string]int{}
}

func (s *countingListStore) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[prefix]
}
