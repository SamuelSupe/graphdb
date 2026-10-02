package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestDiskPressurePreservesReadsDrainAndCleanup(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.ConfigureDiskSpace(DiskSpacePolicy{MinFreeBytes: 256 << 20}); err != nil {
		t.Fatal(err)
	}
	available := int64(1 << 30)
	files.diskProbe = func(string) (DiskSpaceStatus, error) {
		return DiskSpaceStatus{TotalBytes: 2 << 30, AvailableBytes: available}, nil
	}
	store := NewTenantStore(files, "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}
	before, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	available = 128 << 20
	_, err = store.Commit(ctx, "tenant-a", mutations, CommitOptions{})
	assertBackpressureReason(t, err, "disk_space_low")
	_, err = store.Compact(ctx, "tenant-a")
	assertBackpressureReason(t, err, "disk_space_low")
	if err := store.CheckAcceptedWALBackpressure(ctx, "tenant-a"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != before.Version {
		t.Fatalf("read after rejection: %#v, %v", manifest, err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{DryRun: true}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	available = 32 << 20
	assertBackpressureReason(t, store.CheckAcceptedWALBackpressure(ctx, "tenant-a"), "disk_space_low")
	files.diskProbe = func(string) (DiskSpaceStatus, error) {
		t.Fatal("replica application consulted local admission")
		return DiskSpaceStatus{}, nil
	}
	if err := store.CheckWriteBackpressure(ReplicatedContext(ctx, "replicated", time.Now()), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	files.diskProbe = func(string) (DiskSpaceStatus, error) { return DiskSpaceStatus{AvailableBytes: 1 << 30}, nil }
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatalf("resume: %v", err)
	}
}

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

	store.taskActive[taskActiveKey("tenant-a", TaskTypeIndexRebuild)] = Task{ID: "task-a", TenantID: "tenant-a", Type: TaskTypeIndexRebuild, Status: TaskStatusRunning}
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
