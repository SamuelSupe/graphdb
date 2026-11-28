package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestApplyReplicatedPreCommitRollbackRestoresState(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("callback failed")
	_, err = files.ApplyReplicated(ctx, 1, "cmd-1", time.Unix(100, 0).UTC(), func(tx context.Context) ([]byte, error) {
		if err := files.Put(tx, "k1", []byte("v2")); err != nil {
			return nil, err
		}
		if err := files.Put(tx, "k2", []byte("new")); err != nil {
			return nil, err
		}
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected callback error, got %v", err)
	}
	got, err := files.Get(ctx, "k1")
	if err != nil || string(got) != "v1" {
		t.Fatalf("k1 after rollback: %q %v", got, err)
	}
	if _, err := files.Get(ctx, "k2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k2 should be deleted after rollback, got %v", err)
	}
}

func TestLocalFileChangedInvalidatesRegistryAndPurgeCaches(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "graphdb")
	if err := store.addTenantToRegistry(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if !store.isRegisteredTenantCached("tenant-a") {
		t.Fatal("expected tenant-a in registeredTenantCache")
	}
	store.localFileChanged("graphdb/tenants/_registry.parquet")
	if store.isRegisteredTenantCached("tenant-a") {
		t.Fatal("expected registeredTenantCache to be cleared")
	}

	store.lockMu.Lock()
	store.purgeTombstoneCache["tenant-a"] = cachedTenantPurgeTombstone{checkedAt: time.Now().UTC()}
	store.lockMu.Unlock()
	store.localFileChanged("graphdb/control/tenant-purges/tenant-a.parquet")
	store.lockMu.Lock()
	purgeLen := len(store.purgeTombstoneCache)
	store.lockMu.Unlock()
	if purgeLen != 0 {
		t.Fatalf("expected purgeTombstoneCache to be cleared, len=%d", purgeLen)
	}
}

type blockingGetStore struct {
	ObjectStore
	key     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == s.key {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.ObjectStore.Get(ctx, key)
}

func TestWriterObjectCacheGenerationFenceAndOversizeObject(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	if err := mem.Put(ctx, "small", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, "huge", bytes.Repeat([]byte("x"), 2048)); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, "raced", []byte("old")); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingGetStore{
		ObjectStore: mem,
		key:         "raced",
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	cache := NewWriterObjectCache(blocked, WriterObjectCacheConfig{MaxBytes: 512, MaxKeys: 16})

	// Warm "small", then read oversize "huge": "small" must NOT be evicted.
	if _, err := cache.Get(ctx, "small"); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(ctx, "huge"); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	_, smallKept := cache.objects["small"]
	_, hugeCached := cache.objects["huge"]
	cache.mu.Unlock()
	if !smallKept || hugeCached {
		t.Fatalf("oversize Get corrupted cache: smallKept=%v hugeCached=%v", smallKept, hugeCached)
	}

	// Start a slow Get on "raced", concurrently Put("raced", "new"), then release Get.
	// The stale Get must NOT overwrite "new" in the cache.
	done := make(chan error, 1)
	go func() {
		_, err := cache.Get(ctx, "raced")
		done <- err
	}()
	<-blocked.entered
	if err := cache.Put(ctx, "raced", []byte("new")); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err := cache.Get(ctx, "raced")
	if err != nil || string(got) != "new" {
		t.Fatalf("stale Get overwrote newer Put: got %q %v", got, err)
	}
}

func TestReaderCacheConcurrentReadHitsRaceFree(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host"}},
		UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	cache := NewReaderCache(store, time.Minute)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
					t.Errorf("cache load: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestDecodeParquetSnapshotRecordRejectsOutOfRangeOrdinal(t *testing.T) {
	ctx := context.Background()
	schema := codec.ParquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	var buf bytes.Buffer
	writerProps := parquetio.DataWriterProperties(schema)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	writer, err := pqarrow.NewFileWriter(schema, &buf, writerProps, arrowProps)
	if err != nil {
		t.Fatal(err)
	}
	header := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		ID:            "snapshot-record",
		Version:       1,
	}
	codec.AppendParquetCommitRow(builder, "tenant-a", "", header, "deadbeef", parquetCommitRow{
		Kind:          commitRowUpsertEntity,
		ParentOrdinal: 1_000_000,
	})
	batch := builder.NewRecordBatch()
	defer batch.Release()
	if err := writer.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = decodeParquetSnapshotRecord(ctx, buf.Bytes())
	if err == nil || !strings.Contains(err.Error(), "invalid parquet snapshot ordinal") {
		t.Fatalf("expected invalid parquet snapshot ordinal error, got %v", err)
	}
}

func TestApplyReplicatedDefersReaderPublicationAndClearsWrapperCachesOnRollback(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()

	protected := NewReadProtectedObjectStore(files, ReadProtectionConfig{MaxConcurrent: 4, Singleflight: true})
	cache := NewWriterObjectCache(protected, WriterObjectCacheConfig{MaxBytes: 16 << 20, MaxKeys: 1024, NegativeTTL: time.Minute})
	store := NewTenantStore(cache, "graphdb")
	readers := NewReaderCache(store, time.Minute)

	// Seed committed version 1 inside journal index 1.
	if _, err := files.ApplyReplicated(ctx, 1, "seed", time.Unix(100, 0).UTC(), func(tx context.Context) ([]byte, error) {
		_, err := store.Commit(tx, "tenant-a", graph.Mutations{
			UpsertCITypes:  []graph.CIType{{Name: "host"}},
			UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}},
		}, CommitOptions{})
		return []byte("v1"), err
	}); err != nil {
		t.Fatal(err)
	}
	g1, m1, err := readers.Load(ctx, "tenant-a")
	if err != nil || m1.Version != 1 || g1.Entities.Len() != 1 {
		t.Fatalf("initial reader load: v=%d len=%d err=%v", m1.Version, g1.Entities.Len(), err)
	}

	// Journal index 2 writes commit v2 + saved query, checks reader isolation mid-batch, then rejects.
	rejected := &ReplicationRejectedError{Response: []byte("conflict")}
	_, err = files.ApplyReplicated(ctx, 2, "batch-reject", time.Unix(101, 0).UTC(), func(tx context.Context) ([]byte, error) {
		if _, err := store.Commit(tx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}},
		}, CommitOptions{}); err != nil {
			return nil, err
		}
		if _, err := store.SaveQuery(tx, "tenant-a", SavedQuery{
			Name: "uncommitted-q",
		}); err != nil {
			// SaveQuery validates Request; write a raw object through cache to test wrapper rollback invalidation.
			if putErr := store.Objects.Put(tx, "graphdb/tenants/tenant-a/custom-temp.bin", []byte("uncommitted")); putErr != nil {
				return nil, putErr
			}
		}
		// Before journal commit, ReaderCache cached view must not have published v2 via publishGraph.
		if cachedVer, ok := readers.CachedVersion("tenant-a"); ok && cachedVer != 1 {
			t.Fatalf("uncommitted version %d leaked into ReaderCache mid-journal", cachedVer)
		}
		return nil, rejected
	})
	if err != rejected {
		t.Fatalf("expected rejected error, got %v", err)
	}

	if _, err := store.Objects.Get(ctx, "graphdb/tenants/tenant-a/custom-temp.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back object leaked through WriterObjectCache: %v", err)
	}
	gAfter, mAfter, err := readers.Load(ctx, "tenant-a")
	if err != nil || mAfter.Version != 1 || gAfter.Entities.Len() != 1 {
		t.Fatalf("reader after rollback saw dirty state: v=%d len=%d err=%v", mAfter.Version, gAfter.Entities.Len(), err)
	}
}

func TestRetainedCheckpointMaterializedOnCleanCloseAndCacheListPreserved(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	retainedCtx := WithCheckpointInReplicationJournal(ctx)
	if _, err := files.ApplyReplicated(retainedCtx, 7, "p13", time.Unix(200, 0).UTC(), func(tx context.Context) ([]byte, error) {
		return []byte("committed-7"), files.Put(tx, "graphdb/k", []byte("v"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	cp, err := NewFileStore(root).ReplicationCheckpoint()
	if err != nil || cp.Index != 7 || string(cp.Response) != "committed-7" {
		t.Fatalf("clean Close did not materialize retained checkpoint: %+v, err=%v", cp, err)
	}

	// NegativeTTL == 0 missing-key Get must not evict cached ancestor Lists.
	mem := NewMemoryStore()
	if err := mem.Put(ctx, "tenants/t1/a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	wcache := NewWriterObjectCache(mem, WriterObjectCacheConfig{MaxBytes: 1 << 20, MaxKeys: 64, NegativeTTL: 0})
	if items, err := wcache.List(ctx, "tenants/t1/"); err != nil || len(items) != 1 {
		t.Fatalf("warm list: %+v %v", items, err)
	}
	if _, err := wcache.Get(ctx, "tenants/t1/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	wcache.mu.Lock()
	_, listStillCached := wcache.lists["tenants/t1/"]
	wcache.mu.Unlock()
	if !listStillCached {
		t.Fatal("NegativeTTL=0 missing Get evicted cached prefix list")
	}

	// Millisecond backpressure config overflow must be rejected.
	tstore := NewTenantStore(NewMemoryStore(), "test")
	overflowMS := MaxDurationMilliseconds + 1
	if _, err := tstore.PutTenantConfig(ctx, "tenant-a", TenantConfig{
		Backpressure: TenantBackpressureConfig{
			ObjectLatencyThresholdMS: &overflowMS,
		},
	}); err == nil {
		t.Fatal("expected overflow ObjectLatencyThresholdMS to fail validation")
	}
}

func TestSingleTenantWALHeadOfLineDrainsUnderTightReserve(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	config.WAL.MaxBytes = 8192
	config.WAL.SegmentBytes = 8192
	config.WAL.ControlReserveBytes = 512
	config.QueueMemoryBytes = 7400

	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	var accepted []IngestAcceptance
	for i := range 6 {
		req := ingestEntityRequest("hol-"+string(rune('a'+i)), "host:"+string(rune('a'+i)))
		req.Items[0].Entity.Fields = graph.Fields{"payload": strings.Repeat("p", 800)}
		a, acceptErr := service.Accept(ctx, "tenant-a", req)
		if errors.Is(acceptErr, ErrIngestWALFull) || errors.Is(acceptErr, ErrIngestQueueFull) {
			break
		}
		if acceptErr != nil {
			t.Fatalf("accept %d: %v", i, acceptErr)
		}
		accepted = append(accepted, a)
	}
	if len(accepted) < 3 {
		t.Fatalf("expected multiple accepted requests queued, got %d", len(accepted))
	}
	if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("FlushTenant deadlocked or failed: %v", err)
	}
	for i, a := range accepted {
		res, waitErr := service.WaitCommitted(ctx, a)
		if waitErr != nil || res.Applied != 1 {
			t.Fatalf("item %d did not commit: res=%+v err=%v", i, res, waitErr)
		}
	}
}
