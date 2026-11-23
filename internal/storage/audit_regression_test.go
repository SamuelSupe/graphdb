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
