package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingIndexCatalogStore struct {
	ObjectStore
	release chan struct{}
	started chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (s *blockingIndexCatalogStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if strings.Contains(key, "/indexes/catalog.parquet") {
		s.calls.Add(1)
		s.once.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ObjectMeta{}, ctx.Err()
		}
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func TestIndexCatalogAtVersionRevalidatesSameVersion(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = 10 * time.Millisecond
	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})

	first, err := store.GetIndexCatalogAtVersion(ctx, "tenant-a", 1)
	if err != nil || len(first.Indexes) != 0 {
		t.Fatalf("first catalog = %#v, err %v", first, err)
	}
	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
		Indexes: []IndexSpec{{
			Name:   "host.name",
			Kind:   "host",
			Field:  "name",
			Type:   "string",
			Status: "ready",
		}},
	})
	time.Sleep(20 * time.Millisecond)

	refreshed, err := store.GetIndexCatalogAtVersion(ctx, "tenant-a", 1)
	if err != nil || len(refreshed.Indexes) != 1 {
		t.Fatalf("refreshed catalog = %#v, err %v", refreshed, err)
	}
	if reads := objects.countContains("/indexes/catalog.parquet"); reads != 2 {
		t.Fatalf("catalog reads = %d, want one initial and one refresh", reads)
	}
}

func TestIndexCatalogVersionMismatchUsesBoundedRevalidation(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = time.Minute
	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})

	for range 2 {
		catalog, err := store.GetIndexCatalogAtVersion(
			ctx, "tenant-a", 2,
		)
		if err != nil || catalog.Version != 1 {
			t.Fatalf("stale catalog = %#v, err %v", catalog, err)
		}
	}
	if reads := objects.countContains("/indexes/catalog.parquet"); reads != 1 {
		t.Fatalf("catalog reads = %d, want one cached read", reads)
	}
}

func TestIndexCatalogMissingUsesBoundedRevalidation(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = 10 * time.Millisecond

	for range 2 {
		if _, err := store.GetIndexCatalogAtVersion(
			ctx, "tenant-a", 1,
		); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing catalog error = %v, want ErrNotFound", err)
		}
	}
	if reads := objects.countContains("/indexes/catalog.parquet"); reads != 1 {
		t.Fatalf("missing catalog reads = %d, want one cached read", reads)
	}

	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})
	time.Sleep(20 * time.Millisecond)
	catalog, err := store.GetIndexCatalogAtVersion(ctx, "tenant-a", 1)
	if err != nil || catalog.Version != 1 {
		t.Fatalf("revalidated catalog = %#v, err %v", catalog, err)
	}
	if reads := objects.countContains("/indexes/catalog.parquet"); reads != 2 {
		t.Fatalf("revalidated catalog reads = %d, want 2", reads)
	}
}

func TestScanCatalogReusesDecodedCatalogWithinTTL(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = time.Minute
	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})

	for range 2 {
		catalog, indexed, err := store.scanCatalog(ctx, "tenant-a", 1)
		if err != nil || !indexed || catalog.Version != 1 {
			t.Fatalf(
				"scan catalog = %#v, indexed %t, err %v",
				catalog, indexed, err,
			)
		}
	}
	if reads := objects.countContains("/indexes/catalog.parquet"); reads != 1 {
		t.Fatalf("scan catalog reads = %d, want one cached read", reads)
	}
}

func TestIndexCatalogConcurrentMissSharesDecode(t *testing.T) {
	const readers = 16
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &blockingIndexCatalogStore{
		ObjectStore: base,
		release:     make(chan struct{}),
		started:     make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})

	start := make(chan struct{})
	errs := make(chan error, readers)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(readers)
	done.Add(readers)
	for range readers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, err := store.GetIndexCatalogAtVersion(
				context.Background(), "tenant-a", 1,
			)
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	<-objects.started
	time.Sleep(50 * time.Millisecond)
	close(objects.release)
	done.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("load catalog: %v", err)
		}
	}
	if calls := objects.calls.Load(); calls != 1 {
		t.Fatalf("catalog object reads = %d, want one shared decode", calls)
	}
}

func TestIndexCatalogWaiterRetriesCanceledLeader(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &blockingIndexCatalogStore{
		ObjectStore: base,
		release:     make(chan struct{}),
		started:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(objects.release) }) }
	t.Cleanup(release)
	store := NewTenantStore(objects, "test")
	putIndexCatalogCacheFixture(t, ctx, store, base, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := store.GetIndexCatalogAtVersion(
			leaderCtx, "tenant-a", 1,
		)
		leaderDone <- err
	}()
	<-objects.started

	waiterDone := make(chan error, 1)
	go func() {
		catalog, err := store.GetIndexCatalogAtVersion(
			context.Background(), "tenant-a", 1,
		)
		if err == nil && catalog.Version != 1 {
			err = errors.New("waiter returned wrong catalog version")
		}
		waiterDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context canceled", err)
	}

	waitForAtomicCalls(t, &objects.calls, 2)
	release()
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiter: %v", err)
	}
}

func waitForAtomicCalls(
	t *testing.T,
	calls *atomic.Int64,
	want int64,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("object reads = %d, want at least %d", calls.Load(), want)
}

func putIndexCatalogCacheFixture(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	objects ObjectStore,
	catalog IndexCatalog,
) {
	t.Helper()
	data, err := marshalParquetIndexCatalog(ctx, catalog)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := objects.Put(ctx, store.indexCatalogKey(catalog.TenantID), data); err != nil {
		t.Fatalf("put catalog: %v", err)
	}
}
