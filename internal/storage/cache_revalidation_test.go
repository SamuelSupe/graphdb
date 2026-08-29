package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type delayedGraphReadStore struct {
	ObjectStore
	delay time.Duration
}

type blockingGraphReadStore struct {
	ObjectStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *delayedGraphReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, "/commits/") || strings.Contains(key, "/snapshots/") {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *blockingGraphReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, "/commits/") || strings.Contains(key, "/snapshots/") {
		s.once.Do(func() { close(s.started) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.release:
		}
	}
	return s.ObjectStore.Get(ctx, key)
}

type blockingHeadCoordinator struct {
	WriteCoordinator
	head    CoordinationHead
	release <-chan struct{}
	calls   atomic.Int64
}

func (c *blockingHeadCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *blockingHeadCoordinator) Namespace() string {
	return "test"
}

func (c *blockingHeadCoordinator) Head(
	context.Context,
	string,
) (CoordinationHead, bool, error) {
	c.calls.Add(1)
	<-c.release
	return c.head, true, nil
}

type unavailableHeadCoordinator struct {
	WriteCoordinator
	calls atomic.Int64
}

func (c *unavailableHeadCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *unavailableHeadCoordinator) Namespace() string {
	return "test"
}

func (c *unavailableHeadCoordinator) Head(
	context.Context,
	string,
) (CoordinationHead, bool, error) {
	c.calls.Add(1)
	return CoordinationHead{}, false, ErrCoordinatorUnavailable
}

func TestReaderCacheExpiredEntrySharesCoordinatorRevalidation(t *testing.T) {
	const readers = 16
	cache, store, head := newExpiredCoordinatedReaderCache(t)
	release := make(chan struct{})
	coordinator := &blockingHeadCoordinator{
		head:    head,
		release: release,
	}
	store.SetCoordinator(coordinator)

	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	errs := make(chan error, readers)
	ready.Add(readers)
	done.Add(readers)
	for range readers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, _, err := cache.Load(context.Background(), "tenant-a")
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	time.Sleep(50 * time.Millisecond)
	close(release)
	done.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("load: %v", err)
		}
	}
	if calls := coordinator.calls.Load(); calls != 1 {
		t.Fatalf("coordinator head calls = %d, want one shared revalidation", calls)
	}
}

func TestReaderCacheCoordinatorOutageSharesStaleFallback(t *testing.T) {
	const readers = 16
	cache, store, _ := newExpiredCoordinatedReaderCache(t)
	coordinator := &unavailableHeadCoordinator{}
	store.SetCoordinator(coordinator)

	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	errs := make(chan error, readers)
	ready.Add(readers)
	done.Add(readers)
	for range readers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, _, err := cache.Load(context.Background(), "tenant-a")
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("stale load: %v", err)
		}
	}
	if calls := coordinator.calls.Load(); calls != 1 {
		t.Fatalf("coordinator outage calls = %d, want one shared fallback", calls)
	}
}

func TestReaderCacheIdleRetentionIsIndependentFromRefreshTTL(t *testing.T) {
	cache := NewReaderCache(nil, time.Second)
	cache.IdleTTL = time.Hour
	cache.entries["tenant-a"] = cacheEntry{
		graph:      graph.New(),
		manifest:   Manifest{TenantID: "tenant-a", Version: 1},
		expiresAt:  time.Now().Add(-time.Minute),
		lastAccess: time.Now().Add(-time.Minute),
	}

	if tenants := cache.cachedTenantsForRefresh(); len(tenants) != 1 || tenants[0] != "tenant-a" {
		t.Fatalf("cached tenants = %#v, want tenant-a retained", tenants)
	}
	cache.IdleTTL = 30 * time.Second
	if tenants := cache.cachedTenantsForRefresh(); len(tenants) != 0 {
		t.Fatalf("cached tenants = %#v, want idle tenant evicted", tenants)
	}
}

func TestReaderCacheReusesLogicalGraphAfterCompaction(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	observer := &testCacheObserver{}
	cache := NewReaderCache(store, time.Minute)
	cache.Observer = observer
	var before *graph.Graph
	if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", 1, func(g *graph.Graph, _ Manifest) error {
		before = g
		return nil
	}); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	cache.mu.Lock()
	entry := cache.entries["tenant-a"]
	entry.expiresAt = time.Now().Add(-time.Second)
	cache.entries["tenant-a"] = entry
	cache.mu.Unlock()

	if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", 1, func(g *graph.Graph, manifest Manifest) error {
		if g != before {
			t.Fatal("compaction rebuilt a logically identical cached graph")
		}
		if manifest.SnapshotVersion != manifest.Version || manifest.SnapshotCatalogKey == "" {
			t.Fatalf("cache manifest was not revalidated after compact: %#v", manifest)
		}
		return nil
	}); err != nil {
		t.Fatalf("load after compact: %v", err)
	}
	if observer.cache["tenant-a\x00revalidated_logical_graph"] != 1 {
		t.Fatalf("cache events = %#v", observer.cache)
	}
}

func TestReaderCacheColdLoadContinuesAfterCallerTimeout(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	reader := NewTenantStore(&delayedGraphReadStore{ObjectStore: base, delay: 50 * time.Millisecond}, "test")
	cache := NewReaderCache(reader, time.Minute)
	cache.LoadTimeout = 5 * time.Second
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	if _, _, err := cache.LoadAtLeast(requestCtx, "tenant-a", 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed load err = %v, want deadline exceeded", err)
	}

	warmCtx, cancelWarm := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWarm()
	if _, _, err := cache.LoadAtLeast(warmCtx, "tenant-a", 1); err != nil {
		t.Fatalf("wait for shared cold load: %v", err)
	}
	if version, ok := cache.CachedVersion("tenant-a"); !ok || version != 1 {
		t.Fatalf("cached version = %d, %v; want 1, true", version, ok)
	}
}

func TestReaderCacheBoundsColdLoadsAcrossTenants(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := writer.Commit(ctx, tenantID, graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:" + tenantID, Kind: "host"}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %s: %v", tenantID, err)
		}
	}
	blocked := &blockingGraphReadStore{
		ObjectStore: base,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	cache := NewReaderCache(NewTenantStore(blocked, "test"), time.Minute)
	cache.ConfigureLoadAdmission(1, 10*time.Millisecond)
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := cache.Load(ctx, "tenant-a")
		firstDone <- err
	}()
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("first cold load did not start")
	}

	if _, _, err := cache.Load(ctx, "tenant-b"); !errors.Is(err, ErrReaderLoadBusy) {
		t.Fatalf("second cold load err = %v, want ErrReaderLoadBusy", err)
	}
	cache.mu.RLock()
	_, queued := cache.loading["tenant-b"]
	cache.mu.RUnlock()
	if queued {
		t.Fatal("rejected cold load remained queued in the background")
	}

	close(blocked.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first cold load: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-b"); err != nil {
		t.Fatalf("second cold load after release: %v", err)
	}
}

func TestReaderCacheRefreshLoadTimeoutReleasesAdmission(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := writer.Commit(ctx, tenantID, graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:" + tenantID, Kind: "host"}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %s: %v", tenantID, err)
		}
	}
	cache := NewReaderCache(NewTenantStore(base, "test"), time.Minute)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("prime tenant-a: %v", err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:updated", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance tenant-a: %v", err)
	}
	blocked := &blockingGraphReadStore{
		ObjectStore: base,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	cache.Store = NewTenantStore(blocked, "test")
	cache.LoadTimeout = 20 * time.Millisecond
	cache.ConfigureLoadAdmission(1, 10*time.Millisecond)

	started := time.Now()
	refreshCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, _, err := cache.Refresh(refreshCtx, "tenant-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("refresh elapsed = %s, want load timeout before parent deadline", elapsed)
	}

	cache.Store = NewTenantStore(base, "test")
	if _, _, err := cache.Load(ctx, "tenant-b"); err != nil {
		t.Fatalf("tenant-b load after timed out refresh: %v", err)
	}
}

func newExpiredCoordinatedReaderCache(
	t *testing.T,
) (*ReaderCache, *TenantStore, CoordinationHead) {
	t.Helper()
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       7,
		HeadCommitID:  "commit-7",
	}
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Generation:           3,
		Status:               TenantStatusActive,
		Revision:             11,
		GraphVersion:         manifest.Version,
		ManifestKey:          "test/tenants/tenant-a/coordination/manifests/v7.parquet",
		ManifestHash:         objectContentHash(data),
		CommitID:             manifest.HeadCommitID,
		WriteContextRevision: 2,
	}
	if err := base.Put(ctx, head.ManifestKey, data); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	g := graph.New()
	g.Version = manifest.Version
	cache := NewReaderCache(store, time.Second)
	cache.entries["tenant-a"] = cacheEntry{
		graph:      g,
		manifest:   manifest,
		meta:       coordinatedManifestMeta(head.ManifestKey, head),
		cachedAt:   time.Now().Add(-2 * time.Second),
		expiresAt:  time.Now().Add(-time.Second),
		lastAccess: time.Now(),
	}
	return cache, store, head
}
