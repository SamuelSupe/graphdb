package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

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
