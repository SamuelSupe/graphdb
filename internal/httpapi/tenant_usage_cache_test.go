package httpapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestTenantUsageCacheCoalescesConcurrentFullScans(t *testing.T) {
	objects := &blockingTenantUsageStore{
		ObjectStore: storage.NewMemoryStore(),
		entered:     make(chan struct{}, 16),
		release:     make(chan struct{}),
	}
	server := &Server{
		Store:      storage.NewTenantStore(objects, "test"),
		usageCache: newTenantUsageCache(time.Minute),
	}
	const callers = 16
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	errs := make(chan error, callers)
	ready.Add(callers)
	done.Add(callers)
	for range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, err := server.cachedTenantUsage(
				context.Background(), "tenant-a", time.Now().UTC(),
			)
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-objects.entered:
	case <-time.After(time.Second):
		t.Fatal("tenant usage scan did not start")
	}
	select {
	case <-objects.entered:
	case <-time.After(100 * time.Millisecond):
	}
	close(objects.release)
	done.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("tenant usage: %v", err)
		}
	}
	objects.mu.Lock()
	calls := objects.calls
	objects.mu.Unlock()
	if calls != 1 {
		t.Fatalf("tenant usage full scans = %d, want one shared scan", calls)
	}
}

func TestTenantUsageCacheWaiterRetriesCanceledLeader(t *testing.T) {
	objects := &cancelFirstTenantUsageStore{
		ObjectStore: storage.NewMemoryStore(),
		started:     make(chan struct{}),
	}
	server := &Server{
		Store:      storage.NewTenantStore(objects, "test"),
		usageCache: newTenantUsageCache(time.Minute),
	}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := server.cachedTenantUsage(
			leaderCtx, "tenant-a", time.Now().UTC(),
		)
		leaderDone <- err
	}()
	select {
	case <-objects.started:
	case <-time.After(time.Second):
		t.Fatal("leader usage scan did not start")
	}

	waiterCtx := &usageDoneObservedContext{
		Context:  context.Background(),
		observed: make(chan struct{}),
	}
	waiterDone := make(chan error, 1)
	go func() {
		_, err := server.cachedTenantUsage(
			waiterCtx, "tenant-a", time.Now().UTC(),
		)
		waiterDone <- err
	}()
	select {
	case <-waiterCtx.observed:
	case <-time.After(time.Second):
		t.Fatal("healthy waiter did not join active usage scan")
	}
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("healthy waiter inherited canceled scan: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy waiter did not retry canceled usage scan")
	}
	objects.mu.Lock()
	calls := objects.calls
	objects.mu.Unlock()
	if calls != 2 {
		t.Fatalf("tenant usage scans = %d, want canceled scan plus retry", calls)
	}
}

func TestTenantUsageCacheBoundsTenantEntries(t *testing.T) {
	cache := newTenantUsageCache(time.Minute)
	now := time.Now().UTC()
	cache.mu.Lock()
	for i := 0; i <= maxTenantUsageCacheEntries; i++ {
		cache.putLocked(
			fmt.Sprintf("tenant-%05d", i),
			storage.TenantUsageReport{TenantID: fmt.Sprintf("tenant-%05d", i)},
			now.Add(time.Duration(i)*time.Millisecond),
		)
	}
	entryCount := len(cache.entries)
	_, newestCached := cache.entries[fmt.Sprintf("tenant-%05d", maxTenantUsageCacheEntries)]
	cache.mu.Unlock()
	if entryCount != maxTenantUsageCacheEntries {
		t.Fatalf(
			"cache entries = %d, want %d",
			entryCount,
			maxTenantUsageCacheEntries,
		)
	}
	if !newestCached {
		t.Fatal("newest tenant usage report was not cached")
	}
}

type blockingTenantUsageStore struct {
	storage.ObjectStore
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (s *blockingTenantUsageStore) ListPage(
	ctx context.Context,
	_ string,
	_ string,
	_ int,
) ([]storage.ObjectInfo, string, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return nil, "", nil
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}
}

type cancelFirstTenantUsageStore struct {
	storage.ObjectStore
	mu      sync.Mutex
	calls   int
	started chan struct{}
	once    sync.Once
}

func (s *cancelFirstTenantUsageStore) ListPage(
	ctx context.Context,
	_ string,
	_ string,
	_ int,
) ([]storage.ObjectInfo, string, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call != 1 {
		return nil, "", nil
	}
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return nil, "", ctx.Err()
}

type usageDoneObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *usageDoneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}
