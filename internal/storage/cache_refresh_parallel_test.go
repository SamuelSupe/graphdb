package storage

import (
	"context"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type blockingRefreshCoordinator struct {
	WriteCoordinator
	entered chan string
	release chan struct{}
}

func (*blockingRefreshCoordinator) Backend() string {
	return CoordinationPostgres
}

func (*blockingRefreshCoordinator) Namespace() string {
	return "refresh-parallel-test"
}

func (c *blockingRefreshCoordinator) Head(
	ctx context.Context,
	tenantID string,
) (CoordinationHead, bool, error) {
	select {
	case c.entered <- tenantID:
	case <-ctx.Done():
		return CoordinationHead{}, false, ctx.Err()
	}
	select {
	case <-c.release:
		return CoordinationHead{}, false, ErrCoordinatorUnavailable
	case <-ctx.Done():
		return CoordinationHead{}, false, ctx.Err()
	}
}

func TestReaderCacheRefreshesTenantsInParallel(t *testing.T) {
	ctx := context.Background()
	coordinator := &blockingRefreshCoordinator{
		entered: make(chan string, 2),
		release: make(chan struct{}),
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	cache := NewReaderCache(store, time.Minute)
	now := time.Now()
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		g := graph.New()
		g.Version = 1
		cache.entries[tenantID] = cacheEntry{
			graph:      g,
			manifest:   Manifest{TenantID: tenantID, Version: 1},
			expiresAt:  now.Add(-time.Second),
			lastAccess: now,
		}
	}

	done := make(chan struct{})
	go func() {
		cache.RefreshCached(ctx)
		close(done)
	}()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case tenantID := <-coordinator.entered:
			seen[tenantID] = true
		case <-time.After(250 * time.Millisecond):
			close(coordinator.release)
			<-done
			t.Fatalf("only %d tenant refresh reached the coordinator", len(seen))
		}
	}
	close(coordinator.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("parallel cache refresh did not finish")
	}
}
