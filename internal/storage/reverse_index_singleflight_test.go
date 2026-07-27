package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingReverseCatalogStore struct {
	ObjectStore
	release chan struct{}
	started chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (s *blockingReverseCatalogStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if strings.Contains(key, "/reverse-index/catalog.json") {
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

func TestReverseIndexCatalogConcurrentMissSharesObjectRead(t *testing.T) {
	const readers = 16
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &blockingReverseCatalogStore{
		ObjectStore: base,
		release:     make(chan struct{}),
		started:     make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	data, err := json.Marshal(ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      "tenant-a",
		Version:       1,
		UpdatedAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := base.Put(
		ctx, store.reverseIndexCatalogKey("tenant-a"), data,
	); err != nil {
		t.Fatalf("put catalog: %v", err)
	}

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
			_, err := store.GetReverseIndexCatalog(
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
		t.Fatalf("catalog object reads = %d, want one shared read", calls)
	}
}

func TestReverseIndexCatalogWaiterRetriesCanceledLeader(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &blockingReverseCatalogStore{
		ObjectStore: base,
		release:     make(chan struct{}),
		started:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(objects.release) }) }
	t.Cleanup(release)
	store := NewTenantStore(objects, "test")
	data, err := json.Marshal(ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      "tenant-a",
		Version:       1,
		UpdatedAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := base.Put(
		ctx, store.reverseIndexCatalogKey("tenant-a"), data,
	); err != nil {
		t.Fatalf("put catalog: %v", err)
	}

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := store.GetReverseIndexCatalog(
			leaderCtx, "tenant-a", 1,
		)
		leaderDone <- err
	}()
	<-objects.started

	waiterDone := make(chan error, 1)
	go func() {
		catalog, err := store.GetReverseIndexCatalog(
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
