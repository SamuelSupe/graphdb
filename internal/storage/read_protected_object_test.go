package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestReadProtectedObjectStoreSingleflightSameKey(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	if err := base.Put(ctx, "objects/a", []byte("value-a")); err != nil {
		t.Fatalf("put: %v", err)
	}
	inner := &blockingReadStore{ObjectStore: base, block: make(chan struct{})}
	store := NewReadProtectedObjectStore(inner, ReadProtectionConfig{MaxConcurrent: 4, Singleflight: true})

	const readers = 12
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			data, meta, err := store.GetWithMeta(ctx, "objects/a")
			if err != nil {
				errs <- err
				return
			}
			if string(data) != "value-a" || meta.Key != "objects/a" || !meta.Exists {
				errs <- fmt.Errorf("unexpected read data=%q meta=%#v", string(data), meta)
				return
			}
			errs <- nil
		}()
	}
	waitForStartedReads(t, inner, 1)
	time.Sleep(20 * time.Millisecond)
	close(inner.block)
	for i := 0; i < readers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
	}
	if inner.startedReads() != 1 {
		t.Fatalf("inner reads = %d, want 1", inner.startedReads())
	}
}

func TestReadProtectedObjectStoreWaiterRetriesCanceledLeader(t *testing.T) {
	base := NewMemoryStore()
	if err := base.Put(
		context.Background(), "objects/a", []byte("value-a"),
	); err != nil {
		t.Fatalf("put: %v", err)
	}
	inner := &blockingReadStore{
		ObjectStore: base,
		block:       make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(inner.block) }) }
	t.Cleanup(release)
	store := NewReadProtectedObjectStore(
		inner,
		ReadProtectionConfig{MaxConcurrent: 4, Singleflight: true},
	)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := store.GetWithMeta(leaderCtx, "objects/a")
		leaderDone <- err
	}()
	waitForStartedReads(t, inner, 1)

	type readResult struct {
		data []byte
		err  error
	}
	waiterDone := make(chan readResult, 1)
	go func() {
		data, _, err := store.GetWithMeta(
			context.Background(), "objects/a",
		)
		waiterDone <- readResult{data: data, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context canceled", err)
	}

	waitForStartedReads(t, inner, 2)
	release()
	result := <-waiterDone
	if result.err != nil || string(result.data) != "value-a" {
		t.Fatalf("waiter data=%q err=%v", result.data, result.err)
	}
	if reads := inner.startedReads(); reads != 2 {
		t.Fatalf("inner reads = %d, want canceled leader plus retry", reads)
	}
}

func TestReadProtectedObjectStoreLimitsConcurrentReads(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	for i := 0; i < 8; i++ {
		if err := base.Put(ctx, fmt.Sprintf("objects/%d", i), []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	inner := &blockingReadStore{ObjectStore: base, delay: 10 * time.Millisecond}
	store := NewReadProtectedObjectStore(inner, ReadProtectionConfig{MaxConcurrent: 2, Singleflight: true})

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := store.GetWithMeta(ctx, fmt.Sprintf("objects/%d", i))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if inner.maxConcurrentReads() > 2 {
		t.Fatalf("max concurrent reads = %d, want <= 2", inner.maxConcurrentReads())
	}
}

type blockingReadStore struct {
	ObjectStore
	block chan struct{}
	delay time.Duration

	mu          sync.Mutex
	started     int
	inflight    int
	maxInflight int
}

func (s *blockingReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	s.started++
	s.inflight++
	if s.inflight > s.maxInflight {
		s.maxInflight = s.inflight
	}
	s.mu.Unlock()

	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			s.finish()
			return nil, ObjectMeta{Key: key}, ctx.Err()
		}
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	data, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	s.finish()
	return data, meta, err
}

func (s *blockingReadStore) finish() {
	s.mu.Lock()
	s.inflight--
	s.mu.Unlock()
}

func (s *blockingReadStore) startedReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *blockingReadStore) maxConcurrentReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInflight
}

func waitForStartedReads(t *testing.T, store *blockingReadStore, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.startedReads() >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("started reads = %d, want at least %d", store.startedReads(), count)
}
