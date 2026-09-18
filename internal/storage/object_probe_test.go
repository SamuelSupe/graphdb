package storage

import (
	"context"
	"errors"

	"os"
	"path/filepath"

	"sync"
	"testing"
	"time"
)

func TestObjectStoreStatusBypassesWriterCache(t *testing.T) {
	inner := &switchableProbeStore{ObjectStore: NewMemoryStore()}
	objects := NewWriterObjectCache(inner, WriterObjectCacheConfig{MaxBytes: 1024, MaxKeys: 10})
	store := NewTenantStore(objects, "test")
	if status := store.ObjectStoreStatus(context.Background()); !status.Available {
		t.Fatalf("initial status = %#v", status)
	}
	inner.setError(errors.New("backend unavailable"))
	status := store.ObjectStoreStatus(context.Background())
	if status.Available || status.LastError != "backend unavailable" {
		t.Fatalf("outage status = %#v", status)
	}
}

func TestObjectStoreProbeDoesNotChangeWriteBackpressure(t *testing.T) {
	inner := &switchableProbeStore{
		ObjectStore: NewMemoryStore(),
		err:         errors.New("backend unavailable"),
	}
	pressure := NewWritePressure(BackpressureConfig{
		ObjectErrorWindow:    time.Minute,
		ObjectErrorThreshold: 1,
	})
	store := NewTenantStore(NewMeteredObjectStore(inner, pressure, nil), "test")
	if status := store.ObjectStoreStatus(context.Background()); status.Available {
		t.Fatalf("probe status = %#v", status)
	}
	if reasons := pressure.Reasons("tenant-a"); len(reasons) != 0 {
		t.Fatalf("readiness probe changed write pressure: %#v", reasons)
	}
}

func TestFileStoreProbeRejectsNonDirectoryRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	if err := os.WriteFile(root, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write root file: %v", err)
	}
	store := NewTenantStore(NewFileStore(root), "test")
	if status := store.ObjectStoreStatus(context.Background()); status.Available {
		t.Fatalf("file-root status = %#v", status)
	}
}

func TestObjectStoreStatusCoalescesConcurrentProbes(t *testing.T) {
	probe := &blockingProbeStore{
		ObjectStore: NewMemoryStore(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	store := NewTenantStore(probe, "test")
	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			_ = store.ObjectStoreStatus(ctx)
		}()
	}
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("object store probe did not start")
	}
	time.Sleep(60 * time.Millisecond)
	probe.mu.Lock()
	calls := probe.calls
	probe.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend probe calls = %d, want 1", calls)
	}
	close(probe.release)
	wg.Wait()
}

func TestObjectStoreStatusWaiterRetriesCanceledLeader(t *testing.T) {
	probe := &cancelFirstProbeStore{
		ObjectStore: NewMemoryStore(),
		started:     make(chan struct{}),
	}
	store := NewTenantStore(probe, "test")
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan ObjectStoreStatus, 1)
	go func() {
		leaderDone <- store.ObjectStoreStatus(leaderCtx)
	}()
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("leader probe did not start")
	}

	waiterCtx := &doneObservedContext{
		Context:  context.Background(),
		observed: make(chan struct{}),
	}
	waiterDone := make(chan ObjectStoreStatus, 1)
	go func() {
		waiterDone <- store.ObjectStoreStatus(waiterCtx)
	}()
	select {
	case <-waiterCtx.observed:
	case <-time.After(time.Second):
		t.Fatal("healthy waiter did not join active probe")
	}
	cancelLeader()
	if status := <-leaderDone; status.Available ||
		status.LastError != context.Canceled.Error() {
		t.Fatalf("leader status = %#v", status)
	}
	select {
	case status := <-waiterDone:
		if !status.Available || status.LastError != "" {
			t.Fatalf("healthy waiter inherited canceled status: %#v", status)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy waiter did not retry canceled probe")
	}
	probe.mu.Lock()
	calls := probe.calls
	probe.mu.Unlock()
	if calls != 2 {
		t.Fatalf("backend probe calls = %d, want canceled load plus retry", calls)
	}
}

type switchableProbeStore struct {
	ObjectStore
	mu  sync.RWMutex
	err error
}

type blockingProbeStore struct {
	ObjectStore
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type cancelFirstProbeStore struct {
	ObjectStore
	mu      sync.Mutex
	calls   int
	started chan struct{}
	once    sync.Once
}

type doneObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func (s *blockingProbeStore) Probe(context.Context) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	s.once.Do(func() { close(s.started) })
	<-s.release
	return nil
}

func (s *cancelFirstProbeStore) Probe(ctx context.Context) error {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call != 1 {
		return nil
	}
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *switchableProbeStore) Probe(context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *switchableProbeStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}
