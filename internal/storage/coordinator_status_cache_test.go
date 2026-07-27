package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCoordinatorStatusCoalescesConcurrentProbes(t *testing.T) {
	coordinator := &blockingStatusCoordinator{
		entered: make(chan struct{}, 16),
		release: make(chan struct{}),
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	const callers = 16
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	statuses := make(chan CoordinatorStatus, callers)
	ready.Add(callers)
	done.Add(callers)
	for range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			statuses <- store.CoordinatorStatus(context.Background())
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-coordinator.entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator status probe did not start")
	}
	select {
	case <-coordinator.entered:
	case <-time.After(100 * time.Millisecond):
	}
	close(coordinator.release)
	done.Wait()
	close(statuses)
	for status := range statuses {
		if !status.Available {
			t.Fatalf("coordinator status = %#v", status)
		}
	}
	coordinator.mu.Lock()
	calls := coordinator.calls
	coordinator.mu.Unlock()
	if calls != 1 {
		t.Fatalf("coordinator status probes = %d, want one shared probe", calls)
	}
}

func TestCoordinatorStatusWaiterRetriesCanceledLeader(t *testing.T) {
	coordinator := &cancelFirstStatusCoordinator{
		started: make(chan struct{}),
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan CoordinatorStatus, 1)
	go func() {
		leaderDone <- store.CoordinatorStatus(leaderCtx)
	}()
	select {
	case <-coordinator.started:
	case <-time.After(time.Second):
		t.Fatal("leader coordinator probe did not start")
	}

	waiterCtx := &doneObservedContext{
		Context:  context.Background(),
		observed: make(chan struct{}),
	}
	waiterDone := make(chan CoordinatorStatus, 1)
	go func() {
		waiterDone <- store.CoordinatorStatus(waiterCtx)
	}()
	select {
	case <-waiterCtx.observed:
	case <-time.After(time.Second):
		t.Fatal("healthy waiter did not join active coordinator probe")
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
		t.Fatal("healthy waiter did not retry canceled coordinator probe")
	}
	coordinator.mu.Lock()
	calls := coordinator.calls
	coordinator.mu.Unlock()
	if calls != 2 {
		t.Fatalf("coordinator probes = %d, want canceled probe plus retry", calls)
	}
	if cached := store.CachedCoordinatorStatus(); !cached.Available {
		t.Fatalf("canceled probe overwrote healthy cache: %#v", cached)
	}
}

type blockingStatusCoordinator struct {
	WriteCoordinator
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (c *blockingStatusCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *blockingStatusCoordinator) Namespace() string {
	return "test"
}

func (c *blockingStatusCoordinator) Status(
	ctx context.Context,
) (CoordinatorStatus, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	c.entered <- struct{}{}
	select {
	case <-c.release:
		return CoordinatorStatus{
			Backend:   CoordinationPostgres,
			Available: true,
			Namespace: "test",
			CheckedAt: time.Now().UTC(),
		}, nil
	case <-ctx.Done():
		return CoordinatorStatus{}, ctx.Err()
	}
}

type cancelFirstStatusCoordinator struct {
	WriteCoordinator
	mu      sync.Mutex
	calls   int
	started chan struct{}
	once    sync.Once
}

func (c *cancelFirstStatusCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *cancelFirstStatusCoordinator) Namespace() string {
	return "test"
}

func (c *cancelFirstStatusCoordinator) Status(
	ctx context.Context,
) (CoordinatorStatus, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	if call != 1 {
		return CoordinatorStatus{
			Backend:   CoordinationPostgres,
			Available: true,
			Namespace: "test",
			CheckedAt: time.Now().UTC(),
		}, nil
	}
	c.once.Do(func() { close(c.started) })
	<-ctx.Done()
	return CoordinatorStatus{}, ctx.Err()
}
