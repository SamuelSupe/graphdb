package storage

import (
	"context"
	"sync"
	"testing"
)

type countingWriteContextCoordinator struct {
	WriteCoordinator
	mu    sync.Mutex
	head  CoordinationHead
	calls int
}

func (c *countingWriteContextCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *countingWriteContextCoordinator) Namespace() string {
	return "write-context-memo-test"
}

func (c *countingWriteContextCoordinator) Head(
	context.Context,
	string,
) (CoordinationHead, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.head, true, nil
}

func (c *countingWriteContextCoordinator) headCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestCoordinatedWriteContextMemoLoadsOncePerCommitAttempt(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	snapshot := emptyWriteContext("tenant-a")
	key, hash, err := store.putCoordinatedWriteContextSnapshot(
		ctx,
		"tenant-a",
		3,
		snapshot,
	)
	if err != nil {
		t.Fatalf("put write context: %v", err)
	}
	coordinator := &countingWriteContextCoordinator{
		head: CoordinationHead{
			TenantID:             "tenant-a",
			Generation:           1,
			Status:               TenantStatusActive,
			Revision:             7,
			WriteContextRevision: 3,
			WriteContextKey:      key,
			WriteContextHash:     hash,
		},
	}
	store.SetCoordinator(coordinator)
	objects.reset()

	attemptCtx := withFreshCoordinatedWriteContextMemo(ctx)
	for range 3 {
		loaded, head, err := store.loadCoordinatedWriteContext(
			attemptCtx,
			"tenant-a",
		)
		if err != nil {
			t.Fatalf("load write context: %v", err)
		}
		if loaded.Revision != 3 || head.WriteContextRevision != 3 {
			t.Fatalf("loaded snapshot=%#v head=%#v", loaded, head)
		}
	}
	if got := coordinator.headCalls(); got != 1 {
		t.Fatalf("coordinator head calls = %d, want 1", got)
	}
	if got := objects.countContains("/coordination/write-contexts/"); got != 1 {
		t.Fatalf("write-context object reads = %d, want 1", got)
	}

	if _, _, err := store.loadCoordinatedWriteContext(
		withFreshCoordinatedWriteContextMemo(ctx),
		"tenant-a",
	); err != nil {
		t.Fatalf("load next attempt write context: %v", err)
	}
	if got := coordinator.headCalls(); got != 2 {
		t.Fatalf("coordinator head calls after next attempt = %d, want 2", got)
	}
	if got := objects.countContains("/coordination/write-contexts/"); got != 2 {
		t.Fatalf("write-context reads after next attempt = %d, want 2", got)
	}
}
