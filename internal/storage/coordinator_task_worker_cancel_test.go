package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

type cancelAwareLeaseCoordinator struct {
	WriteCoordinator

	mu                sync.Mutex
	lease             CoordinatorTaskLease
	renewCalls        int
	firstRenewEntered chan struct{}
	releaseFirstRenew chan struct{}
	secondRenew       chan struct{}
}

func (*cancelAwareLeaseCoordinator) Backend() string {
	return CoordinationPostgres
}

func (*cancelAwareLeaseCoordinator) Namespace() string {
	return "task-cancel-test"
}

func (c *cancelAwareLeaseCoordinator) AcquireTaskLease(
	_ context.Context,
	tenantID string,
	taskType string,
	owner string,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	c.lease = CoordinatorTaskLease{
		TenantID:   tenantID,
		TaskType:   taskType,
		OwnerToken: owner,
		FenceEpoch: 1,
		ExpiresAt:  time.Now().UTC().Add(ttl),
	}
	return c.lease, true, nil
}

func (c *cancelAwareLeaseCoordinator) RenewTaskLease(
	_ context.Context,
	lease CoordinatorTaskLease,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	c.mu.Lock()
	c.renewCalls++
	call := c.renewCalls
	c.mu.Unlock()
	if call == 1 {
		close(c.firstRenewEntered)
		<-c.releaseFirstRenew
	} else if call == 2 {
		close(c.secondRenew)
	}
	lease.ExpiresAt = time.Now().UTC().Add(ttl)
	return lease, true, nil
}

func (*cancelAwareLeaseCoordinator) ReleaseTaskLease(
	context.Context,
	CoordinatorTaskLease,
) error {
	return nil
}

func TestCoordinatorTaskLeaseHeartbeatStopsWhenTaskIsCanceled(t *testing.T) {
	coordinator := &cancelAwareLeaseCoordinator{
		firstRenewEntered: make(chan struct{}),
		releaseFirstRenew: make(chan struct{}),
		secondRenew:       make(chan struct{}),
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = 30 * time.Millisecond
	store.SetCoordinator(coordinator)
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := store.startCoordinatorLease(
		ctx,
		"tenant-a",
		TaskTypeCompact,
		"writer-a/task-a",
		func() {},
	)
	if err != nil {
		t.Fatalf("start coordinator lease: %v", err)
	}
	defer stop()

	select {
	case <-coordinator.firstRenewEntered:
	case <-time.After(time.Second):
		t.Fatal("coordinator lease heartbeat did not start")
	}
	cancel()
	close(coordinator.releaseFirstRenew)

	select {
	case <-coordinator.secondRenew:
		t.Fatal("coordinator lease heartbeat renewed after task cancellation")
	case <-time.After(100 * time.Millisecond):
	}
}
