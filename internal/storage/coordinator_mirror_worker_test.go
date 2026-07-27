package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSyncDerivedTasksDefersDisabledTenant(t *testing.T) {
	ctx := context.Background()
	coordinator := &disabledDerivedTaskCoordinator{
		head: CoordinationHead{
			TenantID:     "tenant-a",
			Generation:   2,
			Status:       TenantStatusDisabled,
			GraphVersion: 7,
		},
		job: DerivedTaskJob{
			TenantID:      "tenant-a",
			TaskType:      derivedTaskIndexes,
			TargetVersion: 7,
			OwnerToken:    "worker-a",
		},
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	processed, err := store.SyncDerivedTasks(ctx)
	if err != nil {
		t.Fatalf("SyncDerivedTasks: %v", err)
	}
	claims, failures, completions := coordinator.results()
	if processed != 0 || claims != 1 || failures != 1 || completions != 0 {
		t.Fatalf(
			"processed=%d claims=%d failures=%d completions=%d, want 0, 1, 1, 0",
			processed,
			claims,
			failures,
			completions,
		)
	}
}

func TestSyncDerivedTasksBoundsDeferredClaims(t *testing.T) {
	ctx := context.Background()
	coordinator := &disabledDerivedTaskCoordinator{
		head: CoordinationHead{
			TenantID:     "tenant-a",
			Generation:   2,
			Status:       TenantStatusDisabled,
			GraphVersion: 7,
		},
		job: DerivedTaskJob{
			TenantID:      "tenant-a",
			TaskType:      derivedTaskIndexes,
			TargetVersion: 7,
			OwnerToken:    "worker-a",
		},
		jobCount: 3,
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	processed, err := store.syncDerivedTasks(ctx, 2)
	if err != nil {
		t.Fatalf("syncDerivedTasks: %v", err)
	}
	claims, failures, completions := coordinator.results()
	if processed != 0 || claims != 2 || failures != 2 || completions != 0 {
		t.Fatalf(
			"processed=%d claims=%d failures=%d completions=%d, want 0, 2, 2, 0",
			processed,
			claims,
			failures,
			completions,
		)
	}
}

type disabledDerivedTaskCoordinator struct {
	WriteCoordinator
	mu          sync.Mutex
	head        CoordinationHead
	job         DerivedTaskJob
	jobCount    int
	claims      int
	failures    int
	completions int
}

func (c *disabledDerivedTaskCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *disabledDerivedTaskCoordinator) Namespace() string {
	return "test"
}

func (c *disabledDerivedTaskCoordinator) Head(
	context.Context,
	string,
) (CoordinationHead, bool, error) {
	return c.head, true, nil
}

func (c *disabledDerivedTaskCoordinator) ClaimDerivedTask(
	context.Context,
	string,
	time.Duration,
) (DerivedTaskJob, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	jobCount := c.jobCount
	if jobCount == 0 {
		jobCount = 1
	}
	if c.claims >= jobCount {
		return DerivedTaskJob{}, false, nil
	}
	c.claims++
	return c.job, true, nil
}

func (c *disabledDerivedTaskCoordinator) CompleteDerivedTask(
	context.Context,
	DerivedTaskJob,
	int64,
) error {
	c.mu.Lock()
	c.completions++
	c.mu.Unlock()
	return nil
}

func (c *disabledDerivedTaskCoordinator) FailDerivedTask(
	context.Context,
	DerivedTaskJob,
	error,
) error {
	c.mu.Lock()
	c.failures++
	c.mu.Unlock()
	return nil
}

func (c *disabledDerivedTaskCoordinator) results() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claims, c.failures, c.completions
}
