package storage

import (
	"errors"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorLifecycleLeaseBlocksMaintenanceWriters(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "maintenance-lifecycle-fence",
	)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:seed", Kind: "host",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	blocker, acquired, err := coordinator.AcquireTaskLease(
		ctx,
		"tenant-a",
		coordinatorLifecycleTaskType,
		"lifecycle-blocker",
		time.Minute,
	)
	if err != nil || !acquired {
		t.Fatalf("acquire lifecycle blocker acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() {
		_ = coordinator.ReleaseTaskLease(ctx, blocker)
	})

	operations := map[string]func() error{
		"compact": func() error {
			_, err := store.Compact(ctx, "tenant-a")
			return err
		},
		"gc": func() error {
			_, err := store.RunGC(ctx, "tenant-a", GCOptions{})
			return err
		},
		"index rebuild": func() error {
			_, err := store.RebuildIndexes(ctx, "tenant-a")
			return err
		},
		"repair apply": func() error {
			_, err := store.RepairTenant(
				ctx, "tenant-a", RepairOptions{Apply: true},
			)
			return err
		},
	}
	for name, run := range operations {
		if err := run(); !errors.Is(err, ErrTaskLeaseHeld) {
			t.Fatalf("%s err=%v, want ErrTaskLeaseHeld", name, err)
		}
	}
}
