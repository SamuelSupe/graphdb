package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

type legacyManifestLeaseTestCoordinator struct {
	WriteCoordinator
	renewed chan struct{}
}

func (*legacyManifestLeaseTestCoordinator) Backend() string {
	return CoordinationPostgres
}

func (*legacyManifestLeaseTestCoordinator) Namespace() string {
	return "test"
}

func (c *legacyManifestLeaseTestCoordinator) RenewLegacyManifest(
	context.Context,
	LegacyManifestJob,
	time.Duration,
) (bool, error) {
	select {
	case c.renewed <- struct{}{}:
	default:
	}
	return false, nil
}

func TestLegacyManifestLeaseLossCancelsMirrorWork(t *testing.T) {
	coordinator := &legacyManifestLeaseTestCoordinator{
		renewed: make(chan struct{}, 1),
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	jobCtx, stop := store.startLegacyManifestLease(
		context.Background(),
		LegacyManifestJob{TenantID: "tenant-a", HeadRevision: 7},
		30*time.Millisecond,
	)

	select {
	case <-jobCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lost legacy manifest lease did not cancel mirror work")
	}
	if err := stop(); !errors.Is(err, ErrConflict) {
		t.Fatalf("stop lease err = %v, want ErrConflict", err)
	}
	select {
	case <-coordinator.renewed:
	default:
		t.Fatal("legacy manifest lease was not renewed")
	}
}
