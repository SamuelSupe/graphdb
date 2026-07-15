package storage

import (
	"context"
	"testing"
	"time"
)

func TestTenantStatusCachesPurgeTombstoneReads(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = time.Hour
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	store.deleteCachedTenantPurgeTombstone("tenant-a")
	objects.Reset()
	for i := 0; i < 10; i++ {
		status, err := store.TenantStatus(ctx, "tenant-a")
		if err != nil || status != TenantStatusActive {
			t.Fatalf("status = %q err=%v", status, err)
		}
	}
	if got := objects.CountContains("/control/tenant-purges/"); got != 1 {
		t.Fatalf("purge tombstone reads = %d, want 1", got)
	}
}

func TestTenantStatusRefreshesCrossInstanceLifecycleChanges(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	owner := NewTenantStore(base, "test")
	if _, err := owner.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	reader := NewTenantStore(base, "test")
	reader.LifecycleCacheTTL = 5 * time.Millisecond
	if status, err := reader.TenantStatus(ctx, "tenant-a"); err != nil || status != TenantStatusActive {
		t.Fatalf("initial status = %q err=%v", status, err)
	}

	if _, err := owner.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if status, err := reader.TenantStatus(ctx, "tenant-a"); err != nil || status != TenantStatusDisabled {
		t.Fatalf("refreshed disabled status = %q err=%v", status, err)
	}

	if _, err := owner.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete tenant: %v", err)
	}
	if _, err := owner.PurgeTenant(ctx, "tenant-a", false); err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if status, err := reader.TenantStatus(ctx, "tenant-a"); err != nil || status != TenantStatusDeleted {
		t.Fatalf("refreshed purged status = %q err=%v", status, err)
	}
}
