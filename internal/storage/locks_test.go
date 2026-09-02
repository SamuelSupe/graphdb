package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTenantLockDropsIdleSlots(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	unlock := store.lockTenant("tenant-a")
	if got := tenantLockSlotCount(store); got != 1 {
		t.Fatalf("tenant lock slots = %d, want 1", got)
	}
	unlock()
	if got := tenantLockSlotCount(store); got != 0 {
		t.Fatalf("tenant lock slots = %d, want 0", got)
	}
}

func TestTenantLockKeepsSlotForWaiter(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	firstUnlock := store.lockTenant("tenant-a")
	waiterUnlocks := make(chan func(), 1)
	go func() {
		waiterUnlocks <- store.lockTenant("tenant-a")
	}()
	waitForTenantLockRefs(t, store, "tenant-a", 2)
	firstUnlock()
	waiterUnlock := <-waiterUnlocks
	if got := tenantLockSlotCount(store); got != 1 {
		t.Fatalf("tenant lock slots while waiter holds lock = %d, want 1", got)
	}
	waiterUnlock()
	if got := tenantLockSlotCount(store); got != 0 {
		t.Fatalf("tenant lock slots after waiter release = %d, want 0", got)
	}
}

func TestTenantLockWaitCanBeCanceled(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	firstUnlock := store.lockTenant("tenant-a")
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		unlock func()
		err    error
	}
	done := make(chan result, 1)
	go func() {
		unlock, err := store.lockTenantForeground(ctx, "tenant-a")
		done <- result{unlock: unlock, err: err}
	}()
	waitForTenantLockRefs(t, store, "tenant-a", 2)
	cancel()
	got := <-done
	if got.unlock != nil || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled lock result unlock=%v err=%v", got.unlock != nil, got.err)
	}
	waitForTenantLockRefs(t, store, "tenant-a", 1)
	firstUnlock()
	if got := tenantLockSlotCount(store); got != 0 {
		t.Fatalf("tenant lock slots after cancellation = %d, want 0", got)
	}
}

func TestTenantOperationsHonorContextWhileWaitingForLock(t *testing.T) {
	operations := map[string]func(context.Context, *TenantStore) error{
		"foreground": func(ctx context.Context, store *TenantStore) error {
			_, err := store.InitTenant(ctx, "tenant-a")
			return err
		},
		"maintenance": func(ctx context.Context, store *TenantStore) error {
			_, err := store.RunGC(ctx, "tenant-a", GCOptions{})
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			store := NewTenantStore(NewMemoryStore(), "test")
			firstUnlock := store.lockTenant("tenant-a")
			defer firstUnlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation(ctx, store) }()
			waitForTenantLockRefs(t, store, "tenant-a", 2)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("operation err = %v, want context canceled", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("operation ignored context while waiting for tenant lock")
			}
		})
	}
}

func TestForegroundTenantLockPassesQueuedMaintenance(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	firstUnlock := store.lockTenant("tenant-a")
	maintenance := make(chan func(), 1)
	go func() {
		unlock, err := store.lockTenantMaintenance(context.Background(), "tenant-a")
		if err != nil {
			return
		}
		maintenance <- unlock
	}()
	waitForTenantLockRefs(t, store, "tenant-a", 2)
	foreground := make(chan func(), 1)
	go func() {
		unlock, err := store.lockTenantForeground(context.Background(), "tenant-a")
		if err != nil {
			return
		}
		foreground <- unlock
	}()
	waitForTenantLockRefs(t, store, "tenant-a", 3)
	firstUnlock()

	var foregroundUnlock func()
	select {
	case foregroundUnlock = <-foreground:
	case <-maintenance:
		t.Fatal("maintenance lock was granted before queued foreground write")
	case <-time.After(time.Second):
		t.Fatal("foreground lock was not granted")
	}
	foregroundUnlock()
	select {
	case maintenanceUnlock := <-maintenance:
		maintenanceUnlock()
	case <-time.After(time.Second):
		t.Fatal("maintenance lock was not granted after foreground release")
	}
	if got := tenantLockSlotCount(store); got != 0 {
		t.Fatalf("tenant lock slots after releases = %d, want 0", got)
	}
}

func tenantLockSlotCount(store *TenantStore) int {
	store.lockMu.Lock()
	defer store.lockMu.Unlock()
	return len(store.tenantLocks)
}

func waitForTenantLockRefs(t *testing.T, store *TenantStore, tenantID string, refs int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.lockMu.Lock()
		lock := store.tenantLocks[tenantID]
		got := 0
		if lock != nil {
			got = lock.refs
		}
		store.lockMu.Unlock()
		if got == refs {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("tenant lock refs for %q did not reach %d", tenantID, refs)
}
