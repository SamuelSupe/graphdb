package storage

import (
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
