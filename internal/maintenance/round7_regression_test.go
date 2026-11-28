package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound7MaintenancePrunesLastGCAndStaleReceiptPacks(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "r7-maint")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "live-tenant", storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}

	runner := &Runner{Store: store}
	runner.maintenance.mu.Lock()
	runner.maintenance.lastGC = map[string]time.Time{
		"live-tenant":   time.Now().UTC(),
		"purged-tenant": time.Now().UTC(),
	}
	runner.maintenance.gcCursor = map[string]string{
		"purged-tenant": "cursor-1",
	}
	runner.maintenance.mu.Unlock()

	runner.Run(ctx, time.Now().UTC())

	runner.maintenance.mu.Lock()
	_, staleLastGC := runner.maintenance.lastGC["purged-tenant"]
	_, staleCursor := runner.maintenance.gcCursor["purged-tenant"]
	runner.maintenance.mu.Unlock()
	if staleLastGC || staleCursor {
		t.Fatalf("expected purged-tenant to be pruned from lastGC and gcCursor, got staleLastGC=%v staleCursor=%v", staleLastGC, staleCursor)
	}

	// Stale entries in packs not present in PendingLocalReceiptPacks must be pruned.
	packs := map[string]receiptPackProgress{
		"purged-tenant": {cursor: "c1", retryAt: time.Now().Add(time.Minute)},
	}
	offset := 0
	var discovery receiptPackDiscovery
	runner.packLocalReceipts(ctx, &discovery, packs, &offset)
	if _, exists := packs["purged-tenant"]; exists {
		t.Fatalf("expected stale purged-tenant entry to be removed from packs")
	}
}
