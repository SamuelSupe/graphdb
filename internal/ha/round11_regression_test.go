package ha

import (
	"context"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestTenantReceiptColdScanPreservesConcurrentDeltas(t *testing.T) {
	dir := t.TempDir()
	files := storage.NewFileStore(dir)
	store := storage.NewTenantStore(files, "")
	app := &Application{
		Store:   store,
		Files:   files,
		ShardID: "s1",
	}

	// Simulate an active cold scan with concurrent track/untrack mutations.
	app.tenantReceiptsMu.Lock()
	app.tenantReceiptScanning = true
	wait := make(chan struct{})
	app.tenantReceiptScanWait = wait
	app.tenantReceiptsMu.Unlock()

	app.trackTenantReceipt("tenant-a", "k1")
	app.trackTenantReceipt("tenant-a", "k2")
	app.untrackTenantReceipt("k1")
	app.trackTenantReceipt("tenant-b", "k3")
	app.untrackTenantReceiptsForTenant("tenant-b")

	// Finish the simulated scan by replaying deltas onto an empty baseline.
	byTenant := map[string]map[string]struct{}{}
	owners := map[string]string{}
	app.tenantReceiptsMu.Lock()
	deltas := app.tenantReceiptDeltas
	app.tenantReceiptDeltas = nil
	app.tenantReceiptScanning = false
	app.tenantReceiptScanWait = nil
	for _, d := range deltas {
		switch d.kind {
		case tenantReceiptDeltaTrack:
			applyTenantReceiptTrack(byTenant, owners, d.tenant, d.key)
		case tenantReceiptDeltaUntrackKey:
			applyTenantReceiptUntrackKey(byTenant, owners, d.key)
		case tenantReceiptDeltaUntrackTenant:
			applyTenantReceiptUntrackTenant(byTenant, owners, d.tenant)
		}
	}
	app.tenantReceipts = byTenant
	app.tenantReceiptOwners = owners
	close(wait)
	app.tenantReceiptsMu.Unlock()

	if err := app.ensureTenantReceipts(context.Background()); err != nil {
		t.Fatalf("ensureTenantReceipts: %v", err)
	}
	if _, ok := app.tenantReceipts["tenant-a"]["k2"]; !ok {
		t.Fatalf("expected k2 tracked for tenant-a, got %+v", app.tenantReceipts)
	}
	if _, ok := app.tenantReceipts["tenant-a"]["k1"]; ok {
		t.Fatalf("expected untracked k1 removed, got %+v", app.tenantReceipts)
	}
	if _, ok := app.tenantReceipts["tenant-b"]; ok {
		t.Fatalf("expected tenant-b removed, got %+v", app.tenantReceipts)
	}
}

func TestPauseTenantWritesReferenceCounted(t *testing.T) {
	c := &Cluster{}
	c.admissionMu.Lock()
	entry := c.tenantAdmission("t1")
	entry.pauseCount = 2
	c.admissionMu.Unlock()

	if !c.maintenanceWritesPaused() {
		t.Fatal("expected writes paused when pauseCount == 2")
	}
	if _, err := c.admitTenantMutation(command{Kind: "http", Tenant: "t1", Method: "POST", URI: "/v1/commits"}); err == nil {
		t.Fatal("expected mutation deferred while paused")
	}

	c.admissionMu.Lock()
	entry.pauseCount--
	c.admissionMu.Unlock()
	if !c.maintenanceWritesPaused() {
		t.Fatal("expected writes still paused after first release when second pause is active")
	}

	c.admissionMu.Lock()
	entry.pauseCount--
	if entry.active == 0 && entry.pauseCount == 0 {
		delete(c.admissions, "t1")
	}
	c.admissionMu.Unlock()
	if c.maintenanceWritesPaused() {
		t.Fatal("expected writes unpaused after final release")
	}
	release, err := c.admitTenantMutation(command{Kind: "http", Tenant: "t1", Method: "POST", URI: "/v1/commits"})
	if err != nil {
		t.Fatalf("expected mutation admitted after unpause, got %v", err)
	}
	release()
}
