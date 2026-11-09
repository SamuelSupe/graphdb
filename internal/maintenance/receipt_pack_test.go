package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestReceiptPackDiscoveryRetriesWithBackoffAndSeparateBudget(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	probe := &receiptDiscoveryProbe{ObjectStore: files}
	store := storage.NewTenantStore(probe, "graphdb")
	defer store.ShutdownTasks(ctx)
	var audit error
	audits := 0
	runner := &Runner{Store: store, Audit: func(event, _ string, err error, _ map[string]any) {
		if event == "receipt_pack_list_failed" {
			audit = err
			audits++
		}
	}}
	var discovery receiptPackDiscovery
	offset := 0
	packs := map[string]receiptPackProgress{}
	runner.packLocalReceipts(ctx, &discovery, packs, &offset)
	runner.packLocalReceipts(ctx, &discovery, packs, &offset)
	if discovery.done || probe.calls != 1 || audits != 1 || !errors.Is(audit, context.DeadlineExceeded) {
		t.Fatalf("expected one discovery attempt and one timeout audit, got state=%+v calls=%d audits=%d err=%v", discovery, probe.calls, audits, audit)
	}
	discovery.retryAt = time.Now().Add(-time.Second)
	runner.packLocalReceipts(ctx, &discovery, packs, &offset)
	if !discovery.done || probe.calls != 2 || audits != 1 {
		t.Fatalf("discovery could not complete after backoff: state=%+v calls=%d audits=%d", discovery, probe.calls, audits)
	}
}

type receiptDiscoveryProbe struct {
	storage.ObjectStore
	calls int
}

func (p *receiptDiscoveryProbe) UnwrapObjectStore() storage.ObjectStore { return p.ObjectStore }

func (p *receiptDiscoveryProbe) GetWithMeta(ctx context.Context, key string) ([]byte, storage.ObjectMeta, error) {
	p.calls++
	if p.calls == 1 {
		return nil, storage.ObjectMeta{}, context.DeadlineExceeded
	}
	// A registry read may take longer than one receipt packing page.
	timer := time.NewTimer(800 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, storage.ObjectMeta{}, ctx.Err()
	case <-timer.C:
		return p.ObjectStore.GetWithMeta(ctx, key)
	}
}

type receiptPackCounter struct{ passes int }

func (*receiptPackCounter) RecordManifestCASConflict(string)      {}
func (*receiptPackCounter) RecordCommitTail(string, int)          {}
func (*receiptPackCounter) RecordMaintenanceMemory(string, int64) {}
func (c *receiptPackCounter) RecordMaintenancePhase(phase string, _ time.Duration) {
	if phase == "pack_local_receipts" {
		c.passes++
	}
}

func TestReceiptPackingStopsScanningAndReschedulesLooseWrites(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := storage.NewTenantStore(files, "graphdb")
	defer store.ShutdownTasks(ctx)
	counter := &receiptPackCounter{}
	store.SetObservers(counter, nil)
	const tenant = "hot"
	write := func(key string) {
		t.Helper()
		if _, err := store.CommitWithReport(ctx, tenant, graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"value": key}}},
		}, storage.CommitOptions{IdempotencyKey: key}); err != nil {
			t.Fatal(err)
		}
	}
	write("first")
	runner := &Runner{Store: store}
	var discovery receiptPackDiscovery
	offset := 0
	packs := map[string]receiptPackProgress{}
	tick := func() { runner.packLocalReceipts(ctx, &discovery, packs, &offset) }

	// The first page finishes the direct prefix. A new write to that
	// prefix before the ingest page finishes requires another sweep.
	tick()
	if !discovery.done || packs[tenant].cursor == "" {
		t.Fatalf("initial receipt sweep did not start: %+v", packs)
	}
	write("second")
	tick()
	if len(store.PendingLocalReceiptPacks()) != 1 {
		t.Fatal("write behind the cursor was lost")
	}
	for i := 0; i < 6 && len(store.PendingLocalReceiptPacks()) != 0; i++ {
		tick()
	}
	if len(store.PendingLocalReceiptPacks()) != 0 || len(packs) != 0 {
		t.Fatalf("finished tenant remained queued: %+v", packs)
	}
	before := counter.passes
	tick()
	tick()
	if counter.passes != before || len(store.PendingLocalReceiptPacks()) != 0 {
		t.Fatal("idle ticks scanned a completed tenant again")
	}
}
