package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestReceiptPackDiscoveryReportsTimeout(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := storage.NewTenantStore(storage.NewDelayedReadObjectStore(files, 2*time.Second), "graphdb")
	defer store.ShutdownTasks(ctx)
	var audit error
	runner := &Runner{Store: store, Audit: func(event, _ string, err error, _ map[string]any) {
		if event == "receipt_pack_list_failed" {
			audit = err
		}
	}}
	discovered, offset := false, 0
	runner.packLocalReceipts(ctx, &discovered, map[string]receiptPackProgress{}, &offset)
	if discovered || !errors.Is(audit, context.DeadlineExceeded) {
		t.Fatalf("expected a retryable discovery timeout in the audit log, discovered=%v err=%v", discovered, audit)
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
	discovered, offset := false, 0
	packs := map[string]receiptPackProgress{}
	tick := func() { runner.packLocalReceipts(ctx, &discovered, packs, &offset) }

	// The first page finishes the direct prefix. A new write to that
	// prefix before the ingest page finishes requires another sweep.
	tick()
	if !discovered || packs[tenant].cursor == "" {
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
