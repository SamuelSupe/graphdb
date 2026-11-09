package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestStandaloneReceiptPackingReclaimsInodesAndPreservesHistory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	store := NewTenantStore(files, "graphdb")
	const tenant = "t"
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}
	direct, err := store.CommitWithReport(ctx, tenant, mutations, CommitOptions{IdempotencyKey: "direct-1"})
	if err != nil {
		t.Fatal(err)
	}
	save := func(i int) IngestRequest {
		t.Helper()
		request := ingestEntityRequest(fmt.Sprintf("batch-%05d", i), "host:a")
		request.IdempotencyKey = fmt.Sprintf("idempotency-%05d", i)
		err := store.saveIngestBatch(ctx, tenant, IngestBatchRecord{Request: request,
			Result:    IngestResult{BatchID: request.BatchID, Version: direct.Version, Applied: 1},
			StartedAt: time.Unix(int64(i), 0), FinishedAt: time.Unix(int64(i), 0)})
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	var first IngestRequest
	for i := range 64 {
		req := save(i)
		if i == 0 {
			first = req
		}
	}
	before, err := files.DiskSpace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	files.EnableLocalReceiptPacking("graphdb")
	store = NewTenantStore(files, "graphdb")
	reclaimed := 0
	for cursor, steps := "", 0; ; steps++ {
		if steps > 16 {
			t.Fatal("packing did not finish")
		}
		next, count, err := store.PackLocalReceipts(ctx, tenant, cursor)
		if err != nil {
			t.Fatal(err)
		}
		reclaimed += count
		if next == "" {
			break
		}
		cursor = next
	}
	if reclaimed != 129 {
		t.Fatalf("expected to reclaim 128 ingest and one direct receipt, got %d", reclaimed)
	}
	after, err := files.DiskSpace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 64; i < 320; i++ {
		save(i)
	}
	final, err := files.DiskSpace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := files.List(ctx, store.tenantObjectPrefix(tenant))
	if err != nil {
		t.Fatal(err)
	}
	packed := 0
	for _, object := range objects {
		if location, loose := files.receipt(ctx, object.Key); loose {
			t.Fatalf("loose receipt remains in %q: %s", location.database, object.Key)
		}
		if filepath.Base(object.Key) == receiptDatabase {
			packed++
		}
	}
	if packed != 1 {
		t.Fatalf("expected one physical receipt database, got %d", packed)
	}
	t.Logf("reclaimed=%d physical receipt database files=%d logical receipts=%d free inodes before=%d after=%d final=%d free bytes final=%d", reclaimed, packed, 641, before.AvailableInodes, after.AvailableInodes, final.AvailableInodes, final.AvailableBytes)

	// Both lookups and collector history remain available after reopening.
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	files.EnableLocalReceiptPacking("graphdb")
	store = NewTenantStore(files, "graphdb")
	record, err := store.GetIngestBatch(ctx, tenant, first.Source, first.CollectorID, first.BatchID)
	if err != nil || record.Result.Version != direct.Version {
		t.Fatalf("legacy batch status lost after packing and restart: %+v %v", record.Result, err)
	}
	replay, found, err := store.loadIngestRecord(ctx, tenant, first)
	if err != nil || !found || replay.Result.BatchID != first.BatchID {
		t.Fatalf("legacy ingest idempotency lost: found=%v err=%v", found, err)
	}
	replayed, err := store.CommitWithReport(ctx, tenant, mutations, CommitOptions{IdempotencyKey: "direct-1"})
	if err != nil || !replayed.IdempotentReplay || replayed.Version != direct.Version {
		t.Fatalf("legacy direct idempotency lost: %+v %v", replayed, err)
	}
	status, err := store.deriveCollectorStatusFromBatches(ctx, tenant, first.Source, first.CollectorID)
	if err != nil || status.AppliedTotal != 320 {
		t.Fatalf("collector history lost or duplicated: %+v %v", status, err)
	}
	// Full runtime backups operate on this physical file under directory ownership.
	if info, err := os.Stat(filepath.Join(root, "graphdb", "tenants", tenant, receiptDatabase)); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("receipt database is not an archivable regular file: %v", err)
	}
}

func TestLocalWALRecoversPackedTerminalReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	config := testIngestServiceConfig(t)
	open := func() (*FileStore, *IngestService) {
		t.Helper()
		files, err := OpenFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		files.EnableLocalReceiptPacking("graphdb")
		service, err := OpenIngestService(NewTenantStore(files, "graphdb"), config)
		if err != nil {
			files.Close()
			t.Fatal(err)
		}
		return files, service
	}
	files, service := open()
	request := ingestEntityRequest("packed-wal", "host:packed")
	request.IdempotencyKey = "packed-retry"
	accepted, err := service.Accept(ctx, "t", request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Wait(ctx, accepted)
	if err != nil || result.Version == 0 {
		t.Fatalf("WAL did not publish packed batch: %+v %v", result, err)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, service = open()
	defer files.Close()
	defer closeIngestService(t, service)
	status, err := service.Status(ctx, "t", request.Source, request.CollectorID, request.BatchID)
	if err != nil || status.Result == nil || status.Result.Version != result.Version {
		t.Fatalf("WAL terminal status did not survive restart: %+v %v", status, err)
	}
	accepted, err = service.Accept(ctx, "t", request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := service.Wait(ctx, accepted)
	if err != nil || replayed.Version != result.Version || !replayed.Skipped {
		t.Fatalf("WAL retried an already published batch: %+v %v", replayed, err)
	}
}

func TestLocalWALAdmissionChecksDataInodesBeforeAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.ConfigureDiskSpace(DiskSpacePolicy{MinFreeInodes: 64}); err != nil {
		t.Fatal(err)
	}
	var available atomic.Int64
	available.Store(32)
	files.diskProbe = func(string) (DiskSpaceStatus, error) {
		return DiskSpaceStatus{AvailableBytes: 1 << 30, TotalInodes: 1000, AvailableInodes: available.Load(), InodesKnown: true}, nil
	}
	// The separate WAL directory itself has no inode pressure.
	service, err := OpenIngestService(NewTenantStore(files, "graphdb"), testIngestServiceConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	request := ingestEntityRequest("inode-blocked", "host:a")
	_, err = service.Accept(ctx, "t", request)
	assertBackpressureReason(t, err, "disk_inodes_low")
	available.Store(128)
	accepted, err := service.Accept(ctx, "t", request)
	if err != nil || accepted.acceptedLSN != 1 {
		t.Fatalf("refused request consumed WAL state: lsn=%d error=%v", accepted.acceptedLSN, err)
	}
	if _, err := service.Wait(ctx, accepted); err != nil {
		t.Fatal(err)
	}
}
