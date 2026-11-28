package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestRound4IngestWALOSModeCheckpointAndRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal-os-checkpoint")
	cfg := DefaultIngestWALConfig(dir)
	cfg.Durability = IngestWALDurabilityOS
	cfg.SegmentBytes = 1024
	wal, err := OpenIngestWAL(cfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	env1, _ := json.Marshal(walIngestEnvelope{RecordID: "req-1", TenantID: "tenant-a", AcceptedLSN: 1, Request: IngestRequest{Source: "s", CollectorID: "c", BatchID: "b1"}})
	if _, err := wal.Append(context.Background(), IngestWALAccepted, env1); err != nil {
		t.Fatal(err)
	}
	if err := wal.prune(context.Background(), 0, true); err != nil {
		t.Fatalf("checkpoint with pending record in OS mode failed: %v", err)
	}
	env2, _ := json.Marshal(walIngestEnvelope{RecordID: "req-2", TenantID: "tenant-a", AcceptedLSN: 2, Request: IngestRequest{Source: "s", CollectorID: "c", BatchID: "b2"}})
	if _, err := wal.Append(context.Background(), IngestWALAccepted, env2); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	replayed := 0
	wal2, err := OpenIngestWAL(cfg, func(IngestWALRecord) error {
		replayed++
		return nil
	})
	if err != nil {
		t.Fatalf("reopen OS-mode WAL after checkpoint failed: %v", err)
	}
	defer wal2.Close()
	if replayed < 2 {
		t.Fatalf("expected >= 2 replayed records after OS checkpoint restart, got %d", replayed)
	}
}

func TestRound4TaskOwnerStoppedRespectsSharedObjectStoreRemoteOwner(t *testing.T) {
	mem := NewMemoryStore()
	nodeB := NewTenantStoreWithOptions(mem, "test", TenantStoreOptions{InstanceID: "node-b"})
	defer nodeB.ShutdownTasks(context.Background())

	remoteTask := Task{
		TenantID:  "tenant-a",
		ID:        "task-remote",
		Type:      TaskTypeTenantBackup,
		Status:    TaskStatusRunning,
		OwnerID:   "node-a",
		UpdatedAt: time.Now().UTC(),
	}
	if nodeB.taskOwnerStopped(remoteTask) {
		t.Fatal("shared object store node-b erroneously marked node-a's active task as stopped")
	}
	staleRemoteTask := remoteTask
	staleRemoteTask.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	if !nodeB.taskOwnerStopped(staleRemoteTask) {
		t.Fatal("node-b failed to recover truly stale crashed remote task")
	}

	ownDeadTask := remoteTask
	ownDeadTask.OwnerID = "node-b"
	if !nodeB.taskOwnerStopped(ownDeadTask) {
		t.Fatal("node-b failed to detect its own dead task")
	}
}

func TestRound4ParquetColumnBoundsCheck(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).Append("hello")
	rec := builder.NewRecordBatch()
	defer rec.Release()

	if _, err := parquetio.StringColumn(rec, 1, "missing"); err == nil {
		t.Fatal("expected out-of-range error from StringColumn")
	}
	if _, err := parquetio.Int64Column(rec, -1, "neg"); err == nil {
		t.Fatal("expected out-of-range error from Int64Column")
	}
	if _, err := parquetio.BoolColumn(rec, 5, "missing"); err == nil {
		t.Fatal("expected out-of-range error from BoolColumn")
	}
	if _, err := parquetio.Float64Column(rec, 2, "missing"); err == nil {
		t.Fatal("expected out-of-range error from Float64Column")
	}
}
