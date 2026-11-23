package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSmallCompletionsDoNotRewriteLargePendingCheckpoint(t *testing.T) {
	service := &IngestService{config: DefaultIngestServiceConfig(t.TempDir()), pendingBytes: 256<<20 + 128*1024}
	for i := range 128 {
		pending := &ingestPending{bytes: 1024, done: make(chan struct{}), envelope: walIngestEnvelope{RecordID: fmt.Sprint(i), TenantID: "tenant-a", Request: IngestRequest{Source: "agent", CollectorID: "collector", BatchID: fmt.Sprint(i)}}}
		service.completePendingStateLocked(pending, IngestResult{}, nil, IngestStateCommitted)
	}
	if service.finishPendingCompletionsLocked() {
		t.Fatal("128KiB of completions triggered a 256MiB checkpoint rewrite")
	}
}
func TestIngestQueueMustFitCheckpointCapacity(t *testing.T) {
	config := DefaultIngestServiceConfig(t.TempDir())
	config.WAL.MaxBytes = 4 << 20
	config.WAL.SegmentBytes = 1 << 20
	config.QueueMemoryBytes = 4 << 20
	if err := config.Validate(); err == nil {
		t.Fatal("queue may grow beyond reclaimable WAL payload capacity")
	}
}

func TestIngestWALCoalescesStaleCheckpointRequests(t *testing.T) {
	cfg := DefaultIngestWALConfig(t.TempDir())
	wal, err := OpenIngestWAL(cfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if _, err = wal.Append(context.Background(), IngestWALAccepted, []byte(`{"record_id":"a","tenant_id":"tenant-a","state":"accepted"}`)); err != nil {
		t.Fatal(err)
	}
	generation := wal.checkpointGeneration.Load()
	if err = wal.prune(context.Background(), 0, true, generation); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(cfg.Dir, ingestWALCheckpointFile))
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err = wal.prune(context.Background(), 0, true, generation); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := os.Stat(filepath.Join(cfg.Dir, ingestWALCheckpointFile))
	if !os.SameFile(before, after) {
		t.Fatal("duplicate pressure requests rewrote pending checkpoint")
	}
}

func TestRecoveryReusesCheckpointAndDrainsPendingWrites(t *testing.T) {
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1024
	config.WAL.AppendQueue = 2
	store := NewTenantStore(NewMemoryStore(), "test")
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	var lastRequest IngestRequest
	for i := range 64 {
		id := fmt.Sprint(i)
		lastRequest = ingestEntityRequest(id, "host:"+id)
		_, err = service.Accept(t.Context(), "a", lastRequest)
		if err != nil {
			crashIngestService(t, service)
			t.Fatal(err)
		}
	}
	if err := service.prune(t.Context()); err != nil {
		crashIngestService(t, service)
		t.Fatal(err)
	}
	crashIngestService(t, service)
	path := filepath.Join(config.WAL.Dir, ingestWALCheckpointFile)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	config.FlushMaxRequests = 2
	config.FlushInterval = time.Millisecond
	service, err = OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("reopening compacted WAL replaced the pending checkpoint")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	last, err := service.Accept(ctx, "a", lastRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Wait(ctx, last); err != nil {
		t.Fatalf("recovered write: %v", err)
	}
}
