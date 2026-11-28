package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWALSingleOversizedPreparationHasDurableFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.WAL.MaxBytes, config.WAL.SegmentBytes = 4096, 4096
	config.WAL.ControlReserveBytes = 512
	config.QueueMemoryBytes = 3400
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { crashIngestService(t, service) })
	request := ingestEntityRequest("oversized-plan", "host:oversized")
	request.Items[0].Entity.Fields["payload"] = strings.Repeat("x", 2700)
	a, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("request should fit at acceptance: %v", err)
	}
	result, err := service.WaitCommitted(ctx, a)
	if err != nil || result.Applied != 0 || len(result.Failures) != 1 || !strings.Contains(result.Failures[0].Error, "accepted request and prepared plan need") {
		t.Fatalf("unfit plan result=%+v error=%v", result, err)
	}
	crashIngestService(t, service)
	reopened, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	service = reopened
	status, err := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil || status.State != IngestStateFailed || status.Result == nil || status.Result.Applied != 0 {
		t.Fatalf("recovered status=%+v error=%v", status, err)
	}
	if _, err = service.Accept(ctx, "tenant-a", ingestEntityRequest("next", "host:next")); err != nil {
		t.Fatalf("unfit plan blocked subsequent acceptance: %v", err)
	}
	if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, manifest, err := store.Load(ctx, "tenant-a"); err != nil || manifest.Version != 1 {
		t.Fatalf("only the next request should publish: %+v, %v", manifest, err)
	}
}

func TestWALRecoversPreparedWorkAtLegacyByteLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	request := ingestEntityRequest("legacy-plan", "host:pending")
	request.Items[0].Entity.Fields["payload"] = strings.Repeat("x", 3500)
	started := time.Now().UTC()
	var plan *IngestPreparedRequest
	stop := errors.New("stop after planning")
	_, err := store.IngestDurableBatchWithHooks(ctx, "tenant-a", []IngestBatchEntry{{Request: request, AcceptedAt: started, AcceptedGeneration: legacyUnboundIngestGeneration}}, IngestBatchHooks{
		Prepared: func(_ context.Context, plans []*IngestPreparedRequest) error {
			plan = plans[0]
			return stop
		},
	})
	if !errors.Is(err, stop) || plan == nil {
		t.Fatalf("capture prepared plan: %v", err)
	}
	wal, err := OpenIngestWAL(config.WAL, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	id := ingestRecordID(ingestRequestIdentity("tenant-a", request))
	var legacyBytes int64
	appendFrame := func(kind IngestWALRecordType, envelope any) {
		t.Helper()
		body, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wal.Append(ctx, kind, body); err != nil {
			t.Fatal(err)
		}
		legacyBytes += int64(len(body) + ingestWALHeaderBytes + ingestWALChecksumBytes)
	}
	appendFrame(IngestWALAccepted, walIngestEnvelope{RecordID: id, TenantID: "tenant-a", Request: request, AcceptedAt: started, State: IngestStateAccepted})
	appendFrame(IngestWALPrepared, walPreparedBatchEnvelope{Items: []walPreparedEnvelope{{RecordID: id, Prepared: plan}}})
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	config.WAL.MaxBytes, config.WAL.SegmentBytes = legacyBytes, legacyBytes
	config.WAL.ControlReserveBytes = 512
	config.QueueMemoryBytes = 1024
	// Reopening without flushing cannot free the legacy volume. Its next
	// start must still be able to write the completion marker and reclaim it.
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	crashIngestService(t, service)
	service, err = OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	status, err := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil || status.State != IngestStateCommitted || status.Result == nil || status.Result.Version != 1 {
		t.Fatalf("legacy WAL did not publish exactly once: %+v, %v", status, err)
	}
	if err := service.prune(ctx); err != nil {
		t.Fatalf("legacy WAL did not return below its limit: %v", err)
	}
}

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

func TestDrainedIngestCheckpointCanBeRemovedAndReopened(t *testing.T) {
	cfg := DefaultIngestWALConfig(t.TempDir())
	wal, err := OpenIngestWAL(cfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	accept := []byte(`{"record_id":"a","tenant_id":"tenant-a","state":"accepted"}`)
	if _, err := wal.Append(t.Context(), IngestWALAccepted, accept); err != nil {
		t.Fatal(err)
	}
	if err := wal.prune(t.Context(), 0, true); err != nil {
		t.Fatal(err)
	}
	final, err := wal.Append(t.Context(), IngestWALFinalized, []byte(`{"record_id":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.prune(t.Context(), 0, true); err != nil {
		t.Fatal(err)
	}
	if wal.hasCheckpoint.Load() {
		t.Fatal("drained WAL still requires a checkpoint")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err = OpenIngestWAL(cfg, func(record IngestWALRecord) error {
		return fmt.Errorf("completed WAL record was replayed: %d", record.LSN)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if next, err := wal.Append(t.Context(), IngestWALAccepted, accept); err != nil || next.LSN != final.LSN+1 {
		t.Fatalf("append after drained checkpoint: %+v, %v", next, err)
	}
}

func TestIndexedIngestCheckpointReadsOnlyLiveWALFrames(t *testing.T) {
	cfg := DefaultIngestWALConfig(t.TempDir())
	cfg.FsyncInterval = time.Microsecond
	initial := ingestRecovery{live: make(ingestWALLiveRecords)}
	wal, err := openIngestWAL(cfg, initial.apply, initial.live)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	appendRecord := func(kind IngestWALRecordType, value any, ids ...string) uint64 {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		result, err := wal.append(t.Context(), kind, data, ids)
		if err != nil {
			t.Fatal(err)
		}
		return result.LSN
	}
	accept := func(id string) uint64 {
		return appendRecord(IngestWALAccepted, walIngestEnvelope{RecordID: id, TenantID: "tenant-a", State: IngestStateAccepted}, id)
	}
	terminal := func(id string) uint64 {
		return appendRecord(IngestWALFinalized, walPendingStateEnvelope{RecordID: id}, id)
	}
	accept("checkpointed")
	if err := wal.prune(t.Context(), 0, true); err != nil {
		t.Fatal(err)
	}
	accept("new")
	accept("reused")
	prepared := walPreparedBatchEnvelope{}
	for _, id := range []string{"checkpointed", "new", "reused"} {
		prepared.Items = append(prepared.Items, walPreparedEnvelope{RecordID: id, Prepared: &IngestPreparedRequest{FlushID: "flush-" + id}})
	}
	appendRecord(IngestWALPrepared, prepared, "checkpointed", "new", "reused")
	terminal("reused")
	reusedLSN := accept("reused")
	var last uint64
	for i := range 64 {
		id := fmt.Sprintf("retired-%d", i)
		accept(id)
		last = terminal(id)
	}
	var full ingestRecovery
	if _, _, _, err := scanIngestWAL(cfg.Dir, full.apply); err != nil {
		t.Fatal(err)
	}
	// All append calls have acknowledged and the writer is waiting for work.
	state := ingestWALWriterState{wal: wal, live: initial.live, checkpointRequired: true, durableLSN: last}
	selected, err := state.recoverLive()
	if err != nil || selected.records != 3 || selected.records >= full.records {
		t.Fatalf("selective replay read %d frames versus %d: %v", selected.records, full.records, err)
	}
	for id, expected := range full.pending {
		if have := selected.pending[id]; have == nil || !reflect.DeepEqual(have.envelope, expected.envelope) {
			t.Fatalf("recovery differs for %q: %+v, want %+v", id, have, expected.envelope)
		}
	}
	if err := wal.prune(t.Context(), 0, true); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := ingestRecovery{live: make(ingestWALLiveRecords)}
	reopened, err := openIngestWAL(cfg, recovered.apply, recovered.live)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(recovered.pending) != 3 || recovered.pending["checkpointed"].envelope.Prepared.FlushID != "flush-checkpointed" ||
		recovered.pending["new"].envelope.Prepared.FlushID != "flush-new" ||
		recovered.pending["reused"].envelope.Prepared != nil || recovered.pending["reused"].acceptedLSN != reusedLSN {
		t.Fatalf("unexpected restart after selective checkpoint: %+v", recovered.pending)
	}
	t.Logf("decoded WAL frames beyond the previous checkpoint: full replay %d, selective replay %d", full.records-1, selected.records)
}

func TestIndexedIngestCheckpointRejectsCorruptLiveFrame(t *testing.T) {
	cfg := DefaultIngestWALConfig(t.TempDir())
	recovery := ingestRecovery{live: make(ingestWALLiveRecords)}
	wal, err := openIngestWAL(cfg, recovery.apply, recovery.live)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	data := []byte(`{"record_id":"active","tenant_id":"tenant-a","state":"accepted"}`)
	frame, err := wal.append(t.Context(), IngestWALAccepted, data, []string{"active"})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(cfg.Dir, frame.Segment), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte{' '}, frame.Offset+ingestWALHeaderBytes)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	if err := wal.prune(t.Context(), 0, true); !errors.Is(err, ErrIngestWALCorrupt) || !errors.Is(err, ErrIngestWALFailed) {
		t.Fatalf("corrupt live WAL frame did not stop checkpointing: %v", err)
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
