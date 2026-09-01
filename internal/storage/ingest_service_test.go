package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel"
)

func TestIngestServicePreservesTenantFIFOWithOneWriteWorker(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 2
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	upsert := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	deleteRequest := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-2",
		Items: []IngestItem{{
			ExternalID:   "host-1",
			DeleteEntity: &graph.EntityDeleteRequest{ID: "host:1", Source: "agent"},
		}},
	}
	first, err := service.Accept(context.Background(), "tenant-a", upsert)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Accept(context.Background(), "tenant-a", deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := service.Wait(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := service.Wait(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.Version != 1 || secondResult.Version != 2 {
		t.Fatalf("versions = %d, %d; want 1, 2", firstResult.Version, secondResult.Version)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want 2", manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:1"); ok {
		t.Fatal("upsert then delete was reordered")
	}
}

func TestIngestServiceTerminalBatchPublishesAndFinalizesInWALOrder(t *testing.T) {
	observer := &ingestWALTestObserver{}
	config := testIngestServiceConfig(t)
	config.WAL.BufferBytes = 16 * 1024
	config.WAL.Observer = observer
	config.Observer = observer
	wal, initial, err := OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 0 {
		t.Fatalf("initial WAL records = %d, want 0", len(initial))
	}
	ctx, cancel := context.WithCancel(context.Background())
	service := &IngestService{
		store:          NewTenantStore(NewMemoryStore(), "test"),
		wal:            wal,
		config:         config,
		active:         map[string]*ingestPending{},
		activeByStatus: map[string]*ingestPending{},
		runCtx:         ctx,
		cancel:         cancel,
	}
	closed := false
	t.Cleanup(func() {
		cancel()
		if !closed {
			_ = wal.Close()
		}
	})

	items := make([]*ingestPending, 0, 2)
	for index := range 2 {
		request, err := PrepareIngestRequest(
			"tenant-a",
			ingestEntityRequest(fmt.Sprintf("batch-terminal-%d", index), fmt.Sprintf("host:terminal-%d", index)),
		)
		if err != nil {
			t.Fatalf("prepare request %d: %v", index, err)
		}
		acceptedAt := time.Now().UTC()
		envelope := walIngestEnvelope{
			RecordID:   ingestRecordID(ingestRequestIdentity("tenant-a", request)),
			TenantID:   "tenant-a",
			Request:    request,
			AcceptedAt: acceptedAt,
			State:      IngestStateAccepted,
		}
		payload, err := json.Marshal(envelope)
		if err != nil {
			t.Fatalf("marshal accepted envelope %d: %v", index, err)
		}
		appendResult, err := wal.Append(ctx, IngestWALAccepted, payload)
		if err != nil {
			t.Fatalf("append accepted envelope %d: %v", index, err)
		}
		pending := &ingestPending{
			envelope:    envelope,
			acceptedLSN: appendResult.LSN,
			estimated:   acceptedAt,
			bytes:       int64(len(payload) + ingestWALHeaderBytes + ingestWALChecksumBytes),
			state:       IngestStateAccepted,
			done:        make(chan struct{}),
		}
		identity := ingestRequestIdentity("tenant-a", request)
		service.active[identity] = pending
		service.activeByStatus[ingestStatusKey("tenant-a", request.Source, request.CollectorID, request.BatchID)] = pending
		service.pendingBytes += pending.bytes
		items = append(items, pending)
	}

	before := len(observer.syncSnapshot())
	results := []IngestResult{
		{BatchID: items[0].envelope.Request.BatchID, Version: 1, Applied: 1},
		{BatchID: items[1].envelope.Request.BatchID, Version: 2, Applied: 1},
	}
	if retryIndex, err := service.appendTerminalBatch(items, results); err != nil || retryIndex != len(items) {
		t.Fatalf("append terminal batch retryIndex=%d err=%v, want %d and nil", retryIndex, err, len(items))
	}
	for index, pending := range items {
		got, err := service.Wait(context.Background(), IngestAcceptance{
			pending:     pending,
			recordID:    pending.envelope.RecordID,
			completion:  pending.done,
			BatchID:     pending.envelope.Request.BatchID,
			Source:      pending.envelope.Request.Source,
			CollectorID: pending.envelope.Request.CollectorID,
		})
		if err != nil {
			t.Fatalf("wait terminal item %d: %v", index, err)
		}
		if got.BatchID != results[index].BatchID || got.Version != results[index].Version ||
			got.Applied != results[index].Applied || got.Failed != results[index].Failed {
			t.Fatalf("terminal result %d = %#v, want %#v", index, got, results[index])
		}
		if pending.state != IngestStateCommitted {
			t.Fatalf("terminal state %d = %q, want committed", index, pending.state)
		}
	}
	service.mu.Lock()
	active := len(service.active)
	service.mu.Unlock()
	if active != 0 {
		t.Fatalf("active terminal requests = %d, want 0", active)
	}
	syncs := observer.syncSnapshot()
	if len(syncs)-before != 1 || syncs[before].status != "ok" || syncs[before].records != 4 {
		t.Fatalf("terminal WAL sync observations = %#v, want one ok group of 4 records", syncs[before:])
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened, recovered, err := OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(recovered) != 6 {
		t.Fatalf("recovered WAL records = %d, want 6 accepted/published/finalized records", len(recovered))
	}
	wantTypes := []IngestWALRecordType{
		IngestWALAccepted, IngestWALAccepted,
		IngestWALPublished, IngestWALFinalized,
		IngestWALPublished, IngestWALFinalized,
	}
	for index, record := range recovered {
		if record.LSN != uint64(index+1) || record.Type != wantTypes[index] {
			t.Fatalf("recovered record %d = %#v, want LSN/type %d/%d", index, record, index+1, wantTypes[index])
		}
		if index < 2 {
			continue
		}
		if strings.Contains(string(record.Payload), `"result"`) || strings.Contains(string(record.Payload), `"request"`) {
			t.Fatalf("terminal WAL record %d retained request/result payload: %s", index, record.Payload)
		}
		var envelope walIngestEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			t.Fatalf("decode terminal envelope %d: %v", index, err)
		}
		wantState := IngestStatePublished
		if record.Type == IngestWALFinalized {
			wantState = IngestStateCommitted
		}
		if envelope.State != wantState {
			t.Fatalf("terminal envelope %d state=%q, want %q", index, envelope.State, wantState)
		}
	}
	recovery := &IngestService{
		config:         config,
		active:         map[string]*ingestPending{},
		activeByStatus: map[string]*ingestPending{},
	}
	pending, err := recovery.recover(recovered)
	if err != nil {
		t.Fatalf("recover compact terminal records: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after compact terminal recovery = %d, want 0", len(pending))
	}
}

func TestIngestServiceSharesActiveIdempotencyAndRejectsDifferentPayload(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	request := IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			Entity: &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	first, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	shared := request
	shared.BatchID = "batch-2"
	second, err := service.Accept(context.Background(), "tenant-a", shared)
	if err != nil {
		t.Fatal(err)
	}
	if first.acceptedLSN != second.acceptedLSN {
		t.Fatalf("shared request LSNs = %d and %d", first.acceptedLSN, second.acceptedLSN)
	}
	conflict := request
	conflict.Items = []IngestItem{{Entity: &graph.Entity{ID: "host:2", Kind: "host"}}}
	if _, err := service.Accept(context.Background(), "tenant-a", conflict); !errors.Is(err, ErrIngestIdentityConflict) {
		t.Fatalf("different payload err = %v, want ErrIngestIdentityConflict", err)
	}
}

func TestIngestServiceRecoversDurableAcceptedRequest(t *testing.T) {
	recorder := installStorageSpanRecorder(t)
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	traceCtx, acceptedSpan := otel.Tracer("graphdb/test").Start(context.Background(), "test.accepted_request")
	acceptedSpanID := acceptedSpan.SpanContext().SpanID()
	request, err := PrepareIngestRequest("tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-recovered",
		Items: []IngestItem{{
			Entity: &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	digestSum := sha256Sum(requestJSON)
	identity := ingestRequestIdentity("tenant-a", request)
	envelope := walIngestEnvelope{
		RecordID:   ingestRecordID(identity),
		TenantID:   "tenant-a",
		Request:    request,
		Digest:     digestSum,
		AcceptedAt: time.Now().UTC(),
		State:      IngestStateAccepted,
		Trace:      captureWALTraceContext(traceCtx),
	}
	acceptedSpan.End()
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	wal, _, err := OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, payload); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	config.FlushInterval = 5 * time.Millisecond
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	serviceClosed := false
	defer func() {
		if !serviceClosed {
			closeIngestService(t, service)
		}
	}()
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.WriterID != config.OwnerID {
		t.Fatalf("recovered acceptance writer ID = %q, want %q", accepted.WriterID, config.OwnerID)
	}
	result, err := service.Wait(context.Background(), accepted)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 1 || result.BatchID != "batch-recovered" {
		t.Fatalf("recovered result = %#v", result)
	}
	closeIngestService(t, service)
	serviceClosed = true
	flushSpan := requireStorageSpan(t, recorder.Ended(), "graphdb.storage.ingest.flush")
	for _, link := range flushSpan.Links() {
		if link.SpanContext.SpanID() == acceptedSpanID {
			return
		}
	}
	t.Fatalf("recovered flush does not link persisted accepted span %s", acceptedSpanID)
}

func TestIngestServiceRejectsPendingWALOwnedByAnotherWriter(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	request, err := PrepareIngestRequest("tenant-a", ingestEntityRequest("batch-owned-by-b", "host:1"))
	if err != nil {
		t.Fatal(err)
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	envelope := walIngestEnvelope{
		RecordID:   ingestRecordID(ingestRequestIdentity("tenant-a", request)),
		WriterID:   "writer-b",
		TenantID:   "tenant-a",
		Request:    request,
		Digest:     sha256Sum(requestJSON),
		AcceptedAt: time.Now().UTC(),
		State:      IngestStateAccepted,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	wal, _, err := OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, payload); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenIngestService(store, config); err == nil || !strings.Contains(err.Error(), "owner mismatch") {
		t.Fatalf("open with another writer's pending WAL err = %v, want owner mismatch", err)
	}
}

func TestIngestServiceClearsWALFullAfterShortCompletion(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.WAL.MaxBytes = 4096
	config.WAL.SegmentBytes = config.WAL.MaxBytes
	config.WAL.ControlReserveBytes = 512
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	serviceClosed := false
	defer func() {
		if !serviceClosed {
			crashIngestService(t, service)
		}
	}()

	pending := make([]*ingestPending, 0)
	for index := range 128 {
		acceptance, acceptErr := service.Accept(context.Background(), "tenant-a", ingestEntityRequest(
			fmt.Sprintf("batch-full-%d", index), fmt.Sprintf("host:%d", index),
		))
		if errors.Is(acceptErr, ErrIngestWALFull) {
			break
		}
		if acceptErr != nil {
			t.Fatalf("accept %d: %v", index, acceptErr)
		}
		pending = append(pending, acceptance.pending)
	}
	if len(pending) == 0 || len(pending) >= 128 || service.Readiness().Writable {
		t.Fatalf("WAL did not enter full state with a short pending queue: pending=%d readiness=%#v", len(pending), service.Readiness())
	}

	for index, item := range pending {
		service.completePendingState(item, IngestResult{
			BatchID: item.envelope.Request.BatchID,
			Version: int64(index + 1),
			Applied: 1,
		}, nil, IngestStateCommitted)
	}
	if readiness := service.Readiness(); !readiness.Writable {
		t.Fatalf("readiness after %d completions = %#v, want writable after WAL prune", len(pending), readiness)
	}

	acceptance, err := service.Accept(context.Background(), "tenant-a", ingestEntityRequest("batch-after-prune", "host:after-prune"))
	if err != nil {
		t.Fatalf("accept after short completion = %v, want WAL capacity reclaimed", err)
	}
	service.completePendingState(acceptance.pending, IngestResult{
		BatchID: acceptance.BatchID,
		Version: int64(len(pending) + 1),
		Applied: 1,
	}, nil, IngestStateCommitted)
	crashIngestService(t, service)
	serviceClosed = true
}

func TestIngestServiceExactDuplicateDoesNotAdvanceVersion(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	request := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"name": "app"}},
		}},
	}
	first, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := service.Wait(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	request.BatchID = "batch-2"
	second, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := service.Wait(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.Version != 1 || secondResult.Version != 1 || !secondResult.Skipped || secondResult.SkipReason != IngestSkipReasonLogicalNoop {
		t.Fatalf("results = first %#v, second %#v", firstResult, secondResult)
	}
}

func TestIngestServiceExactReplayAfterIdentityAttemptFailureStatusCommitted(t *testing.T) {
	tests := []struct {
		name   string
		object ObjectStore
	}{
		{name: "conditional_delete", object: conditionalDeleteUnsupportedStore{ObjectStore: NewMemoryStore()}},
		{name: "conditional_delete_supported", object: NewMemoryStore()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewTenantStore(test.object, "test")
			config := testIngestServiceConfig(t)
			config.FlushInterval = time.Hour
			config.FlushMaxRequests = 8
			service, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIngestService(t, service)

			flushTenant := func() {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
					t.Fatalf("flush tenant: %v", err)
				}
			}

			payloadA := ingestEntityRequest("batch-attempt-marker", "host:a")
			first, err := service.Accept(context.Background(), "tenant-a", payloadA)
			if err != nil {
				t.Fatalf("accept payload A: %v", err)
			}
			flushTenant()
			firstResult, err := service.Wait(context.Background(), first)
			if err != nil {
				t.Fatalf("wait payload A: %v", err)
			}
			if firstResult.Version != 1 || firstResult.Applied != 1 || firstResult.Failed != 0 {
				t.Fatalf("payload A result = %#v, want committed version 1", firstResult)
			}

			payloadB := payloadA
			payloadB.Items = []IngestItem{{
				ExternalID: "host:b",
				Entity:     &graph.Entity{ID: "host:b", Kind: "host"},
			}}
			conflict, err := service.Accept(context.Background(), "tenant-a", payloadB)
			if err != nil {
				t.Fatalf("accept payload B: %v", err)
			}
			flushTenant()
			conflictResult, err := service.Wait(context.Background(), conflict)
			if err != nil {
				t.Fatalf("wait payload B: %v", err)
			}
			if conflictResult.Applied != 0 || conflictResult.Failed != 1 {
				t.Fatalf("payload B result = %#v, want one failed identity attempt", conflictResult)
			}

			replay, err := service.Accept(context.Background(), "tenant-a", payloadA)
			if err != nil {
				t.Fatalf("accept exact replay A: %v", err)
			}
			flushTenant()
			replayResult, err := service.Wait(context.Background(), replay)
			if err != nil {
				t.Fatalf("wait exact replay A: %v", err)
			}
			if replayResult.Version != 1 || replayResult.Applied != 1 || replayResult.Failed != 0 ||
				!replayResult.Skipped || replayResult.SkipReason != IngestSkipReasonIdempotentReplay {
				t.Fatalf("exact replay A result = %#v, want successful idempotent replay", replayResult)
			}

			status, err := service.Status(
				context.Background(), "tenant-a", payloadA.Source, payloadA.CollectorID, payloadA.BatchID,
			)
			if err != nil {
				t.Fatalf("status after exact replay A: %v", err)
			}
			if status.State != IngestStateCommitted || status.Result == nil ||
				status.Result.Version != 1 || status.Result.Applied != 1 || status.Result.Failed != 0 {
				t.Fatalf("status after exact replay A = %#v, want committed replay result", status)
			}
			if _, err := store.GetIngestAttemptFailure(
				context.Background(), config.OwnerID, "tenant-a", payloadA.Source, payloadA.CollectorID, payloadA.BatchID,
			); !errors.Is(err, ErrNotFound) {
				t.Fatalf("attempt failure marker after exact replay = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestIngestServiceTerminalizesDurableIdentityConflictWithoutBlockingTenant(t *testing.T) {
	tests := []struct {
		name          string
		request       func() IngestRequest
		conflict      func(IngestRequest) IngestRequest
		statusBatchID func(IngestRequest) string
	}{
		{
			name: "batch",
			request: func() IngestRequest {
				return ingestEntityRequest("batch-durable-conflict", "host:first")
			},
			conflict: func(request IngestRequest) IngestRequest {
				request.Items = []IngestItem{{
					ExternalID: "host:conflict",
					Entity:     &graph.Entity{ID: "host:conflict", Kind: "host"},
				}}
				return request
			},
			statusBatchID: func(request IngestRequest) string { return request.BatchID },
		},
		{
			name: "idempotency",
			request: func() IngestRequest {
				request := ingestEntityRequest("batch-durable-idempotency", "host:first")
				request.IdempotencyKey = "idem-durable-conflict"
				return request
			},
			conflict: func(request IngestRequest) IngestRequest {
				request.BatchID = "batch-durable-idempotency-replay"
				request.Items = []IngestItem{{
					ExternalID: "host:conflict",
					Entity:     &graph.Entity{ID: "host:conflict", Kind: "host"},
				}}
				return request
			},
			statusBatchID: func(request IngestRequest) string { return request.BatchID },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewTenantStore(NewMemoryStore(), "test")
			config := testIngestServiceConfig(t)
			config.FlushInterval = time.Hour
			config.FlushMaxRequests = 8
			service, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIngestService(t, service)

			flushTenant := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
					t.Fatalf("flush tenant: %v", err)
				}
			}

			request := test.request()
			first, err := service.Accept(context.Background(), "tenant-a", request)
			if err != nil {
				t.Fatal(err)
			}
			flushTenant()
			firstResult, err := service.Wait(context.Background(), first)
			if err != nil {
				t.Fatalf("wait first request: %v", err)
			}
			if firstResult.Version != 1 || firstResult.Applied != 1 || firstResult.Failed != 0 {
				t.Fatalf("first result = %#v, want one applied mutation at version 1", firstResult)
			}

			conflictRequest := test.conflict(request)
			conflict, err := service.Accept(context.Background(), "tenant-a", conflictRequest)
			if err != nil {
				t.Fatalf("accept durable identity conflict: %v", err)
			}
			if conflict.State != IngestStateAccepted {
				t.Fatalf("conflict acceptance state = %q, want accepted WAL state", conflict.State)
			}
			next, err := service.Accept(
				context.Background(),
				"tenant-a",
				ingestEntityRequest("batch-after-durable-conflict", "host:next"),
			)
			if err != nil {
				t.Fatalf("accept request after durable identity conflict: %v", err)
			}
			// Keep the conflict and the valid request in one tenant flush. A
			// request-local identity failure must not poison its FIFO neighbors.
			flushTenant()
			conflictResult, err := service.Wait(context.Background(), conflict)
			if err != nil {
				t.Fatalf("wait durable identity conflict: %v", err)
			}
			if conflictResult.Applied != 0 || conflictResult.Failed != 1 || len(conflictResult.Failures) != 1 {
				t.Fatalf("conflict result = %#v, want one failed item and no applied items", conflictResult)
			}
			status, err := service.Status(
				context.Background(),
				"tenant-a",
				conflictRequest.Source,
				conflictRequest.CollectorID,
				test.statusBatchID(conflictRequest),
			)
			if err != nil {
				t.Fatalf("status after durable identity conflict: %v", err)
			}
			if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed != 1 {
				t.Fatalf("status after durable identity conflict = %#v, want failed terminal state", status)
			}
			nextResult, err := service.Wait(context.Background(), next)
			if err != nil {
				t.Fatalf("wait request after durable identity conflict: %v", err)
			}
			if nextResult.Version != 2 || nextResult.Applied != 1 || nextResult.Failed != 0 {
				t.Fatalf("next result = %#v, want version 2 with one applied mutation", nextResult)
			}
			loaded, _, err := store.Load(context.Background(), "tenant-a")
			if err != nil {
				t.Fatalf("load graph after durable identity conflict: %v", err)
			}
			if _, ok := loaded.GetEntity("host:next"); !ok {
				t.Fatal("valid request after durable identity conflict was not committed")
			}
		})
	}
}

func TestIngestServiceTerminalizesNonFullSyncEmptyItemsIdentityConflict(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	flushTenant := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
			t.Fatalf("flush tenant: %v", err)
		}
	}

	request := ingestEntityRequest("batch-empty-items-conflict", "host:first")
	request.FullSync = false
	first, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	flushTenant()
	firstResult, err := service.Wait(context.Background(), first)
	if err != nil {
		t.Fatalf("wait first request: %v", err)
	}
	if firstResult.Version != 1 || firstResult.Applied != 1 || firstResult.Failed != 0 {
		t.Fatalf("first result = %#v, want one applied mutation at version 1", firstResult)
	}

	conflictRequest := request
	conflictRequest.Items = []IngestItem{}
	conflictRequest.FullSync = false
	conflict, err := service.Accept(context.Background(), "tenant-a", conflictRequest)
	if err != nil {
		t.Fatalf("accept empty-items identity conflict: %v", err)
	}
	flushTenant()
	conflictResult, err := service.Wait(context.Background(), conflict)
	if err != nil {
		t.Fatalf("wait empty-items identity conflict: %v", err)
	}
	if conflictResult.Applied != 0 || conflictResult.Failed < 1 {
		t.Fatalf("conflict result = %#v, want no applied items and at least one failure", conflictResult)
	}

	status, err := service.Status(
		context.Background(),
		"tenant-a",
		conflictRequest.Source,
		conflictRequest.CollectorID,
		conflictRequest.BatchID,
	)
	if err != nil {
		t.Fatalf("status after empty-items identity conflict: %v", err)
	}
	if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed < 1 {
		t.Fatalf("status after empty-items identity conflict = %#v, want failed terminal state", status)
	}
}

func TestIngestServicePersistsIdentityConflictStatusAcrossRestart(t *testing.T) {
	tests := []struct {
		name     string
		request  func() IngestRequest
		conflict func(IngestRequest) IngestRequest
	}{
		{
			name: "batch",
			request: func() IngestRequest {
				return ingestEntityRequest("batch-restart-conflict", "host:first")
			},
			conflict: func(request IngestRequest) IngestRequest {
				request.Items = []IngestItem{{
					ExternalID: "host:conflict",
					Entity:     &graph.Entity{ID: "host:conflict", Kind: "host"},
				}}
				return request
			},
		},
		{
			name: "idempotency",
			request: func() IngestRequest {
				request := ingestEntityRequest("batch-restart-idempotency", "host:first")
				request.IdempotencyKey = "idem-restart-conflict"
				return request
			},
			conflict: func(request IngestRequest) IngestRequest {
				request.BatchID = "batch-restart-idempotency-replay"
				request.Items = []IngestItem{{
					ExternalID: "host:conflict",
					Entity:     &graph.Entity{ID: "host:conflict", Kind: "host"},
				}}
				return request
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewTenantStore(NewMemoryStore(), "test")
			config := testIngestServiceConfig(t)
			config.FlushInterval = time.Hour
			config.FlushMaxRequests = 8
			service, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					closeIngestService(t, service)
				}
			}()

			flushTenant := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
					t.Fatalf("flush tenant: %v", err)
				}
			}

			request := test.request()
			first, err := service.Accept(context.Background(), "tenant-a", request)
			if err != nil {
				t.Fatal(err)
			}
			flushTenant()
			firstResult, err := service.Wait(context.Background(), first)
			if err != nil {
				t.Fatalf("wait first request: %v", err)
			}
			if firstResult.Version != 1 || firstResult.Applied != 1 || firstResult.Failed != 0 {
				t.Fatalf("first result = %#v, want one applied mutation at version 1", firstResult)
			}

			conflictRequest := test.conflict(request)
			conflict, err := service.Accept(context.Background(), "tenant-a", conflictRequest)
			if err != nil {
				t.Fatalf("accept identity conflict: %v", err)
			}
			flushTenant()
			conflictResult, err := service.Wait(context.Background(), conflict)
			if err != nil {
				t.Fatalf("wait identity conflict: %v", err)
			}
			if conflictResult.Applied != 0 || conflictResult.Failed < 1 {
				t.Fatalf("conflict result = %#v, want no applied items and at least one failure", conflictResult)
			}

			status, err := service.Status(
				context.Background(),
				"tenant-a",
				conflictRequest.Source,
				conflictRequest.CollectorID,
				conflictRequest.BatchID,
			)
			if err != nil {
				t.Fatalf("status before restart: %v", err)
			}
			if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed < 1 {
				t.Fatalf("status before restart = %#v, want failed terminal state", status)
			}

			closeIngestService(t, service)
			closed = true
			reopened, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIngestService(t, reopened)
			status, err = reopened.Status(
				context.Background(),
				"tenant-a",
				conflictRequest.Source,
				conflictRequest.CollectorID,
				conflictRequest.BatchID,
			)
			if err != nil {
				t.Fatalf("status after restart: %v", err)
			}
			if status.State != IngestStateFailed || status.Result == nil ||
				status.Result.Failed < 1 || status.Result.Applied != 0 {
				t.Fatalf("status after restart = %#v, want persisted failed terminal state", status)
			}
		})
	}
}

func TestIngestServiceFailsClosedAfterRuntimeWALFailure(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	fault := &ingestWALFaultFile{failAfterWrite: 1}
	config.WAL.openWriterFile = ingestWALFaultOpener(fault)
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = service.Close(context.Background())
		}
	}()

	prefixRequest := ingestEntityRequest("batch-wal-prefix", "host:prefix")
	_, err = service.Accept(context.Background(), "tenant-a", prefixRequest)
	if err != nil {
		t.Fatalf("accept valid WAL prefix: %v", err)
	}
	readiness := service.Readiness()
	if readiness.Pending != 1 {
		t.Fatalf("readiness after valid prefix = %#v, want one pending request", readiness)
	}

	_, err = service.Accept(
		context.Background(),
		"tenant-a",
		ingestEntityRequest("batch-wal-failure", "host:failure"),
	)
	if !errors.Is(err, ErrIngestWALFailed) {
		t.Fatalf("runtime WAL failure acceptance err = %v, want ErrIngestWALFailed", err)
	}
	readiness = service.Readiness()
	if readiness.Ready || readiness.Writable || readiness.LastError == "" {
		t.Fatalf("readiness after runtime WAL failure = %#v, want not ready/writable with error", readiness)
	}
	_, err = service.Accept(
		context.Background(),
		"tenant-a",
		ingestEntityRequest("batch-wal-after-failure", "host:after-failure"),
	)
	if !errors.Is(err, ErrIngestWALFailed) {
		t.Fatalf("post-failure acceptance err = %v, want ErrIngestWALFailed", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	started := time.Now()
	closeErr := service.Close(closeCtx)
	cancel()
	closed = true
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("close with pending request took %s, want under one second", elapsed)
	}
	if !errors.Is(closeErr, ErrIngestWALFailed) {
		t.Fatalf("close after runtime WAL failure err = %v, want ErrIngestWALFailed", closeErr)
	}

	recoveryConfig := config
	recoveryConfig.WAL.openWriterFile = nil
	reopened, err := OpenIngestService(store, recoveryConfig)
	if err != nil {
		t.Fatalf("reopen after runtime WAL failure: %v", err)
	}
	defer closeIngestService(t, reopened)
	recovered, err := reopened.Accept(context.Background(), "tenant-a", prefixRequest)
	if err != nil {
		t.Fatalf("accept recovered prefix: %v", err)
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	if err := reopened.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush recovered prefix: %v", err)
	}
	recoveredResult, err := reopened.Wait(flushCtx, recovered)
	if err != nil {
		t.Fatalf("wait recovered prefix: %v", err)
	}
	if recoveredResult.Version != 1 || recoveredResult.Applied != 1 || recoveredResult.Failed != 0 {
		t.Fatalf("recovered prefix result = %#v, want one applied mutation at version 1", recoveredResult)
	}
}

func TestIngestServiceDirectCommitBarrierFlushesAcceptedTenantQueue(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	accepted, err := service.Accept(context.Background(), "tenant-a", ingestEntityRequest("batch-1", "host:ingest"))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := store.CommitWithReport(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:direct", Kind: "host"}},
	}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ingestResult, err := service.Wait(context.Background(), accepted)
	if err != nil {
		t.Fatal(err)
	}
	if ingestResult.Version != 1 || commit.Version != 2 {
		t.Fatalf("ingest/direct versions = %d/%d, want 1/2", ingestResult.Version, commit.Version)
	}
}

func TestIngestSchedulerCancellationUnblocksFullDispatchQueue(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service := &IngestService{
		config: IngestServiceConfig{
			FlushInterval: time.Hour,
		},
		enqueueCh:   make(chan *ingestPending, 2),
		forceCh:     make(chan ingestForceRequest),
		completeCh:  make(chan ingestWorkerCompletion),
		shutdownCh:  make(chan struct{}),
		schedulerOK: make(chan struct{}),
		readyCh:     make(chan ingestTenantFlush, 1),
		runCtx:      runCtx,
		cancel:      cancel,
	}
	go service.schedule()

	service.enqueueCh <- &ingestPending{
		envelope: walIngestEnvelope{
			TenantID:   "tenant-a",
			AcceptedAt: time.Now().UTC(),
			Request:    IngestRequest{FullSync: true},
		},
	}
	deadline := time.Now().Add(time.Second)
	for len(service.readyCh) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first tenant was not dispatched")
		}
		time.Sleep(time.Millisecond)
	}

	service.enqueueCh <- &ingestPending{
		envelope: walIngestEnvelope{
			TenantID:   "tenant-b",
			AcceptedAt: time.Now().UTC(),
			Request:    IngestRequest{FullSync: true},
		},
	}
	deadline = time.Now().Add(time.Second)
	for len(service.enqueueCh) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("second tenant was not accepted by the scheduler")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case <-service.schedulerOK:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("scheduler did not stop after cancellation with a full dispatch queue")
	}
}

func TestIngestDurableRetriesMetadataFailureWithoutDuplicateVersion(t *testing.T) {
	objects := &failIngestRecordOnceStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	request := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			Entity: &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	objects.failKey = store.ingestBatchKey("tenant-a", request.Source, request.CollectorID, request.BatchID)
	first, err := store.IngestDurable(context.Background(), "tenant-a", request)
	if err == nil || first.Version != 1 {
		t.Fatalf("first result/err = %#v / %v, want published version and metadata error", first, err)
	}
	second, err := store.IngestDurable(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != 1 || !second.Skipped {
		t.Fatalf("retry result = %#v, want replay of version 1", second)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
}

func TestIngestServiceRecoversPreparedPublishedBatchWithoutDuplicateVersion(t *testing.T) {
	objects := &failIngestRecordOnceStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.RetryInterval = time.Hour
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	first := ingestEntityRequest("batch-1", "host:1")
	second := ingestEntityRequest("batch-2", "host:2")
	objects.failKey = store.ingestBatchKey("tenant-a", first.Source, first.CollectorID, first.BatchID)
	firstAccepted, err := service.Accept(context.Background(), "tenant-a", first)
	if err != nil {
		t.Fatal(err)
	}
	secondAccepted, err := service.Accept(context.Background(), "tenant-a", second)
	if err != nil {
		t.Fatal(err)
	}
	service.forceCh <- ingestForceRequest{
		tenantID:   "tenant-a",
		throughLSN: max(firstAccepted.acceptedLSN, secondAccepted.acceptedLSN),
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		manifest, manifestErr := store.CurrentManifest(context.Background(), "tenant-a")
		status, statusErr := service.Status(
			context.Background(),
			"tenant-a",
			first.Source,
			first.CollectorID,
			first.BatchID,
		)
		if manifestErr == nil && manifest.Version == 2 &&
			statusErr == nil && status.State == IngestStateRetrying {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("prepared publish was not reached: manifest=%#v status=%#v manifestErr=%v statusErr=%v", manifest, status, manifestErr, statusErr)
		}
		time.Sleep(5 * time.Millisecond)
	}

	crashIngestService(t, service)
	recoveryConfig := config
	recoveryConfig.FlushInterval = 10 * time.Millisecond
	recovered, err := OpenIngestService(store, recoveryConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, recovered)

	deadline = time.Now().Add(5 * time.Second)
	for {
		status, statusErr := recovered.Status(
			context.Background(),
			"tenant-a",
			first.Source,
			first.CollectorID,
			first.BatchID,
		)
		if statusErr == nil && status.State == IngestStateCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered request did not commit: status=%#v err=%v", status, statusErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 {
		t.Fatalf("manifest after recovery = %#v", manifest)
	}
}

func TestIngestServicePreparedWALDoesNotDuplicateAcceptedRequest(t *testing.T) {
	const (
		requestLimit = 32 << 20
		sampleBytes  = 2 << 20
	)
	empty := ingestEntityRequest("batch-large", "host:large")
	empty.Items[0].Entity.Fields = graph.Fields{"payload": ""}
	preparedEmpty, err := PrepareIngestRequest("tenant-a", empty)
	if err != nil {
		t.Fatal(err)
	}
	emptyJSON, err := json.Marshal(preparedEmpty)
	if err != nil {
		t.Fatal(err)
	}
	large := ingestEntityRequest("batch-large", "host:large")
	large.Items[0].Entity.Fields = graph.Fields{
		"payload": strings.Repeat("x", sampleBytes),
	}
	request, err := PrepareIngestRequest("tenant-a", large)
	if err != nil {
		t.Fatal(err)
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(requestJSON) - len(emptyJSON); got != sampleBytes {
		t.Fatalf("encoded payload growth = %d, want %d", got, sampleBytes)
	}

	plan := &IngestPreparedRequest{
		FlushID:      "flush-large",
		FinalVersion: 1,
		Result:       IngestResult{BatchID: request.BatchID, Version: 1, Applied: 1},
		Commit: &graph.Commit{
			LayoutVersion: CurrentObjectLayoutVersion,
			ID:            "commit-large",
			TenantID:      "tenant-a",
			Version:       1,
			Mutations: graph.Mutations{
				UpsertEntities: []graph.Entity{*request.Items[0].Entity},
			},
		},
	}
	oldPayload, err := json.Marshal(walIngestEnvelope{
		RecordID: "record-large",
		TenantID: "tenant-a",
		Request:  request,
		State:    IngestStatePrepared,
		Prepared: plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	compactPayload, err := json.Marshal(walPreparedBatchEnvelope{Items: []walPreparedEnvelope{{
		RecordID: "record-large",
		Prepared: plan,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	maxFieldBytes := requestLimit - len(emptyJSON)
	legacyAtRequestLimit := len(oldPayload) + 2*(maxFieldBytes-sampleBytes)
	compactAtRequestLimit := len(compactPayload) + maxFieldBytes - sampleBytes
	if legacyAtRequestLimit <= ingestWALMaxPayload {
		t.Fatalf("projected legacy prepared payload bytes = %d, want > %d", legacyAtRequestLimit, ingestWALMaxPayload)
	}
	if compactAtRequestLimit > ingestWALMaxPayload {
		t.Fatalf("projected compact prepared payload bytes = %d, want <= %d", compactAtRequestLimit, ingestWALMaxPayload)
	}
	if strings.Contains(string(compactPayload[:min(len(compactPayload), 4096)]), `"request"`) {
		t.Fatal("compact prepared payload contains the accepted request")
	}
}

func TestIngestServiceRecoversLegacyPreparedBatchEnvelope(t *testing.T) {
	request, err := PrepareIngestRequest(
		"tenant-a", ingestEntityRequest("batch-legacy-prepared", "host:legacy"),
	)
	if err != nil {
		t.Fatal(err)
	}
	recordID := ingestRecordID(ingestRequestIdentity("tenant-a", request))
	accepted := walIngestEnvelope{
		RecordID: recordID, WriterID: "writer-a", TenantID: "tenant-a",
		Request: request, State: IngestStateAccepted, AcceptedAt: time.Now().UTC(),
	}
	acceptedPayload, err := json.Marshal(accepted)
	if err != nil {
		t.Fatal(err)
	}
	plan := &IngestPreparedRequest{
		FlushID: "flush-legacy", FinalVersion: 1,
		Result: IngestResult{BatchID: request.BatchID, Version: 1, Applied: 1},
		Commit: &graph.Commit{
			ID: "commit-legacy", TenantID: "tenant-a", Version: 1,
			Mutations: graph.Mutations{UpsertEntities: []graph.Entity{*request.Items[0].Entity}},
		},
	}
	legacyPrepared := accepted
	legacyPrepared.State = IngestStatePrepared
	legacyPrepared.Prepared = plan
	legacyPrepared.Result = &plan.Result
	legacyPayload, err := json.Marshal(struct {
		Items []walIngestEnvelope `json:"items"`
	}{Items: []walIngestEnvelope{legacyPrepared}})
	if err != nil {
		t.Fatal(err)
	}
	service := &IngestService{
		config: IngestServiceConfig{OwnerID: "writer-a"},
		active: map[string]*ingestPending{}, activeByStatus: map[string]*ingestPending{},
	}
	pending, err := service.recover([]IngestWALRecord{
		{LSN: 1, Type: IngestWALAccepted, Payload: acceptedPayload},
		{LSN: 2, Type: IngestWALPrepared, Payload: legacyPayload},
	})
	if err != nil {
		t.Fatalf("recover legacy prepared batch: %v", err)
	}
	if len(pending) != 1 || pending[0].state != IngestStatePrepared ||
		pending[0].envelope.Prepared == nil || pending[0].envelope.Prepared.FlushID != plan.FlushID ||
		pending[0].envelope.Request.BatchID != request.BatchID {
		t.Fatalf("legacy prepared recovery = %#v", pending)
	}
}

type failIngestRecordOnceStore struct {
	ObjectStore
	mu      sync.Mutex
	failKey string
	failed  bool
}

func (s *failIngestRecordOnceStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	s.mu.Lock()
	if key == s.failKey && !s.failed {
		s.failed = true
		s.mu.Unlock()
		return ObjectMeta{Key: key}, errors.New("injected ingest metadata failure")
	}
	s.mu.Unlock()
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func testIngestServiceConfig(t *testing.T) IngestServiceConfig {
	t.Helper()
	config := DefaultIngestServiceConfig(t.TempDir())
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.BufferBytes = 1024
	config.WAL.MaxBytes = 16 * 1024 * 1024
	config.WAL.SegmentBytes = 1024 * 1024
	config.FlushInterval = 10 * time.Millisecond
	config.FlushTimeout = 5 * time.Second
	config.RetryInterval = 5 * time.Millisecond
	return config
}

func closeIngestService(t *testing.T, service *IngestService) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatalf("close ingest service: %v", err)
	}
}

func crashIngestService(t *testing.T, service *IngestService) {
	t.Helper()
	service.cancel()
	select {
	case <-service.schedulerOK:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not stop")
	}
	service.workers.Wait()
	if err := service.wal.Close(); err != nil {
		t.Fatalf("close crashed WAL: %v", err)
	}
}

func sha256Sum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
