package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
