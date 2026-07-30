package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

func TestIngestMetadataSegmentCompressesBurstObjectWrites(t *testing.T) {
	base := NewMemoryStore()
	objects := &countingConditionalStore{ObjectStore: base, puts: map[string]int{}}
	store := NewTenantStore(objects, "test")
	config := testIngestServiceConfig(t)
	config.Metadata.Mode = IngestMetadataModeSegment
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 256
	config.Metadata.FlushInterval = time.Hour
	config.Metadata.MaxRequests = 256
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	accepted := make([]IngestAcceptance, 0, 256)
	for index := range 256 {
		request := ingestEntityRequest(
			fmt.Sprintf("batch-%03d", index),
			fmt.Sprintf("host:%03d", index),
		)
		request.IdempotencyKey = fmt.Sprintf("idem-%03d", index)
		item, err := service.Accept(context.Background(), "tenant-a", request)
		if err != nil {
			t.Fatal(err)
		}
		accepted = append(accepted, item)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, item := range accepted {
		if _, err := service.Wait(ctx, item); err != nil {
			t.Fatal(err)
		}
	}

	segments, err := objects.List(ctx, "test/tenants/tenant-a/ingest/metadata/segments/")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("metadata segments = %d, want 1", len(segments))
	}
	if got := objects.putCount(segments[0].Key); got != 1 {
		t.Fatalf("metadata segment PUTs = %d, want 1", got)
	}
	if _, err := objects.Get(ctx, store.ingestMetadataManifestKey("tenant-a")); err != nil {
		t.Fatalf("metadata manifest: %v", err)
	}
	if got := objects.putCount(store.ingestMetadataManifestKey("tenant-a")); got != 1 {
		t.Fatalf("metadata manifest publishes = %d, want 1", got)
	}
	for _, prefix := range []string{
		"test/tenants/tenant-a/ingest/agent/batches/",
		"test/tenants/tenant-a/ingest/agent/idempotency/",
		"test/tenants/tenant-a/ingest/agent/collectors/",
	} {
		items, err := objects.List(ctx, prefix)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 0 {
			t.Fatalf("legacy metadata objects under %q = %d, want 0", prefix, len(items))
		}
	}
	record, err := store.GetIngestBatch(ctx, "tenant-a", "agent", "collector-a", "batch-255")
	if err != nil {
		t.Fatal(err)
	}
	if record.Request.IdempotencyKey != "idem-255" || record.Result.Version != 256 {
		t.Fatalf("segment batch lookup = %#v", record)
	}
	replay := ingestEntityRequest("batch-replay", "host:255")
	replay.IdempotencyKey = "idem-255"
	if replayRecord, ok, err := store.loadIngestRecord(ctx, "tenant-a", replay); err != nil || !ok || replayRecord.Result.BatchID != "batch-255" {
		t.Fatalf("segment idempotency lookup = %#v, %v, %v", replayRecord, ok, err)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "agent", "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	if status.AppliedTotal != 256 || status.LastVersion != 256 {
		t.Fatalf("collector status = %#v", status)
	}
}

func TestIngestMetadataBatchesAcrossGraphFlushesAndPreservesNoop(t *testing.T) {
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	config := testIngestServiceConfig(t)
	config.Metadata.Mode = IngestMetadataModeSegment
	config.FlushMaxRequests = 1
	config.Metadata.FlushInterval = time.Hour
	config.Metadata.MaxRequests = 2
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	firstRequest := ingestEntityRequest("batch-1", "host:1")
	first, err := service.Accept(context.Background(), "tenant-a", firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, statusErr := service.Status(context.Background(), "tenant-a", firstRequest.Source, firstRequest.CollectorID, firstRequest.BatchID)
		if statusErr == nil && status.State == IngestStatePublished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("first graph flush did not publish: status=%#v err=%v", status, statusErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	secondRequest := ingestEntityRequest("batch-2", "host:1")
	second, err := service.Accept(context.Background(), "tenant-a", secondRequest)
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
	if firstResult.Version != 1 || secondResult.Version != 1 ||
		!secondResult.Skipped || secondResult.SkipReason != IngestSkipReasonLogicalNoop {
		t.Fatalf("cross-flush results = %#v / %#v", firstResult, secondResult)
	}
	segments, err := objects.List(context.Background(), "test/tenants/tenant-a/ingest/metadata/segments/")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("cross-flush metadata segments = %d, want 1", len(segments))
	}
}

func TestIngestMetadataPublishedIsVisibleOnlyAfterManifest(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.Metadata.Mode = IngestMetadataModeSegment
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	config.Metadata.FlushInterval = time.Hour
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	request := ingestEntityRequest("batch-1", "host:1")
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, statusErr := service.Status(context.Background(), "tenant-a", request.Source, request.CollectorID, request.BatchID)
		if statusErr == nil && status.State == IngestStatePublished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("request did not reach published: status=%#v err=%v", status, statusErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := store.GetIngestBatch(context.Background(), "tenant-a", request.Source, request.CollectorID, request.BatchID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("batch before metadata manifest err = %v, want ErrNotFound", err)
	}
	if _, err := service.WaitCommitted(context.Background(), accepted); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetIngestBatch(context.Background(), "tenant-a", request.Source, request.CollectorID, request.BatchID); err != nil {
		t.Fatalf("batch after metadata manifest: %v", err)
	}
}

func TestIngestMetadataRecoversSegmentPutBeforeManifest(t *testing.T) {
	base := NewMemoryStore()
	objects := &failConditionalKeyOnceStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	config := testIngestServiceConfig(t)
	config.Metadata.Mode = IngestMetadataModeSegment
	config.FlushMaxRequests = 1
	config.Metadata.MaxRequests = 1
	config.RetryInterval = time.Hour
	objects.failKey = store.ingestMetadataManifestKey("tenant-a")
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	request := ingestEntityRequest("batch-1", "host:1")
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, statusErr := service.Status(context.Background(), "tenant-a", request.Source, request.CollectorID, request.BatchID)
		segments, _ := base.List(context.Background(), "test/tenants/tenant-a/ingest/metadata/segments/")
		if statusErr == nil && status.State == IngestStateRetrying && len(segments) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metadata failure not reached: status=%#v err=%v segments=%d", status, statusErr, len(segments))
		}
		time.Sleep(5 * time.Millisecond)
	}
	secondRequest := ingestEntityRequest("batch-2", "host:2")
	if _, err := service.Accept(context.Background(), "tenant-a", secondRequest); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		status, statusErr := service.Status(context.Background(), "tenant-a", secondRequest.Source, secondRequest.CollectorID, secondRequest.BatchID)
		if statusErr == nil && status.State == IngestStatePublished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second graph publication not reached: status=%#v err=%v", status, statusErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	crashIngestService(t, service)

	recovery := config
	recovery.RetryInterval = 5 * time.Millisecond
	recovered, err := OpenIngestService(store, recovery)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, recovered)
	recoveredAcceptance, err := recovered.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := recovered.WaitCommitted(context.Background(), recoveredAcceptance)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 1 {
		t.Fatalf("recovered result = %#v", result)
	}
	secondAcceptance, err := recovered.Accept(context.Background(), "tenant-a", secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := recovered.WaitCommitted(context.Background(), secondAcceptance)
	if err != nil {
		t.Fatal(err)
	}
	if secondResult.Version != 2 {
		t.Fatalf("second recovered result = %#v", secondResult)
	}
	segments, err := base.List(context.Background(), "test/tenants/tenant-a/ingest/metadata/segments/")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 {
		t.Fatalf("recovery created %d metadata segments, want 2 without an orphan duplicate", len(segments))
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 {
		t.Fatalf("graph version after metadata recovery = %d, want 2", manifest.Version)
	}
	_ = accepted
}

func TestIngestMetadataIndexKeepsArchivedHistoryReadable(t *testing.T) {
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.IngestMetadataMode = IngestMetadataModeSegment
	ctx := context.Background()
	for index := 1; index <= 41; index++ {
		request := ingestEntityRequest(fmt.Sprintf("batch-%02d", index), fmt.Sprintf("host:%02d", index))
		result := IngestResult{BatchID: request.BatchID, Version: int64(index), Applied: 1}
		record := IngestBatchRecord{
			TenantID: "tenant-a", Request: request, Result: result,
			StartedAt: time.Unix(int64(index), 0).UTC(), FinishedAt: time.Unix(int64(index), 1).UTC(),
		}
		if _, err := store.publishIngestMetadataSegment(ctx, "tenant-a", []ingestMetadataRecord{{
			AcceptedLSN: uint64(index), Digest: fmt.Sprintf("digest-%02d", index), Batch: record,
		}}); err != nil {
			t.Fatal(err)
		}
	}
	manifest, _, err := store.loadIngestMetadataManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Recent) != ingestMetadataRecentLimit {
		t.Fatalf("recent refs = %d, want %d", len(manifest.Recent), ingestMetadataRecentLimit)
	}
	if len(manifest.Indexes) != 1 || manifest.Indexes[0].Level != 1 {
		t.Fatalf("index refs = %#v, want one level-1 catalog", manifest.Indexes)
	}
	record, err := store.GetIngestBatch(ctx, "tenant-a", "agent", "collector-a", "batch-01")
	if err != nil {
		t.Fatal(err)
	}
	if record.Result.Version != 1 {
		t.Fatalf("archived record = %#v", record)
	}
}

func TestLegacyIngestRejectsTenantAfterSegmentActivation(t *testing.T) {
	objects := NewMemoryStore()
	segmentWriter := NewTenantStore(objects, "test")
	segmentWriter.IngestMetadataMode = IngestMetadataModeSegment
	request := ingestEntityRequest("batch-1", "host:1")
	record := IngestBatchRecord{
		TenantID: "tenant-a", Request: request,
		Result:    IngestResult{BatchID: request.BatchID, Version: 1, Applied: 1},
		StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(),
	}
	if _, err := segmentWriter.publishIngestMetadataSegment(context.Background(), "tenant-a", []ingestMetadataRecord{{
		AcceptedLSN: 1, Digest: "digest", Batch: record,
	}}); err != nil {
		t.Fatal(err)
	}
	legacy := NewTenantStore(objects, "test")
	_, err := legacy.Ingest(context.Background(), "tenant-a", ingestEntityRequest("batch-2", "host:2"))
	if !errors.Is(err, ErrIngestMetadataFormatActivated) {
		t.Fatalf("legacy write err = %v, want ErrIngestMetadataFormatActivated", err)
	}
}

type failIngestRecordOnceStore struct {
	ObjectStore
	mu      sync.Mutex
	failKey string
	failed  bool
}

type failConditionalKeyOnceStore struct {
	ObjectStore
	mu      sync.Mutex
	failKey string
	failed  bool
}

type countingConditionalStore struct {
	ObjectStore
	mu   sync.Mutex
	puts map[string]int
}

func (s *countingConditionalStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err == nil {
		s.mu.Lock()
		s.puts[key]++
		s.mu.Unlock()
	}
	return meta, err
}

func (s *countingConditionalStore) putCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts[key]
}

func (s *failConditionalKeyOnceStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	s.mu.Lock()
	if key == s.failKey && !s.failed {
		s.failed = true
		s.mu.Unlock()
		return ObjectMeta{Key: key}, errors.New("injected conditional write failure")
	}
	s.mu.Unlock()
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
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
	if service.config.Metadata.Mode == IngestMetadataModeSegment {
		select {
		case <-service.metadataSchedulerOK:
		case <-time.After(5 * time.Second):
			t.Fatal("metadata scheduler did not stop")
		}
		service.metadataWorkers.Wait()
	}
	if err := service.wal.Close(); err != nil {
		t.Fatalf("close crashed WAL: %v", err)
	}
}

func sha256Sum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
