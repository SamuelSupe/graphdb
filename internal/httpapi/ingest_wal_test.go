package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPIngestWALAcceptedStatusAndPreferCommitted(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	config := storage.DefaultIngestServiceConfig(t.TempDir())
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.BufferBytes = 1024
	config.WAL.MaxBytes = 16 * 1024 * 1024
	config.WAL.SegmentBytes = 1024 * 1024
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	config.FlushTimeout = 5 * time.Second
	service, err := storage.OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Fatalf("close ingest service: %v", err)
		}
	}()
	handler := (&Server{Store: store, Mode: "all", IngestService: service}).Handler()

	request := storage.IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []storage.IngestItem{{
			Entity: &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	accepted := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request)
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("accepted status = %d body=%s", accepted.Code, accepted.Body.String())
	}
	location := accepted.Header().Get("Location")
	if location != "/v1/ingest/batches/agent/collector-a/batch-1" {
		t.Fatalf("Location = %q", location)
	}
	var acceptedBody struct {
		Source      string `json:"source"`
		CollectorID string `json:"collector_id"`
		State       string `json:"state"`
		Durability  string `json:"durability"`
		StatusURL   string `json:"status_url"`
	}
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptedBody); err != nil {
		t.Fatal(err)
	}
	if acceptedBody.Source != "agent" || acceptedBody.CollectorID != "collector-a" ||
		acceptedBody.State != storage.IngestStateAccepted || acceptedBody.Durability != "durable" || acceptedBody.StatusURL != location {
		t.Fatalf("accepted body = %#v", acceptedBody)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		status := serveJSON(handler, http.MethodGet, location, "tenant-a", nil)
		if status.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", status.Code, status.Body.String())
		}
		var body storage.IngestBatchStatus
		if err := json.Unmarshal(status.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.State == storage.IngestStateCommitted {
			if body.Result == nil || body.Result.Version != 1 {
				t.Fatalf("committed status = %#v", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batch did not commit: %#v", body)
		}
		time.Sleep(5 * time.Millisecond)
	}

	request.BatchID = "batch-2"
	request.Items[0].Entity = &graph.Entity{ID: "host:2", Kind: "host"}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	waitRequest := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(requestJSON))
	waitRequest.Header.Set("X-Tenant-ID", "tenant-a")
	waitRequest.Header.Set("Content-Type", "application/json")
	waitRequest.Header.Set("Prefer", "wait=committed")
	waitResponse := httptest.NewRecorder()
	handler.ServeHTTP(waitResponse, waitRequest)
	if waitResponse.Code != http.StatusOK {
		t.Fatalf("wait status = %d body=%s", waitResponse.Code, waitResponse.Body.String())
	}
	if got := waitResponse.Header().Get("Preference-Applied"); got != "wait=committed" {
		t.Fatalf("Preference-Applied = %q, want wait=committed", got)
	}
	var result storage.IngestResult
	if err := json.Unmarshal(waitResponse.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Version != 2 {
		t.Fatalf("wait result = %#v", result)
	}
}

func TestHTTPIngestWALBatchIdentityConflictUsesStableErrorCode(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	config := storage.DefaultIngestServiceConfig(t.TempDir())
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.BufferBytes = 1024
	config.WAL.MaxBytes = 16 * 1024 * 1024
	config.WAL.SegmentBytes = 1024 * 1024
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	service, err := storage.OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Fatalf("close ingest service: %v", err)
		}
	}()
	handler := (&Server{Store: store, Mode: "all", IngestService: service}).Handler()

	request := storage.IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "batch-conflict",
		IdempotencyKey: "idem-a",
		Items: []storage.IngestItem{{
			Entity: &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	accepted := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request)
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("accepted status = %d body=%s", accepted.Code, accepted.Body.String())
	}
	request.IdempotencyKey = "idem-b"
	conflict := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request)
	var body ErrorResponse
	if err := json.Unmarshal(conflict.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode conflict: %v body=%s", err, conflict.Body.String())
	}
	if conflict.Code != http.StatusConflict || body.Code != ErrorCodeIdempotencyConflict || body.Retryable {
		t.Fatalf("conflict status = %d body=%#v", conflict.Code, body)
	}
}

func TestWriteIngestResultMapsAsynchronousIdentityConflict(t *testing.T) {
	response := httptest.NewRecorder()
	writeIngestResult(response, storage.IngestResult{
		BatchID:   "batch-conflict",
		Failed:    1,
		ErrorCode: storage.IngestErrorIdempotencyConflict,
		Failures: []storage.IngestFailure{{
			Error: "ingest identity conflict",
		}},
	})
	var body ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode conflict: %v body=%s", err, response.Body.String())
	}
	if response.Code != http.StatusConflict || body.Code != ErrorCodeIdempotencyConflict || body.Retryable {
		t.Fatalf("conflict status = %d body=%#v", response.Code, body)
	}
}

func TestHTTPIngestSingleNodeStrictFailureStatusMappings(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}); err != nil {
		t.Fatalf("put source policy: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"state": "ready", "owner": "platform"},
	}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	versionZero := int64(0)
	cases := []struct {
		name       string
		request    storage.IngestRequest
		wantStatus int
		wantCode   ErrorCode
	}{
		{
			name: "version conflict",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "http-version-conflict", ExpectedVersion: &versionZero,
				Items: []storage.IngestItem{{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:2", Kind: "host"}}},
			},
			wantStatus: http.StatusConflict, wantCode: ErrorCodeVersionConflict,
		},
		{
			name: "precondition failure",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "http-precondition-failure",
				Preconditions: []storage.IngestPrecondition{{ResourceType: "entity", ID: "host:1", Field: "state", Op: "eq", Value: "absent"}},
				Items:         []storage.IngestItem{{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"state": "should-not-publish"}}}},
			},
			wantStatus: http.StatusPreconditionFailed, wantCode: ErrorCodePreconditionFailed,
		},
		{
			name: "atomic validation failure",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "http-atomic-validation", FailureMode: storage.IngestFailureModeAtomic,
				Items: []storage.IngestItem{
					{ExternalID: "host-3", Entity: &graph.Entity{ID: "host:3", Kind: "host"}},
					{ExternalID: "invalid"},
				},
			},
			wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeAtomicValidationFailed,
		},
		{
			name: "atomic source suppression",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "http-atomic-suppressed", FailureMode: storage.IngestFailureModeAtomic,
				Items: []storage.IngestItem{
					{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "collector"}}},
					{ExternalID: "host-4", Entity: &graph.Entity{ID: "host:4", Kind: "host", Fields: graph.Fields{"owner": "collector"}}},
				},
			},
			wantStatus: http.StatusConflict, wantCode: ErrorCodeAtomicSuppressed,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", testCase.request)
			if response.Code != testCase.wantStatus {
				t.Fatalf("status = %d body=%s, want %d", response.Code, response.Body.String(), testCase.wantStatus)
			}
			var body ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Code != testCase.wantCode {
				t.Fatalf("error code = %q body=%s, want %q", body.Code, response.Body.String(), testCase.wantCode)
			}
		})
	}

	bestEffort := storage.IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "http-best-effort",
		Items: []storage.IngestItem{
			{ExternalID: "host-5", Entity: &graph.Entity{ID: "host:5", Kind: "host"}},
			{ExternalID: "invalid"},
		},
	}
	response := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", bestEffort)
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("best-effort status = %d body=%s, want 207", response.Code, response.Body.String())
	}
	var result storage.IngestResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode best-effort result: %v", err)
	}
	if result.Applied != 1 || result.Failed != 1 || result.Version == 0 {
		t.Fatalf("best-effort result = %#v, want partial success", result)
	}
}

func TestHTTPIngestWALPreferCommittedStrictFailureStatusMappings(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}); err != nil {
		t.Fatalf("put source policy: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"state": "ready", "owner": "platform"},
	}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	config := storage.DefaultIngestServiceConfig(t.TempDir())
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.BufferBytes = 1024
	config.WAL.MaxBytes = 16 * 1024 * 1024
	config.WAL.SegmentBytes = 1024 * 1024
	config.FlushInterval = time.Millisecond
	config.FlushMaxRequests = 1
	config.FlushTimeout = 5 * time.Second
	config.RetryInterval = time.Millisecond
	service, err := storage.OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(closeCtx); err != nil {
			t.Fatalf("close ingest service: %v", err)
		}
	}()
	handler := (&Server{Store: store, Mode: "all", IngestService: service}).Handler()
	versionZero := int64(0)
	cases := []struct {
		name       string
		request    storage.IngestRequest
		wantStatus int
		wantCode   ErrorCode
	}{
		{
			name: "version conflict",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "wal-version-conflict", ExpectedVersion: &versionZero,
				Items: []storage.IngestItem{{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:2", Kind: "host"}}},
			},
			wantStatus: http.StatusConflict, wantCode: ErrorCodeVersionConflict,
		},
		{
			name: "precondition failure",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "wal-precondition-failure",
				Preconditions: []storage.IngestPrecondition{{ResourceType: "entity", ID: "host:1", Field: "state", Op: "eq", Value: "absent"}},
				Items:         []storage.IngestItem{{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"state": "should-not-publish"}}}},
			},
			wantStatus: http.StatusPreconditionFailed, wantCode: ErrorCodePreconditionFailed,
		},
		{
			name: "atomic validation failure",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "wal-atomic-validation", FailureMode: storage.IngestFailureModeAtomic,
				Items: []storage.IngestItem{
					{ExternalID: "host-3", Entity: &graph.Entity{ID: "host:3", Kind: "host"}},
					{ExternalID: "invalid"},
				},
			},
			wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeAtomicValidationFailed,
		},
		{
			name: "atomic source suppression",
			request: storage.IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "wal-atomic-suppressed", FailureMode: storage.IngestFailureModeAtomic,
				Items: []storage.IngestItem{
					{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "collector"}}},
					{ExternalID: "host-4", Entity: &graph.Entity{ID: "host:4", Kind: "host", Fields: graph.Fields{"owner": "collector"}}},
				},
			},
			wantStatus: http.StatusConflict, wantCode: ErrorCodeAtomicSuppressed,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			requestJSON, err := json.Marshal(testCase.request)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(requestJSON))
			req.Header.Set("X-Tenant-ID", "tenant-a")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Prefer", "wait=committed")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != testCase.wantStatus {
				t.Fatalf("status = %d body=%s, want %d", response.Code, response.Body.String(), testCase.wantStatus)
			}
			var body ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Code != testCase.wantCode {
				t.Fatalf("error code = %q body=%s, want %q", body.Code, response.Body.String(), testCase.wantCode)
			}
		})
	}

	request := storage.IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "wal-best-effort",
		Items: []storage.IngestItem{
			{ExternalID: "host-5", Entity: &graph.Entity{ID: "host:5", Kind: "host"}},
			{ExternalID: "invalid"},
		},
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(requestJSON))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "wait=committed")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("best-effort status = %d body=%s, want 207", response.Code, response.Body.String())
	}
	var result storage.IngestResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode best-effort result: %v", err)
	}
	if result.Applied != 1 || result.Failed != 1 || result.Version == 0 {
		t.Fatalf("best-effort result = %#v, want partial success", result)
	}
}

func TestHTTPIngestWALMetricsLogsAndTraceLinks(t *testing.T) {
	recorder := installSpanRecorder(t)
	var logs bytes.Buffer
	obs := observability.New(&logs, 0)
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	config := storage.DefaultIngestServiceConfig(t.TempDir())
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.BufferBytes = 1024
	config.WAL.MaxBytes = 16 * 1024 * 1024
	config.WAL.SegmentBytes = 1024 * 1024
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 2
	config.FlushTimeout = 5 * time.Second
	config.Observer = obs.Metrics
	config.Logger = obs.Logger
	service, err := storage.OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	serviceClosed := false
	defer func() {
		if serviceClosed {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Fatalf("close ingest service: %v", err)
		}
	}()
	handler := (&Server{
		Store:         store,
		Mode:          "all",
		IngestService: service,
		Observability: obs,
	}).Handler()

	request := storage.IngestRequest{
		Source:      "agent",
		CollectorID: "collector-observed",
		BatchID:     "batch-observed",
		Items: []storage.IngestItem{{
			Entity: &graph.Entity{ID: "host:observed", Kind: "host"},
		}},
	}
	accepted := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-observed", request)
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("accepted status = %d body=%s", accepted.Code, accepted.Body.String())
	}
	location := accepted.Header().Get("Location")
	status := serveJSON(handler, http.MethodGet, location, "tenant-observed", nil)
	if status.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", status.Code, status.Body.String())
	}
	var statusBody storage.IngestBatchStatus
	if err := json.Unmarshal(status.Body.Bytes(), &statusBody); err != nil {
		t.Fatal(err)
	}
	if statusBody.State != storage.IngestStateAccepted {
		t.Fatalf("status state = %q", statusBody.State)
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-observed"); err != nil {
		t.Fatalf("flush tenant: %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := service.Close(closeCtx); err != nil {
		closeCancel()
		t.Fatalf("close ingest service: %v", err)
	}
	closeCancel()
	serviceClosed = true

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, metricsRequest)
	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("metrics status = %d body=%s", metricsResponse.Code, metricsResponse.Body.String())
	}
	metrics := metricsResponse.Body.String()
	for _, want := range []string{
		`graphdb_ingest_wal_append_total{record_type="accepted",status="ok"} 1`,
		`graphdb_ingest_wal_fsync_total{status="ok"}`,
		`graphdb_ingest_queue_pending_requests 0`,
		`graphdb_ingest_queue_cache_hits_total 1`,
		`graphdb_ingest_queue_cache_evictions_total 1`,
		`graphdb_ingest_flush_total{status="ok"} 1`,
		`graphdb_ingest_flush_manifest_publishes_count{status="ok"} 1`,
		`graphdb_ingest_wal_recovery_total{status="ok"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	for _, line := range strings.Split(metrics, "\n") {
		if (strings.HasPrefix(line, "graphdb_ingest_wal_") ||
			strings.HasPrefix(line, "graphdb_ingest_queue_") ||
			strings.HasPrefix(line, "graphdb_ingest_flush_")) &&
			strings.Contains(line, "tenant=") {
			t.Fatalf("ingest WAL metric has high-cardinality tenant label: %s", line)
		}
	}

	logOutput := logs.String()
	for _, want := range []string{
		`"event":"ingest_wal_recovery"`,
		`"event":"ingest_wal_accepted"`,
		`"event":"ingest_flush_started"`,
		`"event":"ingest_flush_completed"`,
		`"event":"ingest_wal_shutdown_completed"`,
		`"tenant":"tenant-observed"`,
		`"trace_id":`,
		`"flush_trace_id":`,
		`"flush_id":`,
		`"first_lsn":1`,
		`"last_lsn":1`,
		`"lsn":`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("logs missing %q:\n%s", want, logOutput)
		}
	}

	spans := recorder.Ended()
	apiSpan := requireRecordedSpan(t, spans, "graphdb.ingest.http")
	acceptSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.accept")
	appendSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest_wal.append")
	groupSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest_wal.write_group")
	flushSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.flush")
	batchSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.batch")
	publishSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.publish")
	metadataSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.finalize_metadata")

	if acceptSpan.Parent().SpanID() != apiSpan.SpanContext().SpanID() {
		t.Fatalf("accept parent = %s, want API span %s", acceptSpan.Parent().SpanID(), apiSpan.SpanContext().SpanID())
	}
	if appendSpan.Parent().SpanID() != acceptSpan.SpanContext().SpanID() {
		t.Fatalf("append parent = %s, want accept span %s", appendSpan.Parent().SpanID(), acceptSpan.SpanContext().SpanID())
	}
	if batchSpan.Parent().SpanID() != flushSpan.SpanContext().SpanID() {
		t.Fatalf("batch parent = %s, want flush span %s", batchSpan.Parent().SpanID(), flushSpan.SpanContext().SpanID())
	}
	if publishSpan.Parent().SpanID() != batchSpan.SpanContext().SpanID() {
		t.Fatalf("publish parent = %s, want batch span %s", publishSpan.Parent().SpanID(), batchSpan.SpanContext().SpanID())
	}
	if metadataSpan.Parent().SpanID() != batchSpan.SpanContext().SpanID() {
		t.Fatalf("metadata parent = %s, want batch span %s", metadataSpan.Parent().SpanID(), batchSpan.SpanContext().SpanID())
	}
	assertSpanAttribute(t, flushSpan, "graphdb.ingest.flush.first_lsn", int64(1))
	assertSpanAttribute(t, flushSpan, "graphdb.ingest.flush.last_lsn", int64(1))
	assertSpanAttribute(t, groupSpan, "graphdb.ingest.wal.first_lsn", int64(1))
	assertSpanAttribute(t, groupSpan, "graphdb.ingest.wal.last_lsn", int64(1))
	flushID := ""
	for _, item := range flushSpan.Attributes() {
		if string(item.Key) == "graphdb.ingest.flush.id" {
			flushID = item.Value.AsString()
		}
	}
	if flushID == "" {
		t.Fatal("flush span is missing its durable flush ID")
	}
	if !spanHasLink(flushSpan, acceptSpan.SpanContext().SpanID()) {
		t.Fatalf("flush span does not link accepted request span %s", acceptSpan.SpanContext().SpanID())
	}
	if !spanHasLink(groupSpan, appendSpan.SpanContext().SpanID()) {
		t.Fatalf("WAL write group does not link append span %s", appendSpan.SpanContext().SpanID())
	}
}

func spanHasLink(span interface {
	Links() []sdktrace.Link
}, spanID trace.SpanID) bool {
	for _, link := range span.Links() {
		if link.SpanContext.SpanID() == spanID {
			return true
		}
	}
	return false
}
