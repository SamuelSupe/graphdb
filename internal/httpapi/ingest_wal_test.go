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
		State      string `json:"state"`
		Durability string `json:"durability"`
		StatusURL  string `json:"status_url"`
	}
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptedBody); err != nil {
		t.Fatal(err)
	}
	if acceptedBody.State != storage.IngestStateAccepted || acceptedBody.Durability != "durable" || acceptedBody.StatusURL != location {
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
	var result storage.IngestResult
	if err := json.Unmarshal(waitResponse.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Version != 2 {
		t.Fatalf("wait result = %#v", result)
	}
}

func TestHTTPIngestWALBackpressureBeforeCapacity(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	config := storage.DefaultIngestServiceConfig(t.TempDir())
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.BufferBytes = 1024
	config.WAL.MaxBytes = 16 * 1024
	config.WAL.SegmentBytes = 16 * 1024
	config.WALHighWatermark = 1
	config.WALStopWatermark = 99
	config.FlushInterval = time.Hour
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
	request.BatchID = "batch-2"
	request.Items[0].Entity = &graph.Entity{ID: "host:2", Kind: "host"}
	rejected := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request)
	body := rejected.Body.String()
	if rejected.Code != http.StatusTooManyRequests || rejected.Header().Get("Retry-After") != "2" ||
		!strings.Contains(body, `"code":"write_backpressure"`) ||
		!strings.Contains(body, `"code":"ingest_wal_high_watermark"`) {
		t.Fatalf("rejected status=%d retry=%q body=%s", rejected.Code, rejected.Header().Get("Retry-After"), body)
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
	config.Metadata.Mode = storage.IngestMetadataModeSegment
	config.Metadata.FlushInterval = time.Hour
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
		`graphdb_ingest_metadata_queue_pending_requests 0`,
		`graphdb_ingest_metadata_flush_total{status="ok"} 1`,
		`graphdb_ingest_metadata_segment_put_total 1`,
		`graphdb_ingest_metadata_manifest_publish_total 1`,
		`graphdb_ingest_metadata_lookup_total{kind="collector",outcome="miss"} 1`,
		`graphdb_ingest_metadata_lookup_duration_seconds_count{kind="collector",outcome="miss"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	for _, line := range strings.Split(metrics, "\n") {
		if (strings.HasPrefix(line, "graphdb_ingest_wal_") ||
			strings.HasPrefix(line, "graphdb_ingest_queue_") ||
			strings.HasPrefix(line, "graphdb_ingest_flush_") ||
			strings.HasPrefix(line, "graphdb_ingest_metadata_")) &&
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
		`"event":"ingest_metadata_flush_started"`,
		`"event":"ingest_metadata_segment_completed"`,
		`"event":"ingest_metadata_manifest_published"`,
		`"event":"ingest_metadata_flush_completed"`,
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
	metadataSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.metadata_flush")
	metadataEncodeSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.metadata_segment.encode")
	metadataPutSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.metadata_segment.put")
	metadataManifestSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.metadata_manifest.cas")
	metadataLookupSpan := requireRecordedSpan(t, spans, "graphdb.storage.ingest.metadata_lookup")

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
	if metadataEncodeSpan.Parent().SpanID() == (trace.SpanID{}) ||
		metadataPutSpan.Parent().SpanID() == (trace.SpanID{}) ||
		metadataManifestSpan.Parent().SpanID() == (trace.SpanID{}) ||
		metadataLookupSpan.Parent().SpanID() == (trace.SpanID{}) {
		t.Fatal("metadata child span is missing its flush/segment parent")
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
	if !spanHasLink(metadataSpan, acceptSpan.SpanContext().SpanID()) {
		t.Fatalf("metadata flush span does not link accepted request span %s", acceptSpan.SpanContext().SpanID())
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
