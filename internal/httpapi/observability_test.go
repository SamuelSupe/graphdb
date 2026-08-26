package httpapi

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestMetricsEndpointRecordsHTTPQueryAndSuppressedConflicts(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}); err != nil {
		t.Fatalf("source policy: %v", err)
	}
	obs := observability.New(io.Discard, 0)
	obs.Metrics.RecordCoordinatorCleanup("ok", 3, 0)
	handler := (&Server{Store: store, Mode: "all", Observability: obs, ReadAdmission: NewQueryAdmission(1, 1, 0)}).Handler()
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Source: "manual", Fields: graph.Fields{"region": "manual"}}},
	}}); rr.Code != http.StatusOK {
		t.Fatalf("manual commit = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Source: "agent", Fields: graph.Fields{"region": "agent"}}},
	}}); rr.Code != http.StatusOK {
		t.Fatalf("agent commit = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host"}); rr.Code != http.StatusOK {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host", "tenant-a", nil); rr.Code != http.StatusOK {
		t.Fatalf("scan = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host", MinVersion: 99}); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale query = %d body=%s", rr.Code, rr.Body.String())
	}
	rr := serveJSON(handler, http.MethodGet, "/metrics", "", nil)
	body := rr.Body.String()
	if rr.Code != http.StatusOK || !strings.Contains(body, "graphdb_http_requests_total") {
		t.Fatalf("metrics = %d body=%s", rr.Code, body)
	}
	for _, want := range []string{
		`graphdb_queries_total{tenant="tenant-a",op="match",status="ok"} 1`,
		`graphdb_queries_total{tenant="tenant-a",op="match",status="error"} 1`,
		`graphdb_write_suppressed_conflicts_total{tenant="tenant-a",resource_type="entity"} 1`,
		`graphdb_reader_not_fresh_total{tenant="tenant-a",reason="manifest_behind"} 1`,
		`graphdb_reader_visible_version{tenant="tenant-a"} 1`,
		`graphdb_reader_catchup_total{tenant="tenant-a",status="ok"} 1`,
		`graphdb_coordinator_status{backend="local",metric="available"} 1`,
		`graphdb_coordinator_cleanup_deleted_total{table="commit_idempotency"} 3`,
		`graphdb_coordinator_cleanup_runs_total{status="ok"} 1`,
		`graphdb_http_requests_total{method="POST",route="POST /v1/query",status="200"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, body)
		}
	}
	if !strings.Contains(body, `graphdb_read_admission_queue_seconds_bucket{tenant="tenant-a",status="ok",le=`) {
		t.Fatalf("metrics missing read admission histogram in:\n%s", body)
	}
	if !strings.Contains(body, `graphdb_reader_catchup_seconds_bucket{tenant="tenant-a",status="ok",le=`) {
		t.Fatalf("metrics missing reader catchup histogram in:\n%s", body)
	}
}

func TestSuccessfulWALIngestHTTPLogsAreSampledButFailuresAreNot(t *testing.T) {
	var logs bytes.Buffer
	obs := observability.New(&logs, 0)
	status := http.StatusAccepted
	handler := (&Server{Observability: obs}).observeHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))

	serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", nil)
	serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", nil)
	status = http.StatusInternalServerError
	serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", nil)

	if got := strings.Count(logs.String(), `"event":"http_request"`); got != 2 {
		t.Fatalf("http request log count = %d, want first success and failure", got)
	}
	metrics := string(obs.Metrics.SnapshotPrometheus())
	for _, want := range []string{
		`graphdb_http_requests_total{method="POST",route="POST /v1/ingest/batches",status="202"} 2`,
		`graphdb_http_requests_total{method="POST",route="POST /v1/ingest/batches",status="500"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, metrics)
		}
	}
}

func TestIngestLogicalNoopReportsReasonAndMetric(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	obs := observability.New(io.Discard, 0)
	handler := (&Server{Store: store, Mode: "all", Observability: obs}).Handler()
	request := storage.IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []storage.IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		}},
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request); rr.Code != http.StatusOK {
		t.Fatalf("first ingest = %d body=%s", rr.Code, rr.Body.String())
	}
	request.BatchID = "batch-2"
	rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"skip_reason":"logical_noop"`) {
		t.Fatalf("logical no-op ingest = %d body=%s", rr.Code, rr.Body.String())
	}
	metrics := string(obs.Metrics.SnapshotPrometheus())
	if !strings.Contains(metrics, `graphdb_ingest_skipped_total{tenant="tenant-a",source="agent",reason="logical_noop"} 1`) {
		t.Fatalf("logical no-op metric missing:\n%s", metrics)
	}
}

func TestSlowQueryWritesStructuredLog(t *testing.T) {
	var log bytes.Buffer
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	obs := observability.New(&log, time.Nanosecond)
	handler := (&Server{Store: store, Mode: "all", Observability: obs}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host"})
	if rr.Code != http.StatusOK {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}
	text := log.String()
	if !strings.Contains(text, `"event":"slow_query"`) || !strings.Contains(text, `"tenant":"tenant-a"`) || !strings.Contains(text, `"op":"match"`) {
		t.Fatalf("slow query log missing fields:\n%s", text)
	}
}
