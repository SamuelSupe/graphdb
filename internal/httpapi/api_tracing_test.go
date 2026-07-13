package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestAPITraceRoutes(t *testing.T) {
	tests := []struct {
		method    string
		path      string
		operation string
		route     string
	}{
		{http.MethodGet, "/v1/health", "health", "GET /v1/health"},
		{http.MethodGet, "/v1/tenant-usage", "tenant_usage", "GET /v1/tenant-usage"},
		{http.MethodGet, "/v1/tenants", "tenant.list", "GET /v1/tenants"},
		{http.MethodPost, "/v1/tenants", "tenant.create", "POST /v1/tenants"},
		{http.MethodGet, "/v1/tenants/tenant-a", "tenant.get", "GET /v1/tenants/{tenant_id}"},
		{http.MethodPut, "/v1/tenants/tenant-a", "tenant.update", "PUT /v1/tenants/{tenant_id}"},
		{http.MethodDelete, "/v1/tenants/tenant-a", "tenant.delete", "DELETE /v1/tenants/{tenant_id}"},
		{http.MethodPost, "/v1/tenants/tenant-a/disable", "tenant.disable", "POST /v1/tenants/{tenant_id}/disable"},
		{http.MethodPost, "/v1/tenants/tenant-a/enable", "tenant.enable", "POST /v1/tenants/{tenant_id}/enable"},
		{http.MethodPost, "/v1/tenants/tenant-a/purge", "tenant.purge", "POST /v1/tenants/{tenant_id}/purge"},
		{http.MethodPost, "/v1/tenants/tenant-a/clone", "tenant.clone", "POST /v1/tenants/{tenant_id}/clone"},
		{http.MethodPost, "/v1/tenants/tenant-a/backup", "tenant.backup", "POST /v1/tenants/{tenant_id}/backup"},
		{http.MethodPost, "/v1/tenants/tenant-a/restore", "tenant.restore", "POST /v1/tenants/{tenant_id}/restore"},
		{http.MethodPost, "/v1/tenants/tenant-a/restore-drill", "tenant.restore_drill", "POST /v1/tenants/{tenant_id}/restore-drill"},
		{http.MethodPost, "/v1/ingest/batches", "ingest", "POST /v1/ingest/batches"},
		{http.MethodGet, "/v1/ingest/collectors/source-a/collector-a", "collector_status", "GET /v1/ingest/collectors/{source}/{collector_id}"},
		{http.MethodGet, "/v1/ingest/deadletters/source-a", "deadletter.list", "GET /v1/ingest/deadletters/{source}"},
		{http.MethodPost, "/v1/ingest/deadletters/source-a/replay", "deadletter.replay", "POST /v1/ingest/deadletters/{source}/replay"},
		{http.MethodGet, "/v1/entities", "entity.list", "GET /v1/entities"},
		{http.MethodGet, "/v1/entities/stream", "entity.stream", "GET /v1/entities/stream"},
		{http.MethodGet, "/v1/entities/host%3Aa", "entity.get", "GET /v1/entities/{id}"},
		{http.MethodGet, "/v1/edges", "edge.list", "GET /v1/edges"},
		{http.MethodGet, "/v1/edges/stream", "edge.stream", "GET /v1/edges/stream"},
		{http.MethodGet, "/v1/export/snapshot", "snapshot.export", "GET /v1/export/snapshot"},
		{http.MethodGet, "/v1/export/snapshot/stream", "snapshot.export_stream", "GET /v1/export/snapshot/stream"},
		{http.MethodGet, "/v1/ci-types", "ci_type.list", "GET /v1/ci-types"},
		{http.MethodGet, "/v1/relation-types", "relation_type.list", "GET /v1/relation-types"},
		{http.MethodGet, "/v1/source-policy", "source_policy.get", "GET /v1/source-policy"},
		{http.MethodPut, "/v1/source-policy", "source_policy.update", "PUT /v1/source-policy"},
		{http.MethodGet, "/v1/tenant-config", "tenant_config.get", "GET /v1/tenant-config"},
		{http.MethodPut, "/v1/tenant-config", "tenant_config.update", "PUT /v1/tenant-config"},
		{http.MethodPost, "/v1/query", "query.execute", "POST /v1/query"},
		{http.MethodPost, "/v1/query/stream", "query.stream", "POST /v1/query/stream"},
		{http.MethodPost, "/v1/query/gql", "query.gql", "POST /v1/query/gql"},
		{http.MethodPost, "/v1/query/gql/stream", "query.gql_stream", "POST /v1/query/gql/stream"},
		{http.MethodGet, "/v1/queries/running", "running_query.list", "GET /v1/queries/running"},
		{http.MethodDelete, "/v1/queries/running/query-a", "running_query.kill", "DELETE /v1/queries/running/{id}"},
		{http.MethodGet, "/v1/query/templates", "query_template.list", "GET /v1/query/templates"},
		{http.MethodPost, "/v1/query/templates", "query_template.save", "POST /v1/query/templates"},
		{http.MethodPost, "/v1/query/templates/template-a/run", "query_template.run", "POST /v1/query/templates/{name}/run"},
		{http.MethodGet, "/v1/tasks", "task.list", "GET /v1/tasks"},
		{http.MethodPost, "/v1/tasks", "task.start", "POST /v1/tasks"},
		{http.MethodGet, "/v1/tasks/task-a", "task.get", "GET /v1/tasks/{id}"},
		{http.MethodPost, "/v1/tasks/task-a/cancel", "task.cancel", "POST /v1/tasks/{id}/cancel"},
		{http.MethodPost, "/v1/tasks/task-a/retry", "task.retry", "POST /v1/tasks/{id}/retry"},
		{http.MethodGet, "/v1/indexes", "index.catalog", "GET /v1/indexes"},
		{http.MethodPost, "/v1/indexes", "index.create", "POST /v1/indexes"},
		{http.MethodGet, "/v1/indexes/definitions", "index.definition_list", "GET /v1/indexes/definitions"},
		{http.MethodDelete, "/v1/indexes/definitions/index-a", "index.drop", "DELETE /v1/indexes/definitions/{name}"},
		{http.MethodGet, "/v1/indexes/health", "index.health", "GET /v1/indexes/health"},
		{http.MethodGet, "/v1/indexes/tasks/task-a", "index.task", "GET /v1/indexes/tasks/{task_id}"},
		{http.MethodPost, "/v1/indexes/rebuild", "index.rebuild", "POST /v1/indexes/rebuild"},
		{http.MethodGet, "/v1/control/writer-lease", "control.writer_lease", "GET /v1/control/writer-lease"},
		{http.MethodGet, "/v1/control/reader-lag", "control.reader_lag", "GET /v1/control/reader-lag"},
		{http.MethodGet, "/v1/control/reader-freshness", "control.reader_freshness", "GET /v1/control/reader-freshness"},
		{http.MethodGet, "/v1/control/reader-fleet-readiness", "control.reader_fleet_readiness", "GET /v1/control/reader-fleet-readiness"},
		{http.MethodGet, "/v1/control/reader-traffic-gate", "control.reader_traffic_gate", "GET /v1/control/reader-traffic-gate"},
		{http.MethodGet, "/v1/control/integrity-audit", "control.integrity_audit", "GET /v1/control/integrity-audit"},
		{http.MethodPost, "/v1/control/profiling", "control.profiling", "POST /v1/control/profiling"},
		{http.MethodPost, "/v1/control/recover", "control.recover", "POST /v1/control/recover"},
		{http.MethodPost, "/v1/control/repair", "control.repair", "POST /v1/control/repair"},
		{http.MethodPost, "/v1/control/cleanup-commits", "control.cleanup_commits", "POST /v1/control/cleanup-commits"},
		{http.MethodPost, "/v1/control/gc", "control.gc", "POST /v1/control/gc"},
		{http.MethodPost, "/v1/compact", "compact", "POST /v1/compact"},
		{http.MethodGet, "/v1/not-registered/value-a", "api.request", "GET /v1/unmatched"},
		{"ATTACKER-CONTROLLED-METHOD", "/v1/not-registered/value-a", "api.request", "OTHER /v1/unmatched"},
	}

	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			r := httptest.NewRequest(test.method, test.path, nil)
			got := apiTraceRouteForRequest(r)
			if got.operation != test.operation || got.route != test.route {
				t.Fatalf("route = %#v, want operation=%q route=%q", got, test.operation, test.route)
			}
		})
	}
}

func TestEntityListEmitsDetailedAPITrace(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	recorder := installSpanRecorder(t)
	handler := (&Server{Store: store, Mode: "all", ReadAdmission: NewQueryAdmission(1, 1, 0)}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host", "tenant-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list entities = %d body=%s", rr.Code, rr.Body.String())
	}

	spans := recorder.Ended()
	top := requireRecordedSpan(t, spans, "graphdb.entity.list.http")
	outer := requireRecordedSpan(t, spans, "GET /v1/entities")
	if top.Parent().SpanID() != outer.SpanContext().SpanID() {
		t.Fatalf("API span parent = %s, want outer HTTP span %s", top.Parent().SpanID(), outer.SpanContext().SpanID())
	}
	for _, name := range []string{
		"graphdb.entity.list.tenant_lifecycle_gate",
		"graphdb.entity.list.resolve_tenant",
		"graphdb.entity.list.read_admission.acquire",
		"graphdb.entity.list.read_target",
		"graphdb.storage.scan.entities",
	} {
		span := requireRecordedSpan(t, spans, name)
		if span.Parent().SpanID() != top.SpanContext().SpanID() {
			t.Fatalf("span %q parent = %s, want API span %s", name, span.Parent().SpanID(), top.SpanContext().SpanID())
		}
	}
	assertSpanAttribute(t, top, "graphdb.api.route", "GET /v1/entities")
	assertSpanAttribute(t, top, "graphdb.api.result", "ok")
	scan := requireRecordedSpan(t, spans, "graphdb.storage.scan.entities")
	assertSpanAttribute(t, scan, "graphdb.scan.returned", int64(1))
}

func TestMalformedRequestRecordsDecodeFailure(t *testing.T) {
	recorder := installSpanRecorder(t)
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	req := httptest.NewRequest(http.MethodPut, "/v1/source-policy", strings.NewReader("{"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("source policy = %d body=%s", rr.Code, rr.Body.String())
	}

	spans := recorder.Ended()
	top := requireRecordedSpan(t, spans, "graphdb.source_policy.update.http")
	assertSpanAttribute(t, top, "graphdb.api.result", "client_error")
	decode := requireRecordedSpan(t, spans, "graphdb.source_policy.update.decode_request")
	if decode.Status().Code != codes.Error {
		t.Fatalf("decode status = %v, want error", decode.Status().Code)
	}
}

func TestQueryEmitsExecutionTrace(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	recorder := installSpanRecorder(t)
	handler := (&Server{Store: store, Mode: "all", Admission: NewQueryAdmission(1, 1, 0)}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host"})
	if rr.Code != http.StatusOK {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}

	spans := recorder.Ended()
	top := requireRecordedSpan(t, spans, "graphdb.query.execute.http")
	for _, name := range []string{
		"graphdb.query.execute.decode_request",
		"graphdb.query.execute.query_admission.acquire",
		"graphdb.query.execute.execute_query",
		"graphdb.query.execute.read_target",
		"graphdb.query.execute.current_index_catalog",
		"graphdb.query.execute.read_graph_view",
		"graphdb.query.execute.load_graph",
		"graphdb.query.operator.plan",
		"graphdb.query.operator.admission",
		"graphdb.query.operator.cursor",
		"graphdb.query.operator.entity_scan",
		"graphdb.query.operator.filter_project",
	} {
		requireRecordedSpan(t, spans, name)
	}
	assertSpanAttribute(t, top, "graphdb.query.op", "match")
	assertSpanAttribute(t, top, "graphdb.query.kind", "host")
	execute := requireRecordedSpan(t, spans, "graphdb.query.execute.execute_query")
	assertSpanAttribute(t, execute, "graphdb.query.execution_path", "materialized_graph")
	assertSpanAttribute(t, execute, "graphdb.query.returned", int64(1))
	entityScan := requireRecordedSpan(t, spans, "graphdb.query.operator.entity_scan")
	if entityScan.Parent().SpanID() != execute.SpanContext().SpanID() {
		t.Fatalf("entity scan parent = %s, want execute span %s", entityScan.Parent().SpanID(), execute.SpanContext().SpanID())
	}
	assertSpanAttribute(t, entityScan, "graphdb.query.operator.rows", int64(1))
	filterProject := requireRecordedSpan(t, spans, "graphdb.query.operator.filter_project")
	assertSpanAttribute(t, filterProject, "graphdb.query.operator.scanned", int64(1))
}

func TestLazyQueryNestsPersistedScanUnderQueryOperator(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.RebuildIndexes(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}

	recorder := installSpanRecorder(t)
	handler := (&Server{Store: store, Mode: "all", Admission: NewQueryAdmission(1, 1, 0)}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host"})
	if rr.Code != http.StatusOK {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}

	spans := recorder.Ended()
	execute := requireRecordedSpan(t, spans, "graphdb.query.execute.execute_query")
	assertSpanAttribute(t, execute, "graphdb.query.execution_path", "lazy_index")
	entityScan := requireRecordedSpan(t, spans, "graphdb.query.operator.entity_scan")
	persistedScan := requireRecordedSpan(t, spans, "graphdb.storage.index_lookup.visit_entities")
	if persistedScan.Parent().SpanID() != entityScan.SpanContext().SpanID() {
		t.Fatalf("persisted scan parent = %s, want entity scan %s", persistedScan.Parent().SpanID(), entityScan.SpanContext().SpanID())
	}
	assertSpanAttribute(t, persistedScan, "graphdb.index_lookup.physical_entities_examined", int64(1))
}

func TestCommitKeepsExistingTraceNames(t *testing.T) {
	recorder := installSpanRecorder(t)
	handler := (&Server{Store: storage.NewTenantStore(storage.NewMemoryStore(), "test"), Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{})
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("commit = %d body=%s", rr.Code, rr.Body.String())
	}

	spans := recorder.Ended()
	requireRecordedSpan(t, spans, "graphdb.commit.http")
	for _, span := range spans {
		if span.Name() == "graphdb.api.request.http" {
			t.Fatalf("commit unexpectedly emitted generic API span")
		}
	}
}

func installSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func requireRecordedSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.Name())
	}
	t.Fatalf("span %q not found in %v", name, names)
	return nil
}

func assertSpanAttribute(t *testing.T, span sdktrace.ReadOnlySpan, key string, want any) {
	t.Helper()
	for _, item := range span.Attributes() {
		if string(item.Key) == key {
			if got := traceAttributeValue(item.Value); got != want {
				t.Fatalf("span %q attribute %q = %#v, want %#v", span.Name(), key, got, want)
			}
			return
		}
	}
	t.Fatalf("span %q missing attribute %q", span.Name(), key)
}

func traceAttributeValue(value attribute.Value) any {
	switch value.Type() {
	case attribute.STRING:
		return value.AsString()
	case attribute.INT64:
		return value.AsInt64()
	case attribute.BOOL:
		return value.AsBool()
	default:
		return value.Emit()
	}
}
