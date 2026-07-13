package httpapi

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type apiTraceContextKey struct{}

type apiTraceState struct {
	operation string
	prefix    string
	route     string
	span      trace.Span
}

type apiTraceRoute struct {
	operation string
	route     string
}

func startAPIRequestTrace(r *http.Request) (*http.Request, trace.Span, bool) {
	if !strings.HasPrefix(r.URL.Path, "/v1/") || r.URL.Path == "/v1/commits" {
		return r, nil, false
	}
	route := apiTraceRouteForRequest(r)
	prefix := "graphdb." + route.operation
	attrs := []attribute.KeyValue{
		attribute.String("graphdb.api.operation", route.operation),
		attribute.String("graphdb.api.route", route.route),
		attribute.String("graphdb.tenant", r.Header.Get("X-Tenant-ID")),
		attribute.String("http.request.method", r.Method),
		attribute.String("http.route", route.route),
		attribute.Bool("graphdb.api.streaming", strings.HasSuffix(r.URL.Path, "/stream")),
	}
	if r.ContentLength >= 0 {
		attrs = append(attrs, attribute.Int64("graphdb.request.content_length", r.ContentLength))
	}
	ctx, span := otel.Tracer("graphdb/http").Start(r.Context(), prefix+".http", trace.WithAttributes(attrs...))
	state := apiTraceState{operation: route.operation, prefix: prefix, route: route.route, span: span}
	ctx = context.WithValue(ctx, apiTraceContextKey{}, state)
	return r.WithContext(ctx), span, true
}

func endAPIRequestTrace(span trace.Span, status int) {
	if span == nil {
		return
	}
	result := "ok"
	if status >= 500 {
		result = "server_error"
	} else if status >= 400 {
		result = "client_error"
	}
	span.SetAttributes(
		attribute.Int("http.response.status_code", status),
		attribute.String("graphdb.api.result", result),
	)
	if status >= 400 {
		span.SetStatus(codes.Error, http.StatusText(status))
	}
	span.End()
}

func startAPIPhase(ctx context.Context, phase string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	state, ok := apiTraceStateFromContext(ctx)
	if !ok {
		return ctx, nil
	}
	return otel.Tracer("graphdb/http").Start(ctx, state.prefix+"."+phase, trace.WithAttributes(attrs...))
}

func apiTracePrefix(ctx context.Context, fallback string) string {
	if state, ok := apiTraceStateFromContext(ctx); ok {
		return state.prefix
	}
	return fallback
}

func setAPITraceAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	if state, ok := apiTraceStateFromContext(ctx); ok && state.span != nil {
		state.span.SetAttributes(attrs...)
	}
}

func setAPITraceTenant(ctx context.Context, tenantID string) {
	if tenantID == "" {
		return
	}
	setAPITraceAttributes(ctx, attribute.String("graphdb.tenant", tenantID))
}

func apiTraceStateFromContext(ctx context.Context) (apiTraceState, bool) {
	state, ok := ctx.Value(apiTraceContextKey{}).(apiTraceState)
	return state, ok
}

func apiTraceRouteForRequest(r *http.Request) apiTraceRoute {
	method := r.Method
	path := r.URL.Path
	switch {
	case path == "/v1/health":
		return apiTraceRoute{"health", "GET /v1/health"}
	case path == "/v1/tenant-usage":
		return apiTraceRoute{"tenant_usage", "GET /v1/tenant-usage"}
	case path == "/v1/tenants":
		if method == http.MethodGet {
			return apiTraceRoute{"tenant.list", "GET /v1/tenants"}
		}
		if method == http.MethodPost {
			return apiTraceRoute{"tenant.create", "POST /v1/tenants"}
		}
	case strings.HasPrefix(path, "/v1/tenants/"):
		return tenantAPITraceRoute(method, path)
	case path == "/v1/ingest/batches":
		return apiTraceRoute{"ingest", "POST /v1/ingest/batches"}
	case strings.HasPrefix(path, "/v1/ingest/collectors/"):
		return apiTraceRoute{"collector_status", "GET /v1/ingest/collectors/{source}/{collector_id}"}
	case strings.HasPrefix(path, "/v1/ingest/deadletters/") && strings.HasSuffix(path, "/replay"):
		return apiTraceRoute{"deadletter.replay", "POST /v1/ingest/deadletters/{source}/replay"}
	case strings.HasPrefix(path, "/v1/ingest/deadletters/"):
		return apiTraceRoute{"deadletter.list", "GET /v1/ingest/deadletters/{source}"}
	case path == "/v1/entities":
		return apiTraceRoute{"entity.list", "GET /v1/entities"}
	case path == "/v1/entities/stream":
		return apiTraceRoute{"entity.stream", "GET /v1/entities/stream"}
	case strings.HasPrefix(path, "/v1/entities/"):
		return apiTraceRoute{"entity.get", "GET /v1/entities/{id}"}
	case path == "/v1/edges":
		return apiTraceRoute{"edge.list", "GET /v1/edges"}
	case path == "/v1/edges/stream":
		return apiTraceRoute{"edge.stream", "GET /v1/edges/stream"}
	case path == "/v1/export/snapshot":
		return apiTraceRoute{"snapshot.export", "GET /v1/export/snapshot"}
	case path == "/v1/export/snapshot/stream":
		return apiTraceRoute{"snapshot.export_stream", "GET /v1/export/snapshot/stream"}
	case path == "/v1/ci-types":
		return apiTraceRoute{"ci_type.list", "GET /v1/ci-types"}
	case path == "/v1/relation-types":
		return apiTraceRoute{"relation_type.list", "GET /v1/relation-types"}
	case path == "/v1/source-policy":
		if method == http.MethodGet {
			return apiTraceRoute{"source_policy.get", "GET /v1/source-policy"}
		}
		if method == http.MethodPut {
			return apiTraceRoute{"source_policy.update", "PUT /v1/source-policy"}
		}
	case path == "/v1/tenant-config":
		if method == http.MethodGet {
			return apiTraceRoute{"tenant_config.get", "GET /v1/tenant-config"}
		}
		if method == http.MethodPut {
			return apiTraceRoute{"tenant_config.update", "PUT /v1/tenant-config"}
		}
	case path == "/v1/query":
		return apiTraceRoute{"query.execute", "POST /v1/query"}
	case path == "/v1/query/stream":
		return apiTraceRoute{"query.stream", "POST /v1/query/stream"}
	case path == "/v1/query/gql":
		return apiTraceRoute{"query.gql", "POST /v1/query/gql"}
	case path == "/v1/query/gql/stream":
		return apiTraceRoute{"query.gql_stream", "POST /v1/query/gql/stream"}
	case path == "/v1/queries/running":
		return apiTraceRoute{"running_query.list", "GET /v1/queries/running"}
	case strings.HasPrefix(path, "/v1/queries/running/"):
		return apiTraceRoute{"running_query.kill", "DELETE /v1/queries/running/{id}"}
	case path == "/v1/query/templates":
		if method == http.MethodGet {
			return apiTraceRoute{"query_template.list", "GET /v1/query/templates"}
		}
		return apiTraceRoute{"query_template.save", "POST /v1/query/templates"}
	case strings.HasPrefix(path, "/v1/query/templates/"):
		return apiTraceRoute{"query_template.run", "POST /v1/query/templates/{name}/run"}
	case path == "/v1/tasks":
		if method == http.MethodGet {
			return apiTraceRoute{"task.list", "GET /v1/tasks"}
		}
		return apiTraceRoute{"task.start", "POST /v1/tasks"}
	case strings.HasPrefix(path, "/v1/tasks/"):
		return taskAPITraceRoute(method, path)
	case path == "/v1/indexes":
		if method == http.MethodGet {
			return apiTraceRoute{"index.catalog", "GET /v1/indexes"}
		}
		return apiTraceRoute{"index.create", "POST /v1/indexes"}
	case path == "/v1/indexes/definitions":
		return apiTraceRoute{"index.definition_list", "GET /v1/indexes/definitions"}
	case strings.HasPrefix(path, "/v1/indexes/definitions/"):
		return apiTraceRoute{"index.drop", "DELETE /v1/indexes/definitions/{name}"}
	case path == "/v1/indexes/health":
		return apiTraceRoute{"index.health", "GET /v1/indexes/health"}
	case strings.HasPrefix(path, "/v1/indexes/tasks/"):
		return apiTraceRoute{"index.task", "GET /v1/indexes/tasks/{task_id}"}
	case path == "/v1/indexes/rebuild":
		return apiTraceRoute{"index.rebuild", "POST /v1/indexes/rebuild"}
	case strings.HasPrefix(path, "/v1/control/"):
		return controlAPITraceRoute(method, path)
	case path == "/v1/compact":
		return apiTraceRoute{"compact", "POST /v1/compact"}
	}
	return apiTraceRoute{"api.request", traceRouteMethod(method) + " /v1/unmatched"}
}

func tenantAPITraceRoute(method string, path string) apiTraceRoute {
	switch {
	case strings.HasSuffix(path, "/disable"):
		return apiTraceRoute{"tenant.disable", "POST /v1/tenants/{tenant_id}/disable"}
	case strings.HasSuffix(path, "/enable"):
		return apiTraceRoute{"tenant.enable", "POST /v1/tenants/{tenant_id}/enable"}
	case strings.HasSuffix(path, "/purge"):
		return apiTraceRoute{"tenant.purge", "POST /v1/tenants/{tenant_id}/purge"}
	case strings.HasSuffix(path, "/clone"):
		return apiTraceRoute{"tenant.clone", "POST /v1/tenants/{tenant_id}/clone"}
	case strings.HasSuffix(path, "/backup"):
		return apiTraceRoute{"tenant.backup", "POST /v1/tenants/{tenant_id}/backup"}
	case strings.HasSuffix(path, "/restore"):
		return apiTraceRoute{"tenant.restore", "POST /v1/tenants/{tenant_id}/restore"}
	case strings.HasSuffix(path, "/restore-drill"):
		return apiTraceRoute{"tenant.restore_drill", "POST /v1/tenants/{tenant_id}/restore-drill"}
	}
	switch method {
	case http.MethodGet:
		return apiTraceRoute{"tenant.get", "GET /v1/tenants/{tenant_id}"}
	case http.MethodPut:
		return apiTraceRoute{"tenant.update", "PUT /v1/tenants/{tenant_id}"}
	case http.MethodDelete:
		return apiTraceRoute{"tenant.delete", "DELETE /v1/tenants/{tenant_id}"}
	default:
		return apiTraceRoute{"tenant.request", traceRouteMethod(method) + " /v1/tenants/{tenant_id}"}
	}
}

func taskAPITraceRoute(method string, path string) apiTraceRoute {
	if method == http.MethodPost && strings.HasSuffix(path, "/cancel") {
		return apiTraceRoute{"task.cancel", "POST /v1/tasks/{id}/cancel"}
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/retry") {
		return apiTraceRoute{"task.retry", "POST /v1/tasks/{id}/retry"}
	}
	return apiTraceRoute{"task.get", "GET /v1/tasks/{id}"}
}

func controlAPITraceRoute(method string, path string) apiTraceRoute {
	switch path {
	case "/v1/control/writer-lease":
		return apiTraceRoute{"control.writer_lease", "GET /v1/control/writer-lease"}
	case "/v1/control/reader-lag":
		return apiTraceRoute{"control.reader_lag", "GET /v1/control/reader-lag"}
	case "/v1/control/reader-freshness":
		return apiTraceRoute{"control.reader_freshness", "GET /v1/control/reader-freshness"}
	case "/v1/control/reader-fleet-readiness":
		return apiTraceRoute{"control.reader_fleet_readiness", "GET /v1/control/reader-fleet-readiness"}
	case "/v1/control/reader-traffic-gate":
		return apiTraceRoute{"control.reader_traffic_gate", "GET /v1/control/reader-traffic-gate"}
	case "/v1/control/integrity-audit":
		return apiTraceRoute{"control.integrity_audit", "GET /v1/control/integrity-audit"}
	case "/v1/control/profiling":
		return apiTraceRoute{"control.profiling", "POST /v1/control/profiling"}
	case "/v1/control/recover":
		return apiTraceRoute{"control.recover", "POST /v1/control/recover"}
	case "/v1/control/repair":
		return apiTraceRoute{"control.repair", "POST /v1/control/repair"}
	case "/v1/control/cleanup-commits":
		return apiTraceRoute{"control.cleanup_commits", "POST /v1/control/cleanup-commits"}
	case "/v1/control/gc":
		return apiTraceRoute{"control.gc", "POST /v1/control/gc"}
	}
	return apiTraceRoute{"control.request", traceRouteMethod(method) + " /v1/control/{operation}"}
}

func traceRouteMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}
