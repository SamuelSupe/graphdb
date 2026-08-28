package httpapi

import (
	"io"
	"net/http"
	"net/http/pprof"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
)

// Handler preserves the 1.0 single-listener API surface, with pprof disabled.
func (s *Server) Handler() http.Handler {
	return s.handler(true, true, false)
}

// DataHandler exposes graph reads, writes, and schema reads.
func (s *Server) DataHandler() http.Handler {
	return s.handler(true, false, false)
}

// AdminHandler exposes lifecycle, configuration, maintenance, and metrics.
func (s *Server) AdminHandler(enablePprof bool) http.Handler {
	return s.handler(false, true, enablePprof)
}

func (s *Server) handler(data, admin, enablePprof bool) http.Handler {
	s.prepareHandler()
	mux := http.NewServeMux()
	s.registerCommonRoutes(mux)
	if data {
		s.registerDataRoutes(mux)
	}
	if admin {
		s.registerAdminRoutes(mux)
	}
	if enablePprof {
		registerPprofRoutes(mux)
	}
	return s.observeHTTP(mux)
}

type routeSpec struct {
	pattern               string
	handler               http.HandlerFunc
	mutation              bool
	bypassTenantLifecycle bool
}

func (s *Server) registerRoutes(mux *http.ServeMux, routes ...routeSpec) {
	for _, route := range routes {
		handler := http.Handler(route.handler)
		if !route.bypassTenantLifecycle {
			handler = s.tenantLifecycleGate(route.mutation, handler)
		}
		mux.Handle(route.pattern, handler)
	}
}

func (s *Server) prepareHandler() {
	s.maintenanceRuntime()
	if s.QueryRegistry == nil {
		s.QueryRegistry = NewRunningQueryRegistry()
	}
	if s.Observability == nil {
		s.Observability = observability.New(io.Discard, 500*time.Millisecond)
	}
	if s.usageCache == nil {
		s.usageCache = newTenantUsageCache(s.tenantUsageCacheTTL())
	}
}

func (s *Server) registerCommonRoutes(mux *http.ServeMux) {
	s.registerRoutes(mux,
		routeSpec{pattern: "GET /v1/health", handler: s.health},
		routeSpec{pattern: "GET /v1/readiness", handler: s.readiness},
		routeSpec{pattern: "GET /openapi.yaml", handler: s.openAPI},
	)
}

func (s *Server) registerDataRoutes(mux *http.ServeMux) {
	s.registerRoutes(mux,
		routeSpec{pattern: "POST /v1/commits", handler: s.commit, mutation: true},
		routeSpec{pattern: "POST /v1/ingest/batches", handler: s.ingest, mutation: true},
		routeSpec{pattern: "GET /v1/ingest/batches/", handler: s.ingestBatchStatus},
		routeSpec{pattern: "POST /v1/imports", handler: s.startImport, mutation: true},
		routeSpec{pattern: "GET /v1/entities", handler: s.listEntities},
		routeSpec{pattern: "GET /v1/entities/stream", handler: s.streamEntities},
		routeSpec{pattern: "GET /v1/entities/", handler: s.entity},
		routeSpec{pattern: "GET /v1/edges", handler: s.listEdges},
		routeSpec{pattern: "GET /v1/edges/stream", handler: s.streamEdges},
		routeSpec{pattern: "GET /v1/export/snapshot", handler: s.exportSnapshot},
		routeSpec{pattern: "GET /v1/export/snapshot/stream", handler: s.streamSnapshot},
		routeSpec{pattern: "GET /v1/ci-types", handler: s.ciTypes},
		routeSpec{pattern: "GET /v1/entity-types", handler: s.entityTypes},
		routeSpec{pattern: "GET /v1/relation-types", handler: s.relationTypes},
		routeSpec{pattern: "GET /v1/relation-schemas", handler: s.relationSchemas},
		routeSpec{pattern: "GET /v1/source-policy", handler: s.getSourcePolicy},
		routeSpec{pattern: "GET /v1/tenant-config", handler: s.getTenantConfig},
		routeSpec{pattern: "POST /v1/query", handler: s.query},
		routeSpec{pattern: "POST /v1/query/stream", handler: s.queryStream},
		routeSpec{pattern: "POST /v1/query/graphql", handler: s.queryGraphQL},
		routeSpec{pattern: "POST /v1/query/gql", handler: s.queryGQL},
		routeSpec{pattern: "POST /v1/query/gql/stream", handler: s.queryGQLStream},
		routeSpec{pattern: "GET /v1/query/templates", handler: s.listQueryTemplates},
		routeSpec{pattern: "POST /v1/query/templates/", handler: s.runQueryTemplate},
	)
}

func (s *Server) registerAdminRoutes(mux *http.ServeMux) {
	s.registerRoutes(mux,
		routeSpec{pattern: "GET /metrics", handler: s.metrics},
		routeSpec{pattern: "/v1/tenants", handler: s.tenantLifecycle, bypassTenantLifecycle: true},
		routeSpec{pattern: "/v1/tenants/", handler: s.tenantLifecycle, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/tenant-usage", handler: s.tenantUsage},
		routeSpec{pattern: "GET /v1/ingest/collectors/", handler: s.collectorStatus},
		routeSpec{pattern: "GET /v1/ingest/deadletters/", handler: s.listDeadLetters},
		routeSpec{pattern: "POST /v1/ingest/deadletters/", handler: s.replayDeadLetters, mutation: true},
		routeSpec{pattern: "PUT /v1/relation-schemas/", handler: s.relationSchema, mutation: true},
		routeSpec{pattern: "DELETE /v1/relation-schemas/", handler: s.relationSchema, mutation: true},
		routeSpec{pattern: "PUT /v1/source-policy", handler: s.putSourcePolicy, mutation: true},
		routeSpec{pattern: "PUT /v1/tenant-config", handler: s.putTenantConfig, mutation: true},
		routeSpec{pattern: "GET /v1/queries/running", handler: s.listRunningQueries},
		routeSpec{pattern: "DELETE /v1/queries/running/", handler: s.killRunningQuery},
		routeSpec{pattern: "POST /v1/query/templates", handler: s.saveQueryTemplate, mutation: true},
		routeSpec{pattern: "GET /v1/tasks", handler: s.listTasks},
		routeSpec{pattern: "POST /v1/tasks", handler: s.startTask, mutation: true},
		routeSpec{pattern: "GET /v1/tasks/", handler: s.task},
		routeSpec{pattern: "POST /v1/tasks/", handler: s.taskAction, mutation: true},
		routeSpec{pattern: "GET /v1/indexes", handler: s.indexCatalog},
		routeSpec{pattern: "POST /v1/indexes", handler: s.createIndex, mutation: true},
		routeSpec{pattern: "GET /v1/indexes/definitions", handler: s.indexDefinitions},
		routeSpec{pattern: "DELETE /v1/indexes/definitions/", handler: s.dropIndex, mutation: true},
		routeSpec{pattern: "GET /v1/indexes/health", handler: s.indexHealth},
		routeSpec{pattern: "GET /v1/indexes/tasks/", handler: s.indexTask},
		routeSpec{pattern: "POST /v1/indexes/rebuild", handler: s.rebuildIndexes, mutation: true},
		routeSpec{pattern: "GET /v1/control/writer-lease", handler: s.writerLease},
		routeSpec{pattern: "GET /v1/control/reader-lag", handler: s.readerLag},
		routeSpec{pattern: "GET /v1/control/reader-freshness", handler: s.readerLag},
		routeSpec{pattern: "GET /v1/control/reader-fleet-readiness", handler: s.readerFleetReadiness},
		routeSpec{pattern: "GET /v1/control/reader-traffic-gate", handler: s.readerTrafficGate},
		routeSpec{pattern: "GET /v1/control/integrity-audit", handler: s.integrityAudit},
		routeSpec{pattern: "POST /v1/control/recover", handler: s.recoverTenant, mutation: true},
		routeSpec{pattern: "POST /v1/control/repair", handler: s.repairTenant, mutation: true},
		routeSpec{pattern: "POST /v1/control/cleanup-commits", handler: s.cleanupCommits, mutation: true},
		routeSpec{pattern: "POST /v1/control/gc", handler: s.runGC, mutation: true},
		routeSpec{pattern: "POST /v1/compact", handler: s.compact, mutation: true},
	)
}

func registerPprofRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}
