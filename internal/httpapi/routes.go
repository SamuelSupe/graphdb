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
	return s.observeHTTP(s.tenantLifecycleGate(mux))
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
	mux.HandleFunc("GET /v1/health", s.health)
	mux.HandleFunc("GET /v1/readiness", s.readiness)
	mux.HandleFunc("GET /openapi.yaml", s.openAPI)
}

func (s *Server) registerDataRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/commits", s.commit)
	mux.HandleFunc("POST /v1/ingest/batches", s.ingest)
	mux.HandleFunc("POST /v1/imports", s.startImport)
	mux.HandleFunc("GET /v1/entities", s.listEntities)
	mux.HandleFunc("GET /v1/entities/stream", s.streamEntities)
	mux.HandleFunc("GET /v1/entities/", s.entity)
	mux.HandleFunc("GET /v1/edges", s.listEdges)
	mux.HandleFunc("GET /v1/edges/stream", s.streamEdges)
	mux.HandleFunc("GET /v1/export/snapshot", s.exportSnapshot)
	mux.HandleFunc("GET /v1/export/snapshot/stream", s.streamSnapshot)
	mux.HandleFunc("GET /v1/ci-types", s.ciTypes)
	mux.HandleFunc("GET /v1/entity-types", s.entityTypes)
	mux.HandleFunc("GET /v1/relation-types", s.relationTypes)
	mux.HandleFunc("GET /v1/relation-schemas", s.relationSchemas)
	mux.HandleFunc("GET /v1/source-policy", s.getSourcePolicy)
	mux.HandleFunc("GET /v1/tenant-config", s.getTenantConfig)
	mux.HandleFunc("POST /v1/query", s.query)
	mux.HandleFunc("POST /v1/query/stream", s.queryStream)
	mux.HandleFunc("POST /v1/query/graphql", s.queryGraphQL)
	mux.HandleFunc("POST /v1/query/gql", s.queryGQL)
	mux.HandleFunc("POST /v1/query/gql/stream", s.queryGQLStream)
	mux.HandleFunc("GET /v1/query/templates", s.listQueryTemplates)
	mux.HandleFunc("POST /v1/query/templates/", s.runQueryTemplate)
}

func (s *Server) registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("/v1/tenants", s.tenantLifecycle)
	mux.HandleFunc("/v1/tenants/", s.tenantLifecycle)
	mux.HandleFunc("GET /v1/tenant-usage", s.tenantUsage)
	mux.HandleFunc("GET /v1/ingest/collectors/", s.collectorStatus)
	mux.HandleFunc("GET /v1/ingest/deadletters/", s.listDeadLetters)
	mux.HandleFunc("POST /v1/ingest/deadletters/", s.replayDeadLetters)
	mux.HandleFunc("PUT /v1/relation-schemas/", s.relationSchema)
	mux.HandleFunc("DELETE /v1/relation-schemas/", s.relationSchema)
	mux.HandleFunc("PUT /v1/source-policy", s.putSourcePolicy)
	mux.HandleFunc("PUT /v1/tenant-config", s.putTenantConfig)
	mux.HandleFunc("GET /v1/queries/running", s.listRunningQueries)
	mux.HandleFunc("DELETE /v1/queries/running/", s.killRunningQuery)
	mux.HandleFunc("POST /v1/query/templates", s.saveQueryTemplate)
	mux.HandleFunc("GET /v1/tasks", s.listTasks)
	mux.HandleFunc("POST /v1/tasks", s.startTask)
	mux.HandleFunc("GET /v1/tasks/", s.task)
	mux.HandleFunc("POST /v1/tasks/", s.taskAction)
	mux.HandleFunc("GET /v1/indexes", s.indexCatalog)
	mux.HandleFunc("POST /v1/indexes", s.createIndex)
	mux.HandleFunc("GET /v1/indexes/definitions", s.indexDefinitions)
	mux.HandleFunc("DELETE /v1/indexes/definitions/", s.dropIndex)
	mux.HandleFunc("GET /v1/indexes/health", s.indexHealth)
	mux.HandleFunc("GET /v1/indexes/tasks/", s.indexTask)
	mux.HandleFunc("POST /v1/indexes/rebuild", s.rebuildIndexes)
	mux.HandleFunc("GET /v1/control/writer-lease", s.writerLease)
	mux.HandleFunc("GET /v1/control/reader-lag", s.readerLag)
	mux.HandleFunc("GET /v1/control/reader-freshness", s.readerLag)
	mux.HandleFunc("GET /v1/control/reader-fleet-readiness", s.readerFleetReadiness)
	mux.HandleFunc("GET /v1/control/reader-traffic-gate", s.readerTrafficGate)
	mux.HandleFunc("GET /v1/control/integrity-audit", s.integrityAudit)
	mux.HandleFunc("POST /v1/control/recover", s.recoverTenant)
	mux.HandleFunc("POST /v1/control/repair", s.repairTenant)
	mux.HandleFunc("POST /v1/control/cleanup-commits", s.cleanupCommits)
	mux.HandleFunc("POST /v1/control/gc", s.runGC)
	mux.HandleFunc("POST /v1/compact", s.compact)
}

func registerPprofRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}
