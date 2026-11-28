package httpapi

import (
	"context"
	"crypto/subtle"
	"io"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/adminui"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Handler preserves the 1.0 single-listener API surface, with pprof disabled.
func (s *Server) Handler() http.Handler {
	return s.handler(true, true, false, true)
}

// InternalHandler serves the full API surface without external bearer-token
// authentication. It is exclusively for Raft state-machine replay and private
// /cluster/data/ requests already authenticated by the Raft cluster token.
func (s *Server) InternalHandler() http.Handler {
	return s.handler(true, true, false, false)
}

// DataHandler exposes graph reads, writes, and schema reads.
func (s *Server) DataHandler() http.Handler {
	return s.handler(true, false, false, true)
}

// AdminHandler exposes lifecycle, configuration, maintenance, and metrics.
func (s *Server) AdminHandler(enablePprof bool) http.Handler {
	return s.handler(false, true, enablePprof, true)
}

func (s *Server) handler(data, admin, enablePprof, enforceAuth bool) http.Handler {
	s.prepareHandler()
	mux := http.NewServeMux()
	authRequired := enforceAuth && (s.AuthToken != "" || s.AdminToken != "" || s.RequireTenantScope)
	s.registerCommonRoutes(mux)
	if data {
		s.registerDataRoutes(mux, authRequired, false)
	}
	if admin {
		adminui.Register(mux)
		s.registerAdminRoutes(mux, authRequired)
		if !data {
			s.registerRoutes(mux, authRequired, true, routeSpec{pattern: "GET /v1/query/templates", handler: s.listQueryTemplates, adminOnly: true})
		}
	}
	if enablePprof {
		s.registerPprofRoutes(mux, authRequired)
	}
	return s.observeHTTP(mux)
}

func (s *Server) routeAuthMiddleware(adminOnly bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if storage.IsReplicatedContext(r.Context()) {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		token := ""
		if strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		}
		adminMatch := s.AdminToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.AdminToken)) == 1
		authMatch := s.AuthToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.AuthToken)) == 1
		if adminOnly && s.AdminToken != "" {
			if !adminMatch {
				w.Header().Set("WWW-Authenticate", `Bearer realm="graphdb-admin"`)
				writeError(w, http.StatusUnauthorized, "valid admin bearer token is required")
				return
			}
		} else if s.AuthToken != "" || s.AdminToken != "" {
			if !authMatch && !adminMatch {
				w.Header().Set("WWW-Authenticate", `Bearer realm="graphdb"`)
				writeError(w, http.StatusUnauthorized, "valid bearer token is required")
				return
			}
		}
		if adminMatch {
			r = r.WithContext(storage.WithAdminAuthorized(r.Context()))
		}
		next.ServeHTTP(w, r)
	})
}

func isAdminAuthRequest(ctx context.Context) bool {
	return storage.IsAdminAuthorized(ctx)
}

type routeSpec struct {
	pattern               string
	handler               http.HandlerFunc
	mutation              bool
	runtimeOnly           bool
	readerActive          bool
	bypassTenantLifecycle bool
	skipReadView          bool
	cancelableReadView    bool
	adminOnly             bool
}

func (s *Server) registerRoutes(mux *http.ServeMux, enforceAuth, defaultAdmin bool, routes ...routeSpec) {
	for _, route := range routes {
		handler := http.Handler(route.handler)
		if !route.mutation && !route.runtimeOnly && !route.skipReadView {
			handler = s.pinLocalReadView(route.cancelableReadView, handler)
		}
		if !route.bypassTenantLifecycle {
			handler = s.tenantLifecycleGate(route.mutation, handler)
		}
		if s.Cluster != nil {
			next := handler
			handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.Cluster.ServeRoute(w, r, route.mutation, route.runtimeOnly, next)
			})
		}
		if route.mutation && (s.writeAllowed() || route.runtimeOnly || route.readerActive) {
			handler = ProtectBrowserWrite(handler)
		}
		if enforceAuth {
			handler = s.routeAuthMiddleware(route.adminOnly || defaultAdmin, handler)
		}
		mux.Handle(route.pattern, handler)
	}
}

func (s *Server) prepareHandler() {
	s.prepareOnce.Do(func() {
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
	})
}

func (s *Server) registerCommonRoutes(mux *http.ServeMux) {
	s.registerRoutes(mux, false, false,
		routeSpec{pattern: "GET /v1/health", handler: s.health, bypassTenantLifecycle: true, skipReadView: true},
		routeSpec{pattern: "GET /v1/readiness", handler: s.readiness, bypassTenantLifecycle: true, skipReadView: true},
		routeSpec{pattern: "GET /openapi.yaml", handler: s.openAPI, bypassTenantLifecycle: true, skipReadView: true},
	)
}

func (s *Server) registerDataRoutes(mux *http.ServeMux, enforceAuth, defaultAdmin bool) {
	s.registerRoutes(mux, enforceAuth, defaultAdmin,
		routeSpec{pattern: "POST /v1/commits", handler: s.commit, mutation: true},
		routeSpec{pattern: "POST /v1/ingest/batches", handler: s.ingest, mutation: true, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/ingest/batches/", handler: s.ingestBatchStatus, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/ingest/writers/", handler: s.ingestBatchStatus, bypassTenantLifecycle: true},
		routeSpec{pattern: "POST /v1/imports", handler: s.startImport, mutation: true},
		routeSpec{pattern: "GET /v1/entities", handler: s.listEntities},
		routeSpec{pattern: "GET /v1/entities/stream", handler: s.streamEntities, cancelableReadView: true},
		routeSpec{pattern: "GET /v1/entities/", handler: s.entity},
		routeSpec{pattern: "GET /v1/edges", handler: s.listEdges},
		routeSpec{pattern: "GET /v1/edges/stream", handler: s.streamEdges, cancelableReadView: true},
		routeSpec{pattern: "GET /v1/export/snapshot", handler: s.exportSnapshot, cancelableReadView: true},
		routeSpec{pattern: "GET /v1/export/snapshot/stream", handler: s.streamSnapshot, cancelableReadView: true},
		routeSpec{pattern: "GET /v1/ci-types", handler: s.ciTypes},
		routeSpec{pattern: "GET /v1/entity-types", handler: s.entityTypes},
		routeSpec{pattern: "GET /v1/relation-types", handler: s.relationTypes},
		routeSpec{pattern: "GET /v1/relation-schemas", handler: s.relationSchemas},
		routeSpec{pattern: "GET /v1/source-policy", handler: s.getSourcePolicy},
		routeSpec{pattern: "GET /v1/tenant-config", handler: s.getTenantConfig},
		routeSpec{pattern: "GET /v1/backup-automation", handler: s.getBackupAutomation},
		routeSpec{pattern: "POST /v1/query", handler: s.query},
		routeSpec{pattern: "POST /v1/query/stream", handler: s.queryStream, cancelableReadView: true},
		routeSpec{pattern: "POST /v1/query/graphql", handler: s.queryGraphQL},
		routeSpec{pattern: "POST /v1/query/gql", handler: s.queryGQL},
		routeSpec{pattern: "POST /v1/query/gql/stream", handler: s.queryGQLStream, cancelableReadView: true},
		routeSpec{pattern: "GET /v1/query/templates", handler: s.listQueryTemplates},
		routeSpec{pattern: "POST /v1/query/templates/", handler: s.runQueryTemplate},
	)
}

func (s *Server) registerAdminRoutes(mux *http.ServeMux, enforceAuth bool) {
	s.registerRoutes(mux, enforceAuth, true,
		routeSpec{pattern: "POST /v1/backup-automation/reset", handler: s.resetBackupAutomation, mutation: true},
		routeSpec{pattern: "GET /metrics", handler: s.metrics, runtimeOnly: true, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/diagnostics", handler: s.diagnostics, runtimeOnly: true, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/tenants", handler: s.tenantLifecycle, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/tenants/", handler: s.tenantLifecycle, bypassTenantLifecycle: true},
		routeSpec{pattern: "/v1/tenants", handler: s.tenantLifecycle, mutation: true, bypassTenantLifecycle: true},
		routeSpec{pattern: "/v1/tenants/", handler: s.tenantLifecycle, mutation: true, bypassTenantLifecycle: true},
		routeSpec{pattern: "GET /v1/tenant-usage", handler: s.tenantUsage},
		routeSpec{pattern: "GET /v1/ingest/collectors/", handler: s.collectorStatus},
		routeSpec{pattern: "GET /v1/ingest/deadletters/", handler: s.listDeadLetters},
		routeSpec{pattern: "POST /v1/ingest/deadletters/", handler: s.replayDeadLetters, mutation: true},
		routeSpec{pattern: "PUT /v1/relation-schemas/", handler: s.relationSchema, mutation: true},
		routeSpec{pattern: "DELETE /v1/relation-schemas/", handler: s.relationSchema, mutation: true},
		routeSpec{pattern: "PUT /v1/source-policy", handler: s.putSourcePolicy, mutation: true},
		routeSpec{pattern: "PUT /v1/tenant-config", handler: s.putTenantConfig, mutation: true},
		routeSpec{pattern: "GET /v1/queries/running", handler: s.listRunningQueries, runtimeOnly: true, bypassTenantLifecycle: true},
		routeSpec{pattern: "DELETE /v1/queries/running/", handler: s.killRunningQuery, mutation: true, runtimeOnly: true, bypassTenantLifecycle: true},
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
		routeSpec{pattern: "POST /v1/control/repair", handler: s.repairTenant, mutation: true, readerActive: true},
		routeSpec{pattern: "POST /v1/control/cleanup-commits", handler: s.cleanupCommits, mutation: true},
		routeSpec{pattern: "POST /v1/control/gc", handler: s.runGC, mutation: true},
		routeSpec{pattern: "POST /v1/compact", handler: s.compact, mutation: true},
	)
}

func (s *Server) registerPprofRoutes(mux *http.ServeMux, enforceAuth bool) {
	wrap := func(fn http.HandlerFunc) http.Handler {
		if enforceAuth {
			return s.routeAuthMiddleware(true, fn)
		}
		return fn
	}
	mux.Handle("GET /debug/pprof/", wrap(pprof.Index))
	mux.Handle("GET /debug/pprof/cmdline", wrap(pprof.Cmdline))
	mux.Handle("GET /debug/pprof/profile", wrap(pprof.Profile))
	mux.Handle("GET /debug/pprof/symbol", wrap(pprof.Symbol))
	mux.Handle("POST /debug/pprof/symbol", wrap(pprof.Symbol))
	mux.Handle("GET /debug/pprof/trace", wrap(pprof.Trace))
}

func (s *Server) pinLocalReadView(cancelable bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Store == nil {
			next.ServeHTTP(w, r)
			return
		}
		var (
			ctx     context.Context
			release func()
			err     error
		)
		if cancelable {
			ctx, release, err = s.Store.ReadViewContextCancelable(r.Context(), r.Header.Get("X-Tenant-ID"))
		} else {
			ctx, release, err = s.Store.ReadViewContext(r.Context(), r.Header.Get("X-Tenant-ID"))
		}
		if err != nil {
			writeStorageError(w, err)
			return
		}
		defer release()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
