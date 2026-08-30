package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/buildinfo"
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Server struct {
	Store                 *storage.TenantStore
	Cache                 *storage.ReaderCache
	Mode                  string
	Admission             *QueryAdmission
	ReadAdmission         *QueryAdmission
	WriteAdmission        *WriteAdmission
	WriteExecutionTimeout time.Duration
	ReaderCatchupTimeout  time.Duration
	ReadinessTimeout      time.Duration
	QueryRegistry         *RunningQueryRegistry
	RetrievalSearcher     retrieval.Searcher
	IngestService         IngestService
	Observability         *observability.Observability
	UsageCacheTTL         time.Duration
	maintenance           *maintenanceState
	maintenanceOnce       sync.Once
	usageCache            *tenantUsageCache
	lazyUnavailable       sync.Map
}

type IngestService interface {
	Accept(context.Context, string, storage.IngestRequest) (storage.IngestAcceptance, error)
	Wait(context.Context, storage.IngestAcceptance) (storage.IngestResult, error)
	Status(context.Context, string, string, string, string) (storage.IngestBatchStatus, error)
	Readiness() storage.IngestServiceReadiness
	ObserveMetrics()
}

type CommitRequest struct {
	ExpectedVersion *int64          `json:"expected_version,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Mutations       graph.Mutations `json:"mutations"`
}

func (s *Server) tenantUsageCacheTTL() time.Duration {
	if s.UsageCacheTTL != 0 {
		return s.UsageCacheTTL
	}
	return defaultTenantUsageCacheTTL
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	coordinator := s.Store.CachedCoordinatorStatus()
	status := "ok"
	if !coordinator.Available {
		status = "degraded"
	}
	response := map[string]any{
		"status": status, "mode": s.Mode, "coordination": coordinator, "build": buildinfo.Current(),
	}
	if s.IngestService != nil {
		response["ingest_wal"] = s.IngestService.Readiness()
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	timeout := s.ReadinessTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	coordinator, objectStore := s.readinessDependencies(ctx)
	status := "ready"
	code := http.StatusOK
	if !coordinator.Available || !objectStore.Available {
		status = "not_ready"
		code = http.StatusServiceUnavailable
	}
	response := map[string]any{
		"status": status, "mode": s.Mode, "coordination": coordinator,
		"object_store": objectStore, "build": buildinfo.Current(),
	}
	if s.IngestService != nil {
		walStatus := s.IngestService.Readiness()
		response["ingest_wal"] = walStatus
		if !walStatus.Ready {
			response["status"] = "not_ready"
			code = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, response)
}

func (s *Server) readinessDependencies(ctx context.Context) (storage.CoordinatorStatus, storage.ObjectStoreStatus) {
	coordinatorResult := make(chan storage.CoordinatorStatus, 1)
	objectStoreResult := make(chan storage.ObjectStoreStatus, 1)
	go func() {
		coordinatorResult <- s.Store.CoordinatorStatus(ctx)
	}()
	go func() {
		objectStoreResult <- s.Store.ObjectStoreStatus(ctx)
	}()
	coordinator := storage.CoordinatorStatus{}
	select {
	case coordinator = <-coordinatorResult:
	case <-ctx.Done():
		coordinator = storage.CoordinatorStatus{
			Backend:   s.Store.CoordinationBackend(),
			Available: false,
			CheckedAt: time.Now().UTC(),
			LastError: ctx.Err().Error(),
		}
	}
	objectStore := storage.ObjectStoreStatus{}
	select {
	case objectStore = <-objectStoreResult:
	case <-ctx.Done():
		objectStore = storage.ObjectStoreStatus{
			Available: false,
			CheckedAt: time.Now().UTC(),
			LastError: ctx.Err().Error(),
		}
	}
	return coordinator, objectStore
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if s.IngestService != nil {
		s.IngestService.ObserveMetrics()
	}
	status := s.Store.CachedCoordinatorStatus()
	s.obs().Metrics.RecordCoordinatorStatus(
		status.Backend,
		status.Available,
		status.MaxMirrorLag,
		status.OutboxBacklog,
		status.DerivedBacklog,
	)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write(s.obs().Metrics.SnapshotPrometheus())
}

func (s *Server) commit(w http.ResponseWriter, r *http.Request) {
	tracer := otel.Tracer("graphdb/http")
	ctx, span := tracer.Start(r.Context(), "graphdb.commit.http")
	var spanErr error
	defer func() { endHTTPSpan(span, spanErr) }()
	r = r.WithContext(ctx)

	if !s.writeAllowed() {
		span.SetAttributes(attribute.String("graphdb.commit.result", "write_disabled"))
		writeError(w, http.StatusMethodNotAllowed, "writes are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		span.SetAttributes(attribute.String("graphdb.commit.result", "tenant_required"))
		return
	}
	span.SetAttributes(attribute.String("graphdb.tenant", tenantID))

	enterCtx, enterSpan := tracer.Start(r.Context(), "graphdb.commit.enter_write", trace.WithAttributes(attribute.String("graphdb.tenant", tenantID)))
	release, ok := s.enterWrite(w, r.WithContext(enterCtx), tenantID)
	if !ok {
		span.SetAttributes(attribute.String("graphdb.commit.result", "write_admission_rejected"))
		endHTTPSpan(enterSpan, traceError("write admission rejected"))
		return
	}
	endHTTPSpan(enterSpan, nil)
	defer release()

	var request CommitRequest
	_, decodeSpan := tracer.Start(r.Context(), "graphdb.commit.decode_request", trace.WithAttributes(attribute.String("graphdb.tenant", tenantID)))
	if !decodeJSONBody(w, r, &request, maxWriteRequestBytes) {
		span.SetAttributes(attribute.String("graphdb.commit.result", "decode_failed"))
		endHTTPSpan(decodeSpan, traceError("decode commit request failed"))
		return
	}
	decodeSpan.SetAttributes(commitRequestAttributes(request)...)
	endHTTPSpan(decodeSpan, nil)

	writeCtx, cancel := s.writeExecutionContext(r.Context())
	defer cancel()

	storeCtx, storeSpan := tracer.Start(writeCtx, "graphdb.commit.store_commit", trace.WithAttributes(append([]attribute.KeyValue{
		attribute.String("graphdb.tenant", tenantID),
	}, commitRequestAttributes(request)...)...))
	result, err := s.Store.CommitWithReport(storeCtx, tenantID, request.Mutations, storage.CommitOptions{
		ExpectedVersion:          request.ExpectedVersion,
		IdempotencyKey:           request.IdempotencyKey,
		WriteBackpressureChecked: true,
	})
	storeSpan.SetAttributes(
		attribute.Int64("graphdb.commit.version", result.Version),
		attribute.Int64("graphdb.commit.readable_version", result.ReadableVersion),
		attribute.Bool("graphdb.commit.skipped", result.Skipped),
		attribute.Bool("graphdb.commit.idempotent_replay", result.IdempotentReplay),
		attribute.Int("graphdb.commit.index_warnings", len(result.IndexWarnings)),
	)
	endHTTPSpan(storeSpan, err)
	if err != nil {
		spanErr = err
		if s.writeBackpressureIfNeeded(w, tenantID, err) {
			span.SetAttributes(attribute.String("graphdb.commit.result", "write_backpressure"))
			return
		}
		if writeCtx.Err() != nil {
			if commitMayHaveChangedData(result) {
				s.invalidate(tenantID)
			}
			s.auditError("commit_timeout", tenantID, err, map[string]any{"idempotency_key": request.IdempotencyKey})
			span.SetAttributes(attribute.String("graphdb.commit.result", "write_context_error"))
			writeRequestError(w, writeCtx.Err())
			return
		}
		if commitMayHaveChangedData(result) {
			s.invalidate(tenantID)
			s.auditError("commit_metadata_failed", tenantID, err, map[string]any{"idempotency_key": request.IdempotencyKey})
			span.SetAttributes(attribute.String("graphdb.commit.result", "metadata_failed"))
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.auditError("commit_failed", tenantID, err, map[string]any{})
		span.SetAttributes(attribute.String("graphdb.commit.result", "failed"))
		writeStorageError(w, err)
		return
	}

	_, afterSpan := tracer.Start(r.Context(), "graphdb.commit.after_commit", trace.WithAttributes(
		attribute.String("graphdb.tenant", tenantID),
		attribute.Int64("graphdb.commit.version", result.Version),
		attribute.Int("graphdb.commit.suppressed", len(result.Suppressed)),
		attribute.Int("graphdb.commit.canonical_entities", len(result.CanonicalEntities)),
		attribute.Int("graphdb.commit.canonical_edges", len(result.CanonicalEdges)),
	))
	s.publishReadCacheAfterWrite(tenantID)
	s.recordSuppressed(tenantID, result.Suppressed)
	s.auditInfo("commit_applied", tenantID, map[string]any{
		"version": result.Version, "suppressed": len(result.Suppressed), "canonical_entities": len(result.CanonicalEntities), "canonical_edges": len(result.CanonicalEdges),
	})
	endHTTPSpan(afterSpan, nil)
	span.SetAttributes(
		attribute.String("graphdb.commit.result", "ok"),
		attribute.Int64("graphdb.commit.version", result.Version),
		attribute.Int("graphdb.commit.index_warnings", len(result.IndexWarnings)),
	)
	writeJSON(w, http.StatusOK, result)
}

func commitMayHaveChangedData(result storage.CommitResult) bool {
	return result.Version > 0 || result.ReadAfterCommitID != "" || result.DataMD5 != ""
}

func (s *Server) entity(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	id, err := escapedPathTail(r, "/v1/entities/")
	if err != nil || id == "" {
		writeError(w, http.StatusBadRequest, "entity id is required")
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	if target.ManifestVersion > 0 {
		options, version, ok := s.lazyQueryOptions(
			r.Context(), tenantID, target.ManifestVersion, false,
		)
		if ok && target.requiresVersion(version) && options.EntityLookup != nil {
			entity, ok, err := options.EntityLookup.GetEntity(r.Context(), id, nil)
			if err != nil {
				writeReadError(w, err)
				return
			}
			if ok {
				s.recordReaderVisible(tenantID, version)
				writeJSON(w, http.StatusOK, map[string]any{"version": version, "entity": entity})
				return
			}
		}
	}
	var entity graph.Entity
	var found bool
	var version int64
	err = s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		entity, found = g.GetEntityByReference(id)
		version = manifest.Version
		return nil
	})
	if err != nil {
		writeReadError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "entity not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "entity": entity})
}

func escapedPathTail(r *http.Request, prefix string) (string, error) {
	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix) {
		return "", nil
	}
	return url.PathUnescape(strings.TrimPrefix(escaped, prefix))
}

func escapedPathParts(r *http.Request, prefix string, count int) ([]string, error) {
	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix) {
		return nil, nil
	}
	raw := strings.TrimPrefix(escaped, prefix)
	parts := strings.Split(raw, "/")
	if len(parts) != count {
		return nil, nil
	}
	decoded := make([]string, 0, len(parts))
	for _, part := range parts {
		value, err := url.PathUnescape(part)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, value)
	}
	return decoded, nil
}

func (s *Server) ciTypes(w http.ResponseWriter, r *http.Request) {
	s.listEntityTypes(w, r, "ci_types")
}

func (s *Server) relationTypes(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	var items []graph.RelationType
	var version int64
	err = s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		items = g.ListRelationTypes()
		version = manifest.Version
		return nil
	})
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "relation_types": items})
}

func (s *Server) compact(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "compaction is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterMaintenance(w, tenantID)
	if !ok {
		return
	}
	defer release()
	manifest, err := s.Store.Compact(r.Context(), tenantID)
	if err != nil {
		s.auditError("compact_failed", tenantID, err, map[string]any{})
		writeStorageError(w, err)
		return
	}
	s.auditInfo("compact_applied", tenantID, map[string]any{"version": manifest.Version})
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) writeAllowed() bool {
	return s.Mode == "" || s.Mode == "all" || s.Mode == "writer"
}

func (s *Server) loadGraph(ctx context.Context, tenantID string) (*graph.Graph, storage.Manifest, error) {
	return s.loadGraphAtLeast(ctx, tenantID, 0)
}

func (s *Server) loadGraphAtLeast(ctx context.Context, tenantID string, minVersion int64) (*graph.Graph, storage.Manifest, error) {
	if s.Cache != nil {
		if minVersion > 0 {
			return s.Cache.LoadAtLeast(ctx, tenantID, minVersion)
		}
		return s.Cache.Load(ctx, tenantID)
	}
	return s.Store.LoadAtLeast(ctx, tenantID, minVersion)
}

func (s *Server) invalidate(tenantID string) {
	if s.Cache != nil {
		s.Cache.Invalidate(tenantID)
	}
}

func (s *Server) publishReadCacheAfterWrite(tenantID string) {
	if s.Cache != nil && s.Cache.PublishFromWriteCache(tenantID) {
		return
	}
	s.invalidate(tenantID)
}

func (s *Server) obs() *observability.Observability {
	if s.Observability == nil {
		s.Observability = observability.New(io.Discard, 500*time.Millisecond)
	}
	return s.Observability
}

func (s *Server) observeHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obs := s.obs()
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		tenantID := r.Header.Get("X-Tenant-ID")
		obs.RegisterTenant(tenantID)
		tracer := otel.Tracer("graphdb/http")
		ctx, span := tracer.Start(ctx, "HTTP request")
		defer span.End()
		r = r.WithContext(ctx)
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		var apiSpan trace.Span
		var apiTraced bool
		r, apiSpan, apiTraced = startAPIRequestTrace(r)
		if apiTraced {
			defer func() { endAPIRequestTrace(apiSpan, recorder.status) }()
		}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		duration := time.Since(start)
		route := observedRoute(r)
		span.SetName(route)
		obs.Metrics.RecordHTTPRequest(r.Method, route, recorder.status, duration)
		span.SetAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", recorder.status),
			attribute.String("graphdb.tenant", tenantID),
		)
		if recorder.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(recorder.status))
		}
		obs.Logger.Info("http_request", map[string]any{
			"tenant": tenantID, "method": r.Method, "path": r.URL.Path, "route": route,
			"status": recorder.status, "duration_ms": float64(duration.Microseconds()) / 1000,
		})
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func observedRoute(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	path := r.URL.Path
	switch {
	case path == "/v1/entities":
		return "GET /v1/entities"
	case path == "/v1/tenant-usage":
		return "GET /v1/tenant-usage"
	case path == "/v1/tenants":
		return r.Method + " /v1/tenants"
	case strings.HasPrefix(path, "/v1/tenants/"):
		return r.Method + " /v1/tenants/{tenant_id}"
	case path == "/v1/entities/stream":
		return "GET /v1/entities/stream"
	case strings.HasPrefix(path, "/v1/entities/"):
		return "GET /v1/entities/{id}"
	case path == "/v1/edges":
		return "GET /v1/edges"
	case path == "/v1/edges/stream":
		return "GET /v1/edges/stream"
	case path == "/v1/export/snapshot":
		return "GET /v1/export/snapshot"
	case path == "/v1/export/snapshot/stream":
		return "GET /v1/export/snapshot/stream"
	case strings.HasPrefix(path, "/v1/ingest/collectors/"):
		return "GET /v1/ingest/collectors/{source}/{collector_id}"
	case strings.HasPrefix(path, "/v1/ingest/deadletters/") && strings.HasSuffix(path, "/replay"):
		return "POST /v1/ingest/deadletters/{source}/replay"
	case strings.HasPrefix(path, "/v1/ingest/deadletters/"):
		return "GET /v1/ingest/deadletters/{source}"
	case path == "/v1/imports":
		return "POST /v1/imports"
	case strings.HasPrefix(path, "/v1/query/templates/"):
		return "POST /v1/query/templates/{name}/run"
	case path == "/v1/queries/running":
		return "GET /v1/queries/running"
	case strings.HasPrefix(path, "/v1/queries/running/"):
		return "DELETE /v1/queries/running/{id}"
	case strings.HasPrefix(path, "/v1/tasks/"):
		return "GET /v1/tasks/{id}"
	case strings.HasPrefix(path, "/v1/indexes/definitions/"):
		return "DELETE /v1/indexes/definitions/{name}"
	case strings.HasPrefix(path, "/v1/indexes/tasks/"):
		return "GET /v1/indexes/tasks/{task_id}"
	case path == "/v1/control/reader-lag":
		return "GET /v1/control/reader-lag"
	case path == "/v1/control/reader-freshness":
		return "GET /v1/control/reader-freshness"
	case path == "/v1/control/reader-fleet-readiness":
		return "GET /v1/control/reader-fleet-readiness"
	case path == "/v1/control/reader-traffic-gate":
		return "GET /v1/control/reader-traffic-gate"
	case path == "/v1/control/integrity-audit":
		return "GET /v1/control/integrity-audit"
	default:
		return r.Method + " " + path
	}
}

func (s *Server) recordSuppressed(tenantID string, conflicts []graph.FieldConflict) {
	if len(conflicts) == 0 {
		return
	}
	counts := map[string]int{}
	for _, conflict := range conflicts {
		resource := conflict.ResourceType
		if resource == "" {
			resource = "entity"
		}
		counts[resource]++
	}
	for resource, count := range counts {
		s.obs().Metrics.RecordSuppressed(tenantID, resource, count)
	}
}

func (s *Server) auditInfo(event string, tenantID string, fields map[string]any) {
	fields["tenant"] = tenantID
	s.obs().Logger.Info(event, fields)
}

func (s *Server) auditError(event string, tenantID string, err error, fields map[string]any) {
	fields["tenant"] = tenantID
	fields["error"] = err.Error()
	s.obs().Logger.Error(event, fields)
}
