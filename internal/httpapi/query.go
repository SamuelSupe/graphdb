package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel/attribute"
)

type GQLQueryRequest struct {
	Query      string `json:"query"`
	MinVersion int64  `json:"min_version,omitempty"`
	AllowStale bool   `json:"allow_stale,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	TimeoutMS  int    `json:"timeout_ms,omitempty"`
	CostLimit  int    `json:"cost_limit,omitempty"`
	Profile    bool   `json:"profile,omitempty"`
}

const lazyUnavailableBackoff = 5 * time.Second

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request query.Request
	if !decodeJSONBody(w, r, &request, maxQueryRequestBytes) {
		return
	}
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(request)...)
	start := time.Now()
	if err := query.ValidateRequest(request); err != nil {
		s.observeQuery(tenantID, request, query.Response{}, err, time.Since(start))
		writeQueryError(w, err)
		return
	}
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, request, "POST /v1/query", r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	response, err := s.executeQuery(r, tenantID, request)
	s.observeQuery(tenantID, request, response, err, time.Since(start))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) queryGQL(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	request, ok := decodeGQLQueryRequest(w, r)
	if !ok {
		return
	}
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(request)...)
	start := time.Now()
	if err := query.ValidateRequest(request); err != nil {
		s.observeQuery(tenantID, request, query.Response{}, err, time.Since(start))
		writeQueryError(w, err)
		return
	}
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, request, "POST /v1/query/gql", r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	response, err := s.executeQuery(r, tenantID, request)
	s.observeQuery(tenantID, request, response, err, time.Since(start))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) queryGQLStream(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	request, ok := decodeGQLQueryRequest(w, r)
	if !ok {
		return
	}
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(request)...)
	s.executeQueryStream(w, r, tenantID, request, "POST /v1/query/gql/stream")
}

func decodeGQLQueryRequest(w http.ResponseWriter, r *http.Request) (query.Request, bool) {
	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.HasPrefix(contentType, "text/plain") || strings.HasPrefix(contentType, "application/gql") {
		_, span := startAPIPhase(r.Context(), "decode_request", traceRequestAttributes(r, maxQueryRequestBytes)...)
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxQueryRequestBytes))
		if err != nil {
			endHTTPSpan(span, err)
			writeError(w, http.StatusBadRequest, err.Error())
			return query.Request{}, false
		}
		if span != nil {
			span.SetAttributes(
				attribute.Bool("graphdb.request.decoded", true),
				attribute.Int("graphdb.request.body_bytes", len(data)),
			)
		}
		endHTTPSpan(span, nil)
		request, err := parseGQLRequest(r.Context(), string(data))
		if err != nil {
			writeQueryError(w, err)
			return query.Request{}, false
		}
		return request, true
	}
	var body GQLQueryRequest
	if !decodeJSONBody(w, r, &body, maxQueryRequestBytes) {
		return query.Request{}, false
	}
	if strings.TrimSpace(body.Query) == "" {
		writeError(w, http.StatusBadRequest, "gql query is required")
		return query.Request{}, false
	}
	request, err := parseGQLRequest(r.Context(), body.Query)
	if err != nil {
		writeQueryError(w, err)
		return query.Request{}, false
	}
	request.MinVersion = body.MinVersion
	request.AllowStale = body.AllowStale
	request.Cursor = body.Cursor
	request.TimeoutMS = body.TimeoutMS
	request.CostLimit = body.CostLimit
	request.Profile = request.Profile || body.Profile
	return request, true
}

func parseGQLRequest(ctx context.Context, gql string) (request query.Request, err error) {
	_, span := startAPIPhase(ctx, "parse_gql", attribute.Int("graphdb.query.gql_bytes", len(gql)))
	defer func() { endHTTPSpan(span, err) }()
	return query.ParseGQL(gql)
}

func (s *Server) queryStream(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request query.Request
	if !decodeJSONBody(w, r, &request, maxQueryRequestBytes) {
		return
	}
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(request)...)
	s.executeQueryStream(w, r, tenantID, request, "POST /v1/query/stream")
}

func (s *Server) executeQueryStream(w http.ResponseWriter, r *http.Request, tenantID string, request query.Request, route string) {
	start := time.Now()
	if err := query.ValidateRequest(request); err != nil {
		s.observeQuery(tenantID, request, query.Response{}, err, time.Since(start))
		writeQueryError(w, err)
		return
	}
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, request, route, r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	ctx, cancel := queryRequestContext(ctx, request)
	defer cancel()
	r = r.WithContext(withQueryReadMemo(ctx))
	release, err := s.acquireQuery(r.Context(), tenantID)
	if err != nil {
		err = normalizeQueryExecutionError(r.Context(), err)
		s.observeQuery(tenantID, request, query.Response{}, err, time.Since(start))
		writeQueryError(w, err)
		return
	}
	defer release()
	if handled, streamErr := s.tryLazyQueryStreamAdmitted(w, r, tenantID, request); handled {
		streamErr = normalizeQueryExecutionError(r.Context(), streamErr)
		s.observeQuery(tenantID, request, query.Response{}, streamErr, time.Since(start))
		return
	}
	response, err := s.executeQueryAdmitted(r, tenantID, request)
	err = normalizeQueryExecutionError(r.Context(), err)
	s.observeQuery(tenantID, request, response, err, time.Since(start))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	_ = encodeMaterializedQueryStream(w, r, response)
}

func encodeMaterializedQueryStream(w http.ResponseWriter, r *http.Request, response query.Response) (err error) {
	ctx, span := startAPIPhase(r.Context(), "encode_stream", queryResponseTraceAttributes(response)...)
	defer func() { endHTTPSpan(span, err) }()
	r = r.WithContext(ctx)
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	stats := response.Stats
	if err = encodeStreamItem(r.Context(), encoder, query.StreamMeta{
		Version:    response.Version,
		NextCursor: response.NextCursor,
		Stats:      &stats,
		Aggregates: response.Aggregates,
		Groups:     response.Groups,
		Plan:       response.Plan,
		Profile:    response.Profile,
	}, flush); err != nil {
		return err
	}
	for _, result := range response.Results {
		if err = encodeStreamItem(r.Context(), encoder, result, flush); err != nil {
			return err
		}
	}
	err = encodeStreamItem(r.Context(), encoder, query.StreamMeta{
		Version:    response.Version,
		NextCursor: response.NextCursor,
		Stats:      &stats,
		Aggregates: response.Aggregates,
		Groups:     response.Groups,
		Profile:    response.Profile,
		Done:       true,
	}, flush)
	return err
}

func (s *Server) tryLazyQueryStreamAdmitted(w http.ResponseWriter, r *http.Request, tenantID string, request query.Request) (handled bool, resultErr error) {
	ctx, span := startAPIPhase(r.Context(), "execute_lazy_stream", append([]attribute.KeyValue{
		attribute.String("graphdb.tenant", tenantID),
	}, queryRequestTraceAttributes(request)...)...)
	r = r.WithContext(ctx)
	started := false
	defer func() {
		if span != nil {
			span.SetAttributes(
				attribute.Bool("graphdb.query.lazy_stream.handled", handled),
				attribute.Bool("graphdb.query.lazy_stream.started", started),
			)
		}
		endHTTPSpan(span, resultErr)
	}()

	target, err := s.readTarget(r, tenantID, queryReadFreshness(request))
	if err != nil {
		err = normalizeQueryExecutionError(r.Context(), err)
		writeQueryError(w, err)
		return true, err
	}
	options := query.ExecuteOptions{}
	version := int64(0)
	ok := false
	useCachedGraph := s.cachedMaterializedQueryAvailable(tenantID, target)
	if !useCachedGraph && !s.lazyQuerySuppressed(tenantID, target.ManifestVersion) {
		options, version, ok = s.lazyQueryOptions(
			r.Context(), tenantID, target.ManifestVersion,
			query.RequiresReverseIndex(request),
		)
	}
	if ok && s.lazyQuerySuppressed(tenantID, version) {
		return false, nil
	}
	if !ok || !target.requiresVersion(version) || !query.SupportsLazyRead(request, options.PlannerStats) {
		return false, nil
	}
	g := graph.New()
	g.Version = version
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	ok, err = query.StreamContextWithOptions(r.Context(), g, request, options, func(item any) error {
		if !started {
			w.Header().Set("Content-Type", "application/x-ndjson")
			started = true
		}
		return encodeStreamItem(r.Context(), encoder, item, flush)
	})
	if !ok {
		return false, nil
	}
	if err != nil {
		if !started {
			if errors.Is(err, query.ErrIndexUnavailable) {
				s.markLazyQueryUnavailable(tenantID, version)
				return false, nil
			}
			err = normalizeQueryExecutionError(r.Context(), err)
			writeQueryError(w, err)
			return true, err
		}
		err = normalizeQueryExecutionError(r.Context(), err)
		_ = encodeStreamItem(r.Context(), encoder, queryStreamError(err), flush)
	}
	return true, err
}

func encodeStreamItem(ctx context.Context, encoder *json.Encoder, item any, flush func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := encoder.Encode(item); err != nil {
		return err
	}
	if flush != nil {
		if err := flush(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func streamFlush(w http.ResponseWriter) func() error {
	controller := http.NewResponseController(w)
	return func() error {
		if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		return nil
	}
}

func (s *Server) listQueryTemplates(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	items, err := s.Store.ListSavedQueries(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queries": items})
}

func (s *Server) saveQueryTemplate(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "saved query updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var saved storage.SavedQuery
	if !decodeJSONBody(w, r, &saved, maxQueryRequestBytes) {
		return
	}
	saved, err := s.Store.SaveQuery(r.Context(), tenantID, saved)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) runQueryTemplate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	name, err := queryTemplateNameFromPath(r)
	if err != nil || name == "" {
		writeError(w, http.StatusBadRequest, "template path must be /v1/query/templates/{name}/run")
		return
	}
	saved, err := s.Store.GetSavedQuery(r.Context(), tenantID, name)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(saved.Request)...)
	start := time.Now()
	if err := query.ValidateRequest(saved.Request); err != nil {
		s.observeQuery(tenantID, saved.Request, query.Response{}, err, time.Since(start))
		writeQueryError(w, err)
		return
	}
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, saved.Request, "POST /v1/query/templates/{name}/run", r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	response, err := s.executeQuery(r, tenantID, saved.Request)
	s.observeQuery(tenantID, saved.Request, response, err, time.Since(start))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func queryTemplateNameFromPath(r *http.Request) (string, error) {
	const prefix = "/v1/query/templates/"
	const suffix = "/run"
	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix) || !strings.HasSuffix(escaped, suffix) {
		return "", nil
	}
	rawName := strings.TrimSuffix(strings.TrimPrefix(escaped, prefix), suffix)
	if rawName == "" {
		return "", nil
	}
	return url.PathUnescape(rawName)
}

func (s *Server) executeQuery(r *http.Request, tenantID string, request query.Request) (query.Response, error) {
	if err := query.ValidateRequest(request); err != nil {
		return query.Response{}, err
	}
	ctx, cancel := queryRequestContext(r.Context(), request)
	defer cancel()
	r = r.WithContext(withQueryReadMemo(ctx))
	release, err := s.acquireQuery(r.Context(), tenantID)
	if err != nil {
		return query.Response{}, normalizeQueryExecutionError(ctx, err)
	}
	defer release()
	response, err := s.executeQueryAdmitted(r, tenantID, request)
	return response, normalizeQueryExecutionError(ctx, err)
}

func (s *Server) executeQueryAdmitted(r *http.Request, tenantID string, request query.Request) (response query.Response, err error) {
	ctx, span := startAPIPhase(r.Context(), "execute_query", append([]attribute.KeyValue{
		attribute.String("graphdb.tenant", tenantID),
	}, queryRequestTraceAttributes(request)...)...)
	r = r.WithContext(ctx)
	defer func() {
		if span != nil {
			span.SetAttributes(queryResponseTraceAttributes(response)...)
		}
		endHTTPSpan(span, err)
	}()
	target, err := s.readTarget(r, tenantID, queryReadFreshness(request))
	if err != nil {
		return query.Response{}, err
	}
	options := query.ExecuteOptions{}
	version := int64(0)
	ok := false
	useCachedGraph := s.cachedMaterializedQueryAvailable(tenantID, target)
	if !useCachedGraph {
		if !s.lazyQuerySuppressed(tenantID, target.ManifestVersion) {
			options, version, ok = s.lazyQueryOptions(
				r.Context(), tenantID, target.ManifestVersion,
				query.RequiresReverseIndex(request),
			)
		} else if span != nil {
			span.SetAttributes(attribute.Bool("graphdb.query.lazy_suppressed", true))
		}
	}
	if ok && s.lazyQuerySuppressed(tenantID, version) {
		ok = false
		if span != nil {
			span.SetAttributes(attribute.Bool("graphdb.query.lazy_suppressed", true))
		}
	}
	if ok && target.requiresVersion(version) && query.SupportsLazyRead(request, options.PlannerStats) {
		if span != nil {
			span.SetAttributes(attribute.String("graphdb.query.execution_path", "lazy_index"))
		}
		g := graph.New()
		g.Version = version
		response, err = query.ExecuteContextWithOptions(r.Context(), g, request, options)
		if err == nil || !errors.Is(err, query.ErrIndexUnavailable) {
			return response, err
		}
		s.markLazyQueryUnavailable(tenantID, version)
		if span != nil {
			span.SetAttributes(attribute.Bool("graphdb.query.lazy_fallback", true))
		}
	}
	if span != nil {
		span.SetAttributes(
			attribute.String("graphdb.query.execution_path", "materialized_graph"),
			attribute.Bool("graphdb.query.materialized_cache_available", useCachedGraph),
		)
	}
	err = s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, _ storage.Manifest) error {
		var executeErr error
		response, executeErr = query.ExecuteContextWithOptions(r.Context(), g, request, query.ExecuteOptions{})
		return executeErr
	})
	return response, err
}

func (s *Server) cachedMaterializedQueryAvailable(tenantID string, target readTarget) bool {
	if s.Mode != "all" || s.Cache == nil {
		return false
	}
	version, ok := s.Cache.CachedVersion(tenantID)
	return ok && target.requiresVersion(version)
}

func queryRequestContext(parent context.Context, request query.Request) (context.Context, context.CancelFunc) {
	if request.TimeoutMS <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, time.Duration(request.TimeoutMS)*time.Millisecond)
}

func normalizeQueryExecutionError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func (s *Server) lazyQuerySuppressed(tenantID string, version int64) bool {
	if version <= 0 || version == unconstrainedVersion {
		return false
	}
	key := fmt.Sprintf("%s\x00%d", tenantID, version)
	value, ok := s.lazyUnavailable.Load(key)
	if !ok {
		return false
	}
	expiresAt, ok := value.(time.Time)
	if !ok || !time.Now().Before(expiresAt) {
		s.lazyUnavailable.Delete(key)
		return false
	}
	return true
}

func (s *Server) markLazyQueryUnavailable(tenantID string, version int64) {
	if version <= 0 || version == unconstrainedVersion {
		return
	}
	key := fmt.Sprintf("%s\x00%d", tenantID, version)
	expiresAt := time.Now().Add(lazyUnavailableBackoff)
	s.lazyUnavailable.Store(key, expiresAt)
	time.AfterFunc(lazyUnavailableBackoff, func() {
		s.lazyUnavailable.CompareAndDelete(key, expiresAt)
	})
}

func (s *Server) acquireQuery(ctx context.Context, tenantID string) (func(), error) {
	acquireCtx, span := startAPIPhase(ctx, "query_admission.acquire", attribute.String("graphdb.tenant", tenantID))
	start := time.Now()
	release, err := s.Admission.Acquire(acquireCtx, tenantID)
	waited := time.Since(start)
	if span != nil {
		result := "accepted"
		if err != nil {
			result = "rejected"
		}
		span.SetAttributes(
			attribute.String("graphdb.query_admission.result", result),
			attribute.Int64("graphdb.query_admission.wait_ms", waited.Milliseconds()),
		)
	}
	endHTTPSpan(span, err)
	return release, err
}

func queryReadFreshness(request query.Request) readFreshness {
	return readFreshness{MinVersion: request.MinVersion, AllowStale: request.AllowStale}
}

func (s *Server) lazyQueryOptions(
	ctx context.Context,
	tenantID string,
	maxVersion int64,
	includeReverse bool,
) (query.ExecuteOptions, int64, bool) {
	expectedVersion := maxVersion
	if maxVersion == unconstrainedVersion {
		expectedVersion = 0
	}
	catalog, err := s.currentQueryCatalog(ctx, tenantID, expectedVersion)
	if err != nil || catalog.Version <= 0 || catalog.Version > maxVersion {
		return query.ExecuteOptions{}, 0, false
	}
	return s.queryOptionsForCatalog(
		ctx, tenantID, catalog, includeReverse,
	), catalog.Version, true
}

func (s *Server) queryOptionsForCatalog(
	ctx context.Context,
	tenantID string,
	catalog storage.IndexCatalog,
	includeReverse bool,
) query.ExecuteOptions {
	lookup := &storage.PersistedIndexLookup{Store: s.Store, TenantID: tenantID, Version: catalog.Version, Catalog: catalog}
	stats := catalog.PlannerStats()
	if includeReverse {
		reverse, err := s.currentQueryReverseCatalog(
			ctx, tenantID, catalog.Version,
		)
		if err == nil {
			lookup.ReverseCatalog = &reverse
			stats.ReverseEdgeIndexAvailable = true
			for _, shard := range reverse.EdgeShards {
				stats.ReverseEdgeShards = append(stats.ReverseEdgeShards, query.PlannerEdgeStat{
					RelationType:    shard.RelationType,
					ImpactDirection: shard.ImpactDirection,
					Shard:           shard.Shard,
					EdgeCount:       shard.EdgeCount,
				})
			}
		}
	}
	return query.ExecuteOptions{
		PlannerStats: stats,
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
}

func writeQueryError(w http.ResponseWriter, err error) {
	if writeReaderNotFresh(w, err) {
		return
	}
	status, response := queryErrorResponse(err)
	writeJSON(w, status, response)
}

func queryStreamError(err error) StreamErrorResponse {
	_, response := queryErrorResponse(err)
	return streamErrorResponse(response)
}

func queryErrorResponse(err error) (int, ErrorResponse) {
	if freshness, ok := asReaderNotFresh(err); ok {
		return http.StatusServiceUnavailable, buildErrorResponse(ErrorCodeReaderNotFresh, err.Error(), true, freshness.detail())
	}
	status := http.StatusBadRequest
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	} else if errors.Is(err, context.Canceled) {
		status = statusClientClosedRequest
	} else if errors.Is(err, query.ErrInvalid) {
		status = http.StatusUnprocessableEntity
	} else if errors.Is(err, query.ErrLimitExceeded) {
		status = http.StatusTooManyRequests
	} else if errors.Is(err, storage.ErrReaderLoadBusy) {
		status = http.StatusTooManyRequests
	}
	return status, errorResponseFor(status, err, "", nil)
}

func (s *Server) observeQuery(tenantID string, request query.Request, response query.Response, err error, duration time.Duration) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	op := request.Op
	if op == "profile" || op == "explain" {
		op = request.TargetOp
	}
	if op == "" {
		op = "unknown"
	}
	threshold := s.obs().SlowQueryThreshold
	slow := threshold > 0 && duration >= threshold
	s.obs().Metrics.RecordQuery(tenantID, op, status, duration, slow)
	fields := map[string]any{
		"tenant":      tenantID,
		"op":          op,
		"status":      status,
		"duration_ms": float64(duration.Microseconds()) / 1000,
		"version":     response.Version,
		"scanned":     response.Stats.Scanned,
		"visited":     response.Stats.Visited,
		"returned":    response.Stats.Returned,
		"cost":        response.Stats.Cost,
		"truncated":   response.Stats.Truncated,
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	if response.Version > 0 {
		s.recordReaderVisible(tenantID, response.Version)
	}
	if slow {
		s.obs().Logger.Info("slow_query", fields)
		return
	}
	s.obs().Logger.Info("query_completed", fields)
}
