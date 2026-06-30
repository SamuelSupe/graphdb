package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/query"
	"graphdb/internal/storage"
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

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request query.Request
	if !decodeJSONBody(w, r, &request, maxQueryRequestBytes) {
		return
	}
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, request, "POST /v1/query", r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	start := time.Now()
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
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, request, "POST /v1/query/gql", r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	start := time.Now()
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
	s.executeQueryStream(w, r, tenantID, request, "POST /v1/query/gql/stream")
}

func decodeGQLQueryRequest(w http.ResponseWriter, r *http.Request) (query.Request, bool) {
	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.HasPrefix(contentType, "text/plain") || strings.HasPrefix(contentType, "application/gql") {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxQueryRequestBytes))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return query.Request{}, false
		}
		request, err := query.ParseGQL(string(data))
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
	request, err := query.ParseGQL(body.Query)
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

func (s *Server) queryStream(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request query.Request
	if !decodeJSONBody(w, r, &request, maxQueryRequestBytes) {
		return
	}
	s.executeQueryStream(w, r, tenantID, request, "POST /v1/query/stream")
}

func (s *Server) executeQueryStream(w http.ResponseWriter, r *http.Request, tenantID string, request query.Request, route string) {
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, request, route, r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	start := time.Now()
	release, err := s.Admission.Acquire(r.Context(), tenantID)
	if err != nil {
		s.observeQuery(tenantID, request, query.Response{}, err, time.Since(start))
		writeQueryError(w, err)
		return
	}
	defer release()
	if handled, streamErr := s.tryLazyQueryStreamAdmitted(w, r, tenantID, request); handled {
		s.observeQuery(tenantID, request, query.Response{}, streamErr, time.Since(start))
		return
	}
	response, err := s.executeQueryAdmitted(r, tenantID, request)
	s.observeQuery(tenantID, request, response, err, time.Since(start))
	if err != nil {
		writeQueryError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	stats := response.Stats
	if err := encodeStreamItem(r.Context(), encoder, query.StreamMeta{
		Version:    response.Version,
		NextCursor: response.NextCursor,
		Stats:      &stats,
		Aggregates: response.Aggregates,
		Groups:     response.Groups,
		Plan:       response.Plan,
		Profile:    response.Profile,
	}, flush); err != nil {
		return
	}
	for _, result := range response.Results {
		if err := encodeStreamItem(r.Context(), encoder, result, flush); err != nil {
			return
		}
	}
	_ = encodeStreamItem(r.Context(), encoder, query.StreamMeta{
		Version:    response.Version,
		NextCursor: response.NextCursor,
		Stats:      &stats,
		Aggregates: response.Aggregates,
		Groups:     response.Groups,
		Profile:    response.Profile,
		Done:       true,
	}, flush)
}

func (s *Server) tryLazyQueryStreamAdmitted(w http.ResponseWriter, r *http.Request, tenantID string, request query.Request) (bool, error) {
	target, err := s.readTarget(r, tenantID, queryReadFreshness(request))
	if err != nil {
		writeQueryError(w, err)
		return true, err
	}
	options, version, ok := s.lazyQueryOptions(r.Context(), tenantID, target.ManifestVersion)
	if !ok || !target.requiresVersion(version) || !query.SupportsLazyRead(request, options.PlannerStats) {
		return false, nil
	}
	g := graph.New()
	g.Version = version
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	started := false
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
				return false, nil
			}
			writeQueryError(w, err)
			return true, err
		}
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
	ctx, queryID, finish := s.QueryRegistry.Start(r.Context(), tenantID, saved.Request, "POST /v1/query/templates/{name}/run", r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	start := time.Now()
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
	release, err := s.Admission.Acquire(r.Context(), tenantID)
	if err != nil {
		return query.Response{}, err
	}
	defer release()
	return s.executeQueryAdmitted(r, tenantID, request)
}

func (s *Server) executeQueryAdmitted(r *http.Request, tenantID string, request query.Request) (query.Response, error) {
	target, err := s.readTarget(r, tenantID, queryReadFreshness(request))
	if err != nil {
		return query.Response{}, err
	}
	options, version, ok := s.lazyQueryOptions(r.Context(), tenantID, target.ManifestVersion)
	if ok && target.requiresVersion(version) && query.SupportsLazyRead(request, options.PlannerStats) {
		g := graph.New()
		g.Version = version
		response, err := query.ExecuteContextWithOptions(r.Context(), g, request, options)
		if err == nil || !errors.Is(err, query.ErrIndexUnavailable) {
			return response, err
		}
	}
	g, manifest, err := s.loadGraphForRead(r.Context(), tenantID, target)
	if err != nil {
		return query.Response{}, err
	}
	options = s.queryOptions(r.Context(), tenantID, manifest.Version)
	return query.ExecuteContextWithOptions(r.Context(), g, request, options)
}

func queryReadFreshness(request query.Request) readFreshness {
	return readFreshness{MinVersion: request.MinVersion, AllowStale: request.AllowStale}
}

func (s *Server) queryOptions(ctx context.Context, tenantID string, version int64) query.ExecuteOptions {
	catalog, err := s.Store.GetIndexCatalog(ctx, tenantID)
	if err != nil || catalog.Version != version {
		return query.ExecuteOptions{}
	}
	return s.queryOptionsForCatalog(tenantID, catalog)
}

func (s *Server) lazyQueryOptions(ctx context.Context, tenantID string, maxVersion int64) (query.ExecuteOptions, int64, bool) {
	catalog, err := s.Store.GetIndexCatalog(ctx, tenantID)
	if err != nil || catalog.Version <= 0 || catalog.Version > maxVersion {
		return query.ExecuteOptions{}, 0, false
	}
	return s.queryOptionsForCatalog(tenantID, catalog), catalog.Version, true
}

func (s *Server) queryOptionsForCatalog(tenantID string, catalog storage.IndexCatalog) query.ExecuteOptions {
	lookup := &storage.PersistedIndexLookup{Store: s.Store, TenantID: tenantID, Version: catalog.Version, Catalog: catalog}
	return query.ExecuteOptions{
		PlannerStats: catalog.PlannerStats(),
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
	if errors.Is(err, query.ErrInvalid) {
		status = http.StatusUnprocessableEntity
	}
	if errors.Is(err, query.ErrLimitExceeded) {
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
