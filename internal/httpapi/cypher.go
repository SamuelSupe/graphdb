package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"go.opentelemetry.io/otel/attribute"
)

func (s *Server) queryCypher(w http.ResponseWriter, r *http.Request) { s.handleCypher(w, r, false) }
func (s *Server) queryCypherStream(w http.ResponseWriter, r *http.Request) {
	s.handleCypher(w, r, true)
}
func (s *Server) handleCypher(w http.ResponseWriter, r *http.Request, stream bool) {
	tenant, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var body query.CypherRequest
	if !decodeJSONBody(w, r, &body, maxQueryRequestBytes) {
		return
	}
	ctx, span := startAPIPhase(r.Context(), "parse_cypher", attribute.Int("graphdb.query.cypher_bytes", len(body.Query)))
	plan, err := query.ParseCypher(body)
	endHTTPSpan(span, err)
	if err != nil {
		writeCypherError(w, err)
		return
	}
	request := plan.ReadRequest()
	ctx, id, finish := s.QueryRegistry.Start(ctx, tenant, request, "POST "+r.URL.Path, r.RemoteAddr)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", id)
	ctx, cancel := queryRequestContext(ctx, request)
	defer cancel()
	r = r.WithContext(storage.WithQueryReadMemo(ctx))
	start := time.Now()
	streamer := NewCypherStreamWriter(ctx, w)
	response, err := s.withCypherRead(r, tenant, plan, func(ctx context.Context, g *graph.Graph, options query.ExecuteOptions) (query.CypherResponse, error) {
		if !stream {
			return plan.Execute(ctx, g, options)
		}
		if err := streamer.Begin(g.Version, plan.Columns()); err != nil {
			return query.CypherResponse{}, err
		}
		return plan.ExecuteStream(ctx, g, options, streamer.Row)
	})
	err = normalizeQueryExecutionError(ctx, err)
	s.observeQuery(tenant, request, query.Response{Version: response.Version, Stats: response.Stats, Plan: response.Plan, Profile: response.Profile}, err, time.Since(start))
	if stream && streamer.Started() {
		_ = streamer.Finish(response, err)
		return
	}
	if err != nil {
		writeCypherError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) withCypherRead(r *http.Request, tenant string, plan *query.CypherPlan, run func(context.Context, *graph.Graph, query.ExecuteOptions) (query.CypherResponse, error)) (response query.CypherResponse, err error) {
	release, err := s.acquireQuery(r.Context(), tenant)
	if err != nil {
		return query.CypherResponse{}, err
	}
	defer release()
	ctx, releaseView, err := s.Store.ReadViewContext(r.Context(), tenant)
	if err != nil {
		return query.CypherResponse{}, err
	}
	defer releaseView()
	r = r.WithContext(ctx)
	request := plan.ReadRequest()
	target, err := s.readTarget(r, tenant, queryReadFreshness(request))
	if err != nil {
		return query.CypherResponse{}, err
	}
	ctx, span := startAPIPhase(r.Context(), "execute_cypher")
	r = r.WithContext(ctx)
	defer func() { endHTTPSpan(span, err) }()
	if !s.cachedMaterializedQueryAvailable(tenant, target) && !s.Store.QueryIndexSuppressed(tenant, target.ManifestVersion) {
		options, version, ok := s.Store.QueryOptions(ctx, tenant, target.ManifestVersion, plan.RequiresReverseIndex())
		if ok && version >= target.TargetVersion && plan.SupportsLazyRead(options) {
			if span != nil {
				span.SetAttributes(attribute.String("graphdb.query.execution_path", "lazy_index"))
			}
			g := graph.New()
			g.Version = version
			response, err = run(ctx, g, options)
			// A failed execution cannot switch to a newer graph snapshot.
			if errors.Is(err, query.ErrIndexUnavailable) {
				s.Store.MarkQueryIndexUnavailable(tenant, version)
			}
			return response, err
		}
	}
	if span != nil {
		span.SetAttributes(attribute.String("graphdb.query.execution_path", "materialized_graph"))
	}
	err = s.withReadOnlyGraphForRead(ctx, tenant, target, func(g *graph.Graph, _ storage.Manifest) error {
		var executeErr error
		response, executeErr = run(ctx, g, query.ExecuteOptions{})
		return executeErr
	})
	return response, err
}

func writeCypherError(w http.ResponseWriter, err error) {
	var ce *query.CypherError
	if !errors.As(err, &ce) {
		writeQueryError(w, err)
		return
	}
	status, response := queryErrorResponse(err)
	response.Detail = ce
	writeJSON(w, status, response)
}
