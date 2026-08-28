package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (s *Server) queryGraphQLEvidence(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
	plan query.GraphQLPlan,
) {
	request := plan.EvidenceRequest
	registryRequest := query.Request{
		Op:         "evidence_search",
		MinVersion: request.MinVersion,
	}
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(registryRequest)...)
	ctx, queryID, finish := s.QueryRegistry.Start(
		r.Context(),
		tenantID,
		registryRequest,
		"POST /v1/query/graphql",
		r.RemoteAddr,
	)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)

	start := time.Now()
	response, err := s.executeEvidenceSearch(r.Context(), tenantID, request)
	s.observeEvidenceSearch(tenantID, request, response, err, time.Since(start))
	if err != nil {
		status, apiError := evidenceSearchErrorResponse(err)
		writeJSON(w, http.StatusOK, graphQLResponse{
			Data: map[string]any{plan.RootName: nil},
			Errors: gqlerror.List{&gqlerror.Error{
				Message: apiError.Message,
				Path:    ast.Path{ast.PathName(plan.RootName)},
				Extensions: map[string]any{
					"code":       apiError.Code,
					"retryable":  apiError.Retryable,
					"httpStatus": status,
					"detail":     apiError.Detail,
				},
			}},
		})
		return
	}
	writeJSON(w, http.StatusOK, graphQLResponse{Data: plan.EvidenceData(response)})
}

func (s *Server) executeEvidenceSearch(
	ctx context.Context,
	tenantID string,
	request retrieval.SearchRequest,
) (retrieval.SearchResponse, error) {
	release, err := s.acquireQuery(ctx, tenantID)
	if err != nil {
		return retrieval.SearchResponse{}, err
	}
	defer release()
	if s.RetrievalSearcher == nil {
		return retrieval.SearchResponse{}, retrieval.ErrNotReady
	}
	return s.RetrievalSearcher.SearchEvidence(ctx, tenantID, request)
}

func evidenceSearchErrorResponse(err error) (int, ErrorResponse) {
	switch {
	case errors.Is(err, retrieval.ErrInvalid):
		return http.StatusUnprocessableEntity, buildErrorResponse(
			ErrorCodeInvalidQuery,
			err.Error(),
			false,
			nil,
		)
	case errors.Is(err, retrieval.ErrNotReady):
		return http.StatusServiceUnavailable, buildErrorResponse(
			ErrorCodeRetrievalNotReady,
			err.Error(),
			true,
			nil,
		)
	case errors.Is(err, retrieval.ErrNotFresh):
		var freshness *retrieval.NotFreshError
		detail := any(nil)
		if errors.As(err, &freshness) {
			detail = map[string]any{
				"visible_version":  freshness.VisibleVersion,
				"required_version": freshness.RequiredVersion,
			}
		}
		return http.StatusServiceUnavailable, buildErrorResponse(
			ErrorCodeIndexNotFresh,
			err.Error(),
			true,
			detail,
		)
	case errors.Is(err, retrieval.ErrEmbeddingUnavailable):
		return http.StatusServiceUnavailable, buildErrorResponse(
			ErrorCodeEmbeddingUnavailable,
			err.Error(),
			true,
			nil,
		)
	case errors.Is(err, retrieval.ErrBudgetExceeded):
		return http.StatusTooManyRequests, buildErrorResponse(
			ErrorCodeRetrievalBudgetExceeded,
			err.Error(),
			false,
			nil,
		)
	case errors.Is(err, query.ErrLimitExceeded):
		return queryErrorResponse(err)
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, errorResponseFor(
			http.StatusGatewayTimeout,
			err,
			"",
			nil,
		)
	case errors.Is(err, context.Canceled):
		return 499, errorResponseFor(499, err, "", nil)
	default:
		return http.StatusInternalServerError, errorResponseFor(
			http.StatusInternalServerError,
			err,
			"",
			nil,
		)
	}
}

func (s *Server) observeEvidenceSearch(
	tenantID string,
	request retrieval.SearchRequest,
	response retrieval.SearchResponse,
	err error,
	duration time.Duration,
) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	threshold := s.obs().SlowQueryThreshold
	slow := threshold > 0 && duration >= threshold
	s.obs().Metrics.RecordQuery(tenantID, "evidence_search", status, duration, slow)
	fields := map[string]any{
		"tenant":               tenantID,
		"op":                   "evidence_search",
		"status":               status,
		"duration_ms":          float64(duration.Microseconds()) / 1000,
		"version":              response.Version,
		"retrieval_revision":   response.RetrievalRevision,
		"embedding_generation": response.EmbeddingGeneration,
		"vector_candidates":    response.Stats.VectorCandidates,
		"lexical_candidates":   response.Stats.LexicalCandidates,
		"visited":              response.Stats.Visited,
		"returned":             response.Stats.Returned,
		"top_k":                request.TopK,
		"max_depth":            request.Expansion.Depth(),
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
