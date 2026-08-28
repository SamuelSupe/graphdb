package httpapi

import (
	"net/http"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type graphQLResponse struct {
	Data   map[string]any `json:"data,omitempty"`
	Errors gqlerror.List  `json:"errors,omitempty"`
}

func (s *Server) queryGraphQL(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var body query.GraphQLRequest
	if !decodeJSONBody(w, r, &body, maxQueryRequestBytes) {
		return
	}
	plan, requestErrors := query.ParseGraphQL(body)
	if len(requestErrors) > 0 {
		writeJSON(w, http.StatusBadRequest, graphQLResponse{Errors: requestErrors})
		return
	}
	if plan.IsEvidenceSearch() {
		s.queryGraphQLEvidence(w, r, tenantID, plan)
		return
	}
	request := plan.Request
	setAPITraceAttributes(r.Context(), queryRequestTraceAttributes(request)...)
	ctx, queryID, finish := s.QueryRegistry.Start(
		r.Context(), tenantID, request, "POST /v1/query/graphql", r.RemoteAddr,
	)
	defer finish()
	w.Header().Set("X-GraphDB-Query-ID", queryID)
	r = r.WithContext(ctx)
	start := time.Now()
	response, err := s.executeQuery(r, tenantID, request)
	s.observeQuery(tenantID, request, response, err, time.Since(start))
	if err != nil {
		status, apiError := queryErrorResponse(err)
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
	writeJSON(w, http.StatusOK, graphQLResponse{Data: plan.Data(response)})
}
