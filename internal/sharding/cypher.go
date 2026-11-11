package sharding

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func serveGraphCypher(w http.ResponseWriter, r *http.Request, g partition.Graph, home partition.Group, remote partition.Remote) {
	var body query.CypherRequest
	if !graphDecode(w, r, &body) {
		return
	}
	freshness, err := httpapi.ParseReadFreshness(r, httpapi.ReadFreshness{MinVersion: body.MinVersion, AllowStale: body.AllowStale})
	if err != nil {
		graphError(w, fmt.Errorf("%w: %v", storage.ErrInvalidArgument, err))
		return
	}
	body.MinVersion = freshness.MinVersion
	body.AllowStale = freshness.AllowStale
	plan, err := query.ParseCypher(body)
	if err != nil {
		graphCypherError(w, err)
		return
	}
	if body.TimeoutMS > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(body.TimeoutMS)*time.Millisecond)
		defer cancel()
		r = r.WithContext(ctx)
	}
	if r.URL.Path == "/v1/query/cypher" {
		response, err := partition.ExecutePinnedCypher(r.Context(), remote, home, g, plan, nil, nil)
		if err != nil {
			graphCypherError(w, err)
			return
		}
		graphJSON(w, http.StatusOK, response)
		return
	}
	streamer := httpapi.NewCypherStreamWriter(r.Context(), w)
	response, err := partition.ExecutePinnedCypher(r.Context(), remote, home, g, plan, func(version int64) error { return streamer.Begin(version, plan.Columns()) }, streamer.Row)
	if !streamer.Started() {
		if err != nil {
			graphCypherError(w, err)
		}
		return
	}
	_ = streamer.Finish(response, err)
}
func graphCypherError(w http.ResponseWriter, err error) {
	status, code := 0, ""
	switch {
	case errors.Is(err, query.ErrLimitExceeded), errors.Is(err, partition.ErrReadBudget):
		status, code = http.StatusTooManyRequests, "query_limit_exceeded"
	case errors.Is(err, context.DeadlineExceeded):
		status, code = http.StatusGatewayTimeout, "request_timeout"
	case errors.Is(err, context.Canceled):
		status, code = 499, "request_canceled"
	}
	if status != 0 {
		graphJSON(w, status, map[string]any{"code": code, "error": err.Error(), "message": err.Error(), "retryable": status != 499})
		return
	}
	var ce *query.CypherError
	if errors.As(err, &ce) {
		graphJSON(w, http.StatusUnprocessableEntity, map[string]any{"code": "invalid_query", "error": err.Error(), "message": err.Error(), "retryable": false, "detail": ce})
		return
	}
	graphError(w, err)
}
