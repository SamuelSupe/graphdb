package partition

import (
	"context"
	"errors"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

// ExecutePinnedCypher uses one leased distributed view and one shared read
// budget for all clauses. The optional callbacks stream that same version.
func ExecutePinnedCypher(ctx context.Context, remote Remote, home Group, g Graph, plan *query.CypherPlan, onView func(int64) error, emit func([]any) error) (query.CypherResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, MaxQueryLifetime)
	defer cancel()
	if g.State != "active" {
		return query.CypherResponse{}, conflict("graph is not active")
	}
	if g.Version < plan.ReadRequest().MinVersion {
		return query.CypherResponse{}, ErrVersionNotReady
	}
	pinned, err := AcquireReadView(ctx, remote, home, g)
	if err != nil {
		return query.CypherResponse{}, err
	}
	view := View{Graph: pinned.Graph, Remote: remote}
	snapshot, options, err := queryReadOptions(view, 128)
	if err != nil {
		return query.CypherResponse{}, err
	}
	if onView != nil {
		if err := onView(snapshot.Version); err != nil {
			return query.CypherResponse{}, err
		}
	}
	var result query.CypherResponse
	if emit == nil {
		result, err = plan.Execute(ctx, snapshot, options)
	} else {
		result, err = plan.ExecuteStream(ctx, snapshot, options, emit)
	}
	if errors.Is(err, ErrReadBudget) {
		err = fmt.Errorf("%w: %v", query.ErrLimitExceeded, err)
	}
	return result, err
}
