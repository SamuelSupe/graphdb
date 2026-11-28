package query

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func executeNeighbors(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	if request.ID == "" {
		return Response{}, fmt.Errorf("%w: neighbors requires id", ErrInvalid)
	}
	start, ok, err := materializeEntity(g, request.ID, request, budget)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		if lazyExecution(g, budget) && !budget.authoritativeEntities {
			return Response{}, ErrMaterializedFallback
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.ID)
	}
	if !budget.allowsEntity(start) {
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.ID)
	}
	startID := start.ID
	if err := validateDirection(request.Direction); err != nil {
		return Response{}, err
	}
	if canPageNeighborsEarly(request) {
		return executeNeighborsPage(g, startID, request, cursor, budget)
	}
	var neighbors []graph.Neighbor
	if err := budget.measure("expand-neighbors", directionDetail(request), 0, func() (int, error) {
		var err error
		neighbors, err = neighborsForBudget(g, startID, request, budget)
		return len(neighbors), err
	}); err != nil {
		return Response{}, err
	}
	acc, groupAcc := newQueryAggregators(request)
	bounded := canBuildBoundedMatchPage(request, cursor)
	keep := boundedMatchPageLimit(request, cursor)
	var results []Result
	var sorted *boundedResults
	if bounded && len(request.Sort) > 0 {
		sorted = newBoundedResults(request.Sort, keep)
	} else {
		initialCap := len(neighbors)
		if bounded {
			initialCap = min(initialCap, boundedInitialCap(keep))
		}
		results = make([]Result, 0, initialCap)
	}
	if err := budget.measure("filter-project", "", len(neighbors), func() (int, error) {
		for _, neighbor := range neighbors {
			budget.visited++
			entity := neighbor.Entity
			if !budget.allowsEntity(entity) || !requestEntityMatches(&request, &entity) {
				continue
			}
			edge := neighbor.Edge
			result := Result{Entity: &entity, Edge: &edge, Direction: neighbor.Direction}
			if err := acc.add(result); err != nil {
				return len(results) + sorted.Len(), err
			}
			if err := groupAcc.add(result); err != nil {
				return len(results) + sorted.Len(), err
			}
			if sorted != nil {
				sorted.Add(result)
			} else if !bounded || len(results) < keep {
				results = append(results, result)
			}
		}
		return len(results) + sorted.Len(), nil
	}); err != nil {
		return Response{}, err
	}
	if sorted != nil {
		results = sorted.Sorted()
		return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
	}
	if bounded {
		return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
	}
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		sortResults(results, request.Sort)
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
}
