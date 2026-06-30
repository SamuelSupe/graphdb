package query

import (
	"fmt"

	"graphdb/internal/graph"
)

func executeNeighbors(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	if request.ID == "" {
		return Response{}, fmt.Errorf("%w: neighbors requires id", ErrInvalid)
	}
	if _, ok, err := materializeEntity(g, request.ID, request, budget); err != nil {
		return Response{}, err
	} else if !ok {
		if lazyExecution(g, budget) {
			return Response{}, ErrIndexUnavailable
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.ID)
	}
	if err := validateDirection(request.Direction); err != nil {
		return Response{}, err
	}
	if canPageNeighborsEarly(request) {
		return executeNeighborsPage(g, request, cursor, budget)
	}
	var neighbors []graph.Neighbor
	if err := budget.measure("expand-neighbors", directionDetail(request), 0, func() (int, error) {
		var err error
		neighbors, err = neighborsForBudget(g, request.ID, request, budget)
		return len(neighbors), err
	}); err != nil {
		return Response{}, err
	}
	results := make([]Result, 0, len(neighbors))
	if err := budget.measure("filter-project", "", len(neighbors), func() (int, error) {
		for _, neighbor := range neighbors {
			budget.visited++
			entity := neighbor.Entity
			if !requestEntityMatches(request, entity) {
				continue
			}
			edge := neighbor.Edge
			result := Result{Entity: &entity, Edge: &edge, Direction: neighbor.Direction}
			results = append(results, result)
		}
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		sortResults(results, request.Sort)
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	return buildResponse(g.Version, results, request, cursor, budget)
}
