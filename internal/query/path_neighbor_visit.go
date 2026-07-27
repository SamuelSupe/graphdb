package query

import (
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func visitIndexedPathNeighbors(
	g *graph.Graph,
	entityID string,
	request Request,
	budget *budget,
	visit func(graph.Neighbor) (bool, error),
) (bool, error) {
	if budget == nil || budget.lookup == nil {
		return false, nil
	}
	if request.DirectionStrategy == "impact" {
		return visitIndexedImpactNeighbors(
			g, entityID, request, budget, visit,
		)
	}
	allowed := relationTypeSet(request)
	visitEdge := func(edge graph.Edge, direction string) (bool, error) {
		return visitIndexedPathNeighborEdge(
			g, request, budget, edge, direction, visit,
		)
	}
	switch request.Direction {
	case "out":
		lookup, ok := budget.lookup.(OutEdgeVisitLookup)
		if !ok {
			return false, nil
		}
		return lookup.VisitOutEdges(
			budget.ctx,
			entityID,
			allowed,
			"",
			func(edge graph.Edge) (bool, error) {
				return visitEdge(edge, "out")
			},
		)
	case "in":
		lookup, ok := budget.lookup.(InEdgeVisitLookup)
		if !ok {
			return false, nil
		}
		return lookup.VisitInEdges(
			budget.ctx,
			entityID,
			allowed,
			"",
			func(edge graph.Edge) (bool, error) {
				return visitEdge(edge, "in")
			},
		)
	case "both", "":
		lookup, ok := budget.lookup.(BothEdgeVisitLookup)
		if !ok {
			return false, nil
		}
		return lookup.VisitBothEdges(
			budget.ctx, entityID, allowed, "", visitEdge,
		)
	default:
		return false, nil
	}
}

func visitIndexedPathNeighborEdge(
	g *graph.Graph,
	request Request,
	budget *budget,
	edge graph.Edge,
	direction string,
	visit func(graph.Neighbor) (bool, error),
) (bool, error) {
	if !requestEdgeMatches(request, edge) {
		return true, nil
	}
	if err := budget.add(1); err != nil {
		return false, err
	}
	entity, ok, err := materializeEntity(
		g, directedNeighborID(edge, direction), request, budget,
	)
	if err != nil {
		return false, err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return false, ErrIndexUnavailable
		}
		return true, nil
	}
	if !pathAllowsKind(entity.Kind, request.Path) {
		return true, nil
	}
	return visit(graph.Neighbor{
		Entity: entity, Edge: edge, Direction: direction,
	})
}
