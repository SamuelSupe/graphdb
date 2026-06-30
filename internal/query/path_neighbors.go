package query

import "graphdb/internal/graph"

func neighborsFor(g *graph.Graph, entityID string, request Request) []graph.Neighbor {
	neighbors, _ := neighborsForBudget(g, entityID, request, nil)
	return neighbors
}

func neighborsForBudget(g *graph.Graph, entityID string, request Request, budget *budget) ([]graph.Neighbor, error) {
	if indexed, ok, err := shardNeighbors(g, entityID, request, budget); ok || err != nil {
		return indexed, err
	}
	charge := func() error {
		if budget == nil {
			return nil
		}
		return budget.add(1)
	}
	if request.DirectionStrategy == "impact" {
		neighbors, err := g.FilteredNeighbors(entityID, "both", relationTypeSet(request), request.Path.NodeKinds, true, charge)
		return filterRequestEdges(request, neighbors), err
	}
	neighbors, err := g.FilteredNeighbors(entityID, request.Direction, relationTypeSet(request), request.Path.NodeKinds, false, charge)
	return filterRequestEdges(request, neighbors), err
}

func shardNeighbors(g *graph.Graph, entityID string, request Request, budget *budget) ([]graph.Neighbor, bool, error) {
	if budget == nil || budget.lookup == nil {
		return nil, false, nil
	}
	if request.Direction != "out" {
		return nil, false, nil
	}
	edges, ok, err := budget.lookup.OutEdges(budget.ctx, entityID, relationTypeSet(request))
	if err != nil || !ok {
		if err == nil && !ok && lazyExecution(g, budget) {
			return nil, false, ErrIndexUnavailable
		}
		return nil, false, err
	}
	neighbors := make([]graph.Neighbor, 0, len(edges))
	for _, edge := range edges {
		if !requestEdgeMatches(request, edge) {
			continue
		}
		if err := budget.add(1); err != nil {
			return nil, false, err
		}
		entity, ok, err := materializeEntity(g, edge.To, request, budget)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			if lazyExecution(g, budget) {
				return nil, false, ErrIndexUnavailable
			}
			continue
		}
		if !pathAllowsKind(entity.Kind, request.Path) {
			continue
		}
		neighbors = append(neighbors, graph.Neighbor{Entity: entity, Edge: edge, Direction: "out"})
	}
	return neighbors, true, nil
}

func filterRequestEdges(request Request, neighbors []graph.Neighbor) []graph.Neighbor {
	if len(request.EdgeWhere) == 0 && request.EdgeWhereExpr == nil {
		return neighbors
	}
	out := make([]graph.Neighbor, 0, len(neighbors))
	for _, neighbor := range neighbors {
		if requestEdgeMatches(request, neighbor.Edge) {
			out = append(out, neighbor)
		}
	}
	return out
}

func relationAllowed(relationType string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[relationType]
	return ok
}
