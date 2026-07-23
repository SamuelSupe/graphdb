package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

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
	if request.DirectionStrategy == "impact" && request.Direction != "out" {
		return nil, false, nil
	}
	allowed := relationTypeSet(request)
	var edges []graph.Edge
	switch request.Direction {
	case "out":
		out, ok, err := budget.lookup.OutEdges(budget.ctx, entityID, allowed)
		if err != nil || !ok {
			return shardUnavailable(g, budget, ok, err)
		}
		edges = out
	case "in":
		reverse, ok := budget.lookup.(ReverseIndexLookup)
		if !ok {
			return shardUnavailable(g, budget, false, nil)
		}
		incoming, found, err := reverse.InEdges(budget.ctx, entityID, allowed)
		if err != nil || !found {
			return shardUnavailable(g, budget, found, err)
		}
		edges = incoming
	case "both", "":
		reverse, ok := budget.lookup.(ReverseIndexLookup)
		if !ok {
			return shardUnavailable(g, budget, false, nil)
		}
		out, outOK, err := budget.lookup.OutEdges(budget.ctx, entityID, allowed)
		if err != nil || !outOK {
			return shardUnavailable(g, budget, outOK, err)
		}
		incoming, inOK, err := reverse.InEdges(budget.ctx, entityID, allowed)
		if err != nil || !inOK {
			return shardUnavailable(g, budget, inOK, err)
		}
		edges = append(out, incoming...)
	default:
		return nil, false, nil
	}
	edges = uniqueNeighborEdges(edges)
	neighbors := make([]graph.Neighbor, 0, len(edges))
	for _, edge := range edges {
		if !requestEdgeMatches(request, edge) {
			continue
		}
		if err := budget.add(1); err != nil {
			return nil, false, err
		}
		neighborID, direction := edge.To, "out"
		if edge.To == entityID && request.Direction != "out" {
			neighborID, direction = edge.From, "in"
		}
		entity, ok, err := materializeEntity(g, neighborID, request, budget)
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
		neighbors = append(neighbors, graph.Neighbor{Entity: entity, Edge: edge, Direction: direction})
	}
	return neighbors, true, nil
}

func shardUnavailable(g *graph.Graph, budget *budget, ok bool, err error) ([]graph.Neighbor, bool, error) {
	if err == nil && !ok && lazyExecution(g, budget) {
		return nil, false, ErrIndexUnavailable
	}
	return nil, false, err
}

func uniqueNeighborEdges(edges []graph.Edge) []graph.Edge {
	seen := make(map[string]struct{}, len(edges))
	out := edges[:0]
	for _, edge := range edges {
		if _, exists := seen[edge.ID]; exists {
			continue
		}
		seen[edge.ID] = struct{}{}
		out = append(out, edge)
	}
	return out
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
