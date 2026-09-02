package query

import (
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

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
	allowed := relationTypeSet(request)
	var edges []directedNeighborEdge
	if request.DirectionStrategy == "impact" {
		directions, ok := impactDirectionsForRequest(request, budget.planner)
		if !ok {
			return shardUnavailable(g, budget, false, nil)
		}
		outAllowed, inAllowed := impactIndexRelations(directions)
		if len(outAllowed) > 0 {
			out, found, err := budget.lookup.OutEdges(
				budget.ctx, entityID, outAllowed,
			)
			if err != nil || !found {
				return shardUnavailable(g, budget, found, err)
			}
			edges = appendDirectedNeighborEdges(edges, out, "out")
		}
		if len(inAllowed) > 0 {
			reverse, reverseOK := budget.lookup.(ReverseIndexLookup)
			if !reverseOK {
				return shardUnavailable(g, budget, false, nil)
			}
			incoming, found, err := reverse.InEdges(
				budget.ctx, entityID, inAllowed,
			)
			if err != nil || !found {
				return shardUnavailable(g, budget, found, err)
			}
			edges = appendDirectedNeighborEdges(edges, incoming, "in")
		}
	} else {
		switch request.Direction {
		case "out":
			out, ok, err := budget.lookup.OutEdges(budget.ctx, entityID, allowed)
			if err != nil || !ok {
				return shardUnavailable(g, budget, ok, err)
			}
			edges = appendDirectedNeighborEdges(edges, out, "out")
		case "in":
			reverse, ok := budget.lookup.(ReverseIndexLookup)
			if !ok {
				return shardUnavailable(g, budget, false, nil)
			}
			incoming, found, err := reverse.InEdges(budget.ctx, entityID, allowed)
			if err != nil || !found {
				return shardUnavailable(g, budget, found, err)
			}
			edges = appendDirectedNeighborEdges(edges, incoming, "in")
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
			edges = appendDirectedNeighborEdges(edges, out, "out")
			edges = appendDirectedNeighborEdges(edges, incoming, "in")
		default:
			return nil, false, nil
		}
	}
	edges = uniqueDirectedNeighborEdges(edges)
	sort.Slice(edges, func(left, right int) bool {
		if edges[left].edge.ID != edges[right].edge.ID {
			return edges[left].edge.ID < edges[right].edge.ID
		}
		leftEntity := directedNeighborID(
			edges[left].edge, edges[left].direction,
		)
		rightEntity := directedNeighborID(
			edges[right].edge, edges[right].direction,
		)
		if leftEntity != rightEntity {
			return leftEntity < rightEntity
		}
		return edges[left].direction < edges[right].direction
	})
	neighbors := make([]graph.Neighbor, 0, len(edges))
	for _, candidate := range edges {
		edge := candidate.edge
		if !requestEdgeMatches(request, edge) {
			continue
		}
		if err := budget.add(1); err != nil {
			return nil, false, err
		}
		neighborID := directedNeighborID(edge, candidate.direction)
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
		neighbors = append(neighbors, graph.Neighbor{
			Entity: entity, Edge: edge, Direction: candidate.direction,
		})
	}
	return neighbors, true, nil
}

func shardUnavailable(g *graph.Graph, budget *budget, ok bool, err error) ([]graph.Neighbor, bool, error) {
	if err == nil && !ok && lazyExecution(g, budget) {
		return nil, false, ErrIndexUnavailable
	}
	return nil, false, err
}

type directedNeighborEdge struct {
	edge      graph.Edge
	direction string
}

func appendDirectedNeighborEdges(
	target []directedNeighborEdge,
	edges []graph.Edge,
	direction string,
) []directedNeighborEdge {
	for _, edge := range edges {
		target = append(target, directedNeighborEdge{
			edge: edge, direction: direction,
		})
	}
	return target
}

func uniqueDirectedNeighborEdges(
	edges []directedNeighborEdge,
) []directedNeighborEdge {
	seen := make(map[string]struct{}, len(edges))
	out := edges[:0]
	for _, edge := range edges {
		key := edge.direction + "\x00" + edge.edge.ID
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
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
