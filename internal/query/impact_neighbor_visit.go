package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func visitIndexedImpactNeighbors(
	g *graph.Graph,
	entityID string,
	request Request,
	budget *budget,
	visit func(graph.Neighbor) (bool, error),
) (bool, error) {
	directions, ok := impactDirectionsForRequest(request, budget.planner)
	if !ok {
		return false, nil
	}
	out, in := impactIndexRelations(directions)
	if len(out) == 0 && len(in) == 0 {
		return true, nil
	}
	visitEdge := func(edge graph.Edge, direction string) (bool, error) {
		if !impactDirectionAllows(directions[edge.Type], direction) {
			return true, nil
		}
		return visitIndexedPathNeighborEdge(
			g, request, budget, edge, direction, visit,
		)
	}
	if len(in) == 0 {
		lookup, ok := budget.lookup.(OutEdgeVisitLookup)
		if !ok {
			return false, nil
		}
		return lookup.VisitOutEdges(
			budget.ctx, entityID, out, "",
			func(edge graph.Edge) (bool, error) {
				return visitEdge(edge, "out")
			},
		)
	}
	if len(out) == 0 {
		lookup, ok := budget.lookup.(InEdgeVisitLookup)
		if !ok {
			return false, nil
		}
		return lookup.VisitInEdges(
			budget.ctx, entityID, in, "",
			func(edge graph.Edge) (bool, error) {
				return visitEdge(edge, "in")
			},
		)
	}
	lookup, ok := budget.lookup.(BothEdgeVisitLookup)
	if !ok {
		return false, nil
	}
	return lookup.VisitBothEdges(
		budget.ctx, entityID, mergeRelationSets(out, in), "", visitEdge,
	)
}

func mergeRelationSets(left, right map[string]struct{}) map[string]struct{} {
	merged := make(map[string]struct{}, len(left)+len(right))
	for relationType := range left {
		merged[relationType] = struct{}{}
	}
	for relationType := range right {
		merged[relationType] = struct{}{}
	}
	return merged
}
