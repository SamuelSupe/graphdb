package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func canPageNeighborsEarly(request Request) bool {
	return len(request.Sort) == 0 && len(request.Aggregate) == 0 && len(request.GroupBy) == 0
}

func executeNeighborsPage(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	if results, state, ok, err := pageDirectedNeighborsFromShard(
		g, request, cursor, budget,
	); ok || err != nil {
		if err != nil {
			return Response{}, err
		}
		if err := validatePageCursor(cursor, state); err != nil {
			return Response{}, err
		}
		return pageResponse(g.Version, results, state.hasNext, request, budget), nil
	}
	var neighbors []graph.Neighbor
	if err := budget.measure("expand-neighbors", directionDetail(request), 0, func() (int, error) {
		var err error
		neighbors, err = neighborsForBudget(g, request.ID, request, budget)
		return len(neighbors), err
	}); err != nil {
		return Response{}, err
	}
	return pageNeighborSlice(g, request, cursor, budget, neighbors)
}

func pageDirectedNeighborsFromShard(
	g *graph.Graph,
	request Request,
	cursor cursorState,
	budget *budget,
) ([]Result, *matchPageState, bool, error) {
	if budget.lookup == nil || request.DirectionStrategy == "impact" {
		return nil, nil, false, nil
	}
	if request.Direction == "both" || request.Direction == "" {
		visitor, ok := budget.lookup.(BothEdgeVisitLookup)
		if !ok {
			return nil, nil, false, nil
		}
		return pageBothNeighborsFromVisitor(
			g, request, cursor, budget, visitor,
		)
	}
	if request.Direction != "out" && request.Direction != "in" {
		return nil, nil, false, nil
	}
	if request.Direction == "out" {
		if visitor, ok := budget.lookup.(OutEdgeVisitLookup); ok {
			results, state, used, err := pageOutNeighborsFromVisitor(
				g, request, cursor, budget, visitor,
			)
			if err != nil || used {
				return results, state, used, err
			}
		}
	} else if visitor, ok := budget.lookup.(InEdgeVisitLookup); ok {
		results, state, used, err := pageInNeighborsFromVisitor(
			g, request, cursor, budget, visitor,
		)
		if err != nil || used {
			return results, state, used, err
		}
	}
	var edges []graph.Edge
	ok := false
	if err := budget.measure("expand-neighbors", directionDetail(request), 0, func() (int, error) {
		var err error
		if request.Direction == "out" {
			edges, ok, err = budget.lookup.OutEdges(
				budget.ctx, request.ID, relationTypeSet(request),
			)
		} else if reverse, available := budget.lookup.(ReverseIndexLookup); available {
			edges, ok, err = reverse.InEdges(
				budget.ctx, request.ID, relationTypeSet(request),
			)
		}
		if err == nil && !ok && lazyExecution(g, budget) {
			return 0, ErrIndexUnavailable
		}
		return len(edges), err
	}); err != nil || !ok {
		return nil, nil, ok, err
	}
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	cursorEdgeID, skipByEdgeID := cursorDirectedEdgeID(
		cursor, request.Direction,
	)
	err := budget.measure("filter-project", "", len(edges), func() (int, error) {
		startVisited := budget.visited
		for _, edge := range edges {
			if skipByEdgeID && !state.pastCursor {
				if edge.ID != cursorEdgeID {
					continue
				}
				if err := confirmCursorDirectedNeighbor(
					g, edge, request.Direction, request, cursor, budget,
				); err != nil {
					return budget.visited - startVisited, err
				}
				state.pastCursor = true
				continue
			}
			if err := budget.add(1); err != nil {
				return budget.visited - startVisited, err
			}
			entity, ok, err := materializeEntity(
				g, directedNeighborID(edge, request.Direction), request, budget,
			)
			if err != nil {
				return budget.visited - startVisited, err
			}
			if !ok {
				if lazyExecution(g, budget) {
					return budget.visited - startVisited, ErrIndexUnavailable
				}
				continue
			}
			if !pathAllowsKind(entity.Kind, request.Path) {
				continue
			}
			neighbor := graph.Neighbor{
				Entity: entity, Edge: edge, Direction: request.Direction,
			}
			stop, err := appendPageNeighbor(&results, neighbor, request, cursor, state, budget)
			if err != nil || stop {
				return budget.visited - startVisited, err
			}
		}
		return budget.visited - startVisited, nil
	})
	return results, state, true, err
}

func pageNeighborSlice(g *graph.Graph, request Request, cursor cursorState, budget *budget, neighbors []graph.Neighbor) (Response, error) {
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	err := budget.measure("filter-project", "", len(neighbors), func() (int, error) {
		startVisited := budget.visited
		for _, neighbor := range neighbors {
			stop, err := appendPageNeighbor(&results, neighbor, request, cursor, state, budget)
			if err != nil || stop {
				return budget.visited - startVisited, err
			}
		}
		return budget.visited - startVisited, nil
	})
	if err != nil {
		return Response{}, err
	}
	if err := validatePageCursor(cursor, state); err != nil {
		return Response{}, err
	}
	return pageResponse(g.Version, results, state.hasNext, request, budget), nil
}

func appendPageNeighbor(results *[]Result, neighbor graph.Neighbor, request Request, cursor cursorState, state *matchPageState, budget *budget) (bool, error) {
	budget.visited++
	entity := neighbor.Entity
	if !requestEntityMatches(request, entity) || !requestEdgeMatches(request, neighbor.Edge) {
		return false, nil
	}
	edge := neighbor.Edge
	result := Result{Entity: &entity, Edge: &edge, Direction: neighbor.Direction}
	identity := resultIdentity(result)
	if skipCursorResult(identity, cursor, state) {
		return false, nil
	}
	if len(*results) >= normalizedLimit(request.Limit) {
		state.hasNext = true
		return true, nil
	}
	applyProjection(&result, request.Project)
	*results = append(*results, result)
	return false, nil
}
