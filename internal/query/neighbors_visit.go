package query

import (
	"context"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type directedEdgeVisit func(
	context.Context,
	string,
	map[string]struct{},
	string,
	func(graph.Edge) (bool, error),
) (bool, error)

func pageOutNeighborsFromVisitor(
	g *graph.Graph,
	request Request,
	cursor cursorState,
	budget *budget,
	visitor OutEdgeVisitLookup,
) ([]Result, *matchPageState, bool, error) {
	return pageDirectedNeighborsFromVisitor(
		g, request, cursor, budget, "out", visitor.VisitOutEdges,
	)
}

func pageInNeighborsFromVisitor(
	g *graph.Graph,
	request Request,
	cursor cursorState,
	budget *budget,
	visitor InEdgeVisitLookup,
) ([]Result, *matchPageState, bool, error) {
	return pageDirectedNeighborsFromVisitor(
		g, request, cursor, budget, "in", visitor.VisitInEdges,
	)
}

func pageBothNeighborsFromVisitor(
	g *graph.Graph,
	request Request,
	cursor cursorState,
	budget *budget,
	visitor BothEdgeVisitLookup,
) ([]Result, *matchPageState, bool, error) {
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	_, cursorEdgeID, hasCursorEdge := cursorNeighborEdge(cursor)
	startEdgeID := ""
	if hasCursorEdge {
		startEdgeID = cursorEdgeID
	}
	indexAvailable := false
	scanned := 0
	err := budget.measure(
		"expand-neighbors",
		directionDetail(request),
		0,
		func() (int, error) {
			var visitErr error
			indexAvailable, visitErr = visitor.VisitBothEdges(
				budget.ctx,
				request.ID,
				relationTypeSet(request),
				startEdgeID,
				func(edge graph.Edge, direction string) (bool, error) {
					scanned++
					if err := budget.add(1); err != nil {
						return false, err
					}
					entity, ok, err := materializeEntity(
						g,
						directedNeighborID(edge, direction),
						request,
						budget,
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
					stop, err := appendPageNeighbor(
						&results,
						graph.Neighbor{
							Entity:    entity,
							Edge:      edge,
							Direction: direction,
						},
						request,
						cursor,
						state,
						budget,
					)
					return !stop, err
				},
			)
			if visitErr == nil && !indexAvailable &&
				lazyExecution(g, budget) {
				visitErr = ErrIndexUnavailable
			}
			return scanned, visitErr
		},
	)
	return results, state, indexAvailable, err
}

func pageDirectedNeighborsFromVisitor(
	g *graph.Graph,
	request Request,
	cursor cursorState,
	budget *budget,
	direction string,
	visitEdges directedEdgeVisit,
) ([]Result, *matchPageState, bool, error) {
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	cursorEdgeID, hasCursorEdge := cursorDirectedEdgeID(cursor, direction)
	startEdgeID := ""
	if hasCursorEdge {
		startEdgeID = cursorEdgeID
	}
	indexAvailable := false
	scanned := 0
	err := budget.measure(
		"expand-neighbors",
		directionDetail(request),
		0,
		func() (int, error) {
			var visitErr error
			indexAvailable, visitErr = visitEdges(
				budget.ctx,
				request.ID,
				relationTypeSet(request),
				startEdgeID,
				func(edge graph.Edge) (bool, error) {
					scanned++
					if hasCursorEdge && !state.pastCursor {
						if edge.ID != cursorEdgeID {
							return false, invalidCursorAfter(cursor)
						}
						if err := confirmCursorDirectedNeighbor(
							g, edge, direction, request, cursor, budget,
						); err != nil {
							return false, err
						}
						state.pastCursor = true
						return true, nil
					}
					if err := budget.add(1); err != nil {
						return false, err
					}
					entityID := directedNeighborID(edge, direction)
					entity, ok, err := materializeEntity(
						g, entityID, request, budget,
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
					stop, err := appendPageNeighbor(
						&results,
						graph.Neighbor{
							Entity:    entity,
							Edge:      edge,
							Direction: direction,
						},
						request,
						cursor,
						state,
						budget,
					)
					return !stop, err
				},
			)
			if visitErr == nil && !indexAvailable && lazyExecution(g, budget) {
				visitErr = ErrIndexUnavailable
			}
			return scanned, visitErr
		},
	)
	return results, state, indexAvailable, err
}

func cursorDirectedEdgeID(
	cursor cursorState,
	direction string,
) (string, bool) {
	if cursor.Legacy || cursor.After == "" {
		return "", false
	}
	id, ok := strings.CutPrefix(
		cursor.After, "edge:"+direction+":",
	)
	return id, ok && id != ""
}

func cursorNeighborEdge(
	cursor cursorState,
) (string, string, bool) {
	if cursor.Legacy || cursor.After == "" {
		return "", "", false
	}
	value, ok := strings.CutPrefix(cursor.After, "edge:")
	if !ok {
		return "", "", false
	}
	direction, id, ok := strings.Cut(value, ":")
	if !ok || id == "" ||
		(direction != "in" && direction != "out") {
		return "", "", false
	}
	return direction, id, true
}

func directedNeighborID(edge graph.Edge, direction string) string {
	if direction == "in" {
		return edge.From
	}
	return edge.To
}

func confirmCursorDirectedNeighbor(
	g *graph.Graph,
	edge graph.Edge,
	direction string,
	request Request,
	cursor cursorState,
	budget *budget,
) error {
	if err := budget.add(1); err != nil {
		return err
	}
	entity, ok, err := materializeEntity(
		g, directedNeighborID(edge, direction), request, budget,
	)
	if err != nil {
		return err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return ErrIndexUnavailable
		}
		return invalidCursorAfter(cursor)
	}
	if !pathAllowsKind(entity.Kind, request.Path) ||
		!requestEntityMatches(request, entity) ||
		!requestEdgeMatches(request, edge) {
		return invalidCursorAfter(cursor)
	}
	budget.visited++
	return nil
}
