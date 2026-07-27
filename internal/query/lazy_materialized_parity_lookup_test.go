package query

import (
	"context"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type graphParityLookup struct {
	graph *graph.Graph
}

func (l *graphParityLookup) MatchFieldIndex(
	context.Context, string, string, []any,
) ([]string, bool, error) {
	return nil, false, nil
}

func (l *graphParityLookup) OutEdges(
	_ context.Context,
	from string,
	allowed map[string]struct{},
) ([]graph.Edge, bool, error) {
	return l.edges(from, "out", allowed), true, nil
}

func (l *graphParityLookup) InEdges(
	_ context.Context,
	to string,
	allowed map[string]struct{},
) ([]graph.Edge, bool, error) {
	return l.edges(to, "in", allowed), true, nil
}

func (l *graphParityLookup) VisitOutEdges(
	ctx context.Context,
	from string,
	allowed map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	return visitParityEdges(
		ctx, l.edges(from, "out", allowed), startEdgeID, visit,
	)
}

func (l *graphParityLookup) VisitInEdges(
	ctx context.Context,
	to string,
	allowed map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	return visitParityEdges(
		ctx, l.edges(to, "in", allowed), startEdgeID, visit,
	)
}

func (l *graphParityLookup) VisitBothEdges(
	ctx context.Context,
	entityID string,
	allowed map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge, string) (bool, error),
) (bool, error) {
	neighbors, err := l.graph.FilteredNeighbors(
		entityID, "both", allowed, nil, false, nil,
	)
	if err != nil {
		return false, err
	}
	for _, neighbor := range neighbors {
		if startEdgeID != "" && neighbor.Edge.ID < startEdgeID {
			continue
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		keepGoing, err := visit(neighbor.Edge, neighbor.Direction)
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func (l *graphParityLookup) GetEntity(
	_ context.Context,
	id string,
	_ []string,
) (graph.Entity, bool, error) {
	entity, ok := l.graph.GetEntity(id)
	return entity, ok, nil
}

func (l *graphParityLookup) VisitEntities(
	ctx context.Context,
	kind string,
	_ []string,
	afterID string,
	visit func(graph.Entity) (bool, error),
) (bool, error) {
	ids := make([]string, 0, len(l.graph.Entities))
	for id, entity := range l.graph.Entities {
		if kind == "" || entity.Kind == kind {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		if afterID != "" && id < afterID {
			continue
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		keepGoing, err := visit(l.graph.Entities[id])
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func (l *graphParityLookup) edges(
	entityID string,
	direction string,
	allowed map[string]struct{},
) []graph.Edge {
	neighbors, _ := l.graph.FilteredNeighbors(
		entityID, direction, allowed, nil, false, nil,
	)
	edges := make([]graph.Edge, 0, len(neighbors))
	for _, neighbor := range neighbors {
		edges = append(edges, neighbor.Edge)
	}
	return edges
}

func visitParityEdges(
	ctx context.Context,
	edges []graph.Edge,
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	for _, edge := range edges {
		if startEdgeID != "" && edge.ID < startEdgeID {
			continue
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		keepGoing, err := visit(edge)
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}
