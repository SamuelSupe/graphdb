package storage

import (
	"container/heap"
	"context"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (l *PersistedIndexLookup) VisitOutEdges(
	ctx context.Context,
	from string,
	allowed map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return false, nil
	}
	slices, ok, err := l.outEdgeVisitSlices(ctx, from, allowed)
	if err != nil || !ok {
		return ok, err
	}
	return visitSortedEdgeSlices(ctx, slices, startEdgeID, visit)
}

func (l *PersistedIndexLookup) VisitInEdges(
	ctx context.Context,
	to string,
	allowed map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	if l == nil || l.ReverseCatalog == nil ||
		l.ReverseCatalog.Version != l.Version {
		return false, nil
	}
	slices, ok, err := l.inEdgeVisitSlices(ctx, to, allowed)
	if err != nil || !ok {
		return ok, err
	}
	return visitSortedEdgeSlices(ctx, slices, startEdgeID, visit)
}

func visitSortedEdgeSlices(
	ctx context.Context,
	slices [][]graph.Edge,
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	queue := make(outEdgeVisitHeap, 0, len(slices))
	for order, edges := range slices {
		index := 0
		if startEdgeID != "" {
			index = sort.Search(len(edges), func(index int) bool {
				return edges[index].ID >= startEdgeID
			})
		}
		if index < len(edges) {
			queue = append(queue, outEdgeVisitCursor{
				edges: edges, index: index, order: order,
			})
		}
	}
	heap.Init(&queue)
	lastID := ""
	for queue.Len() > 0 {
		if err := objectContextErr(ctx); err != nil {
			return false, err
		}
		cursor := heap.Pop(&queue).(outEdgeVisitCursor)
		edge := cursor.edges[cursor.index]
		cursor.index++
		if cursor.index < len(cursor.edges) {
			heap.Push(&queue, cursor)
		}
		if edge.ID == lastID {
			continue
		}
		lastID = edge.ID
		keepGoing, err := visit(graph.CopyEdge(edge))
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func (l *PersistedIndexLookup) outEdgeVisitSlices(
	ctx context.Context,
	from string,
	allowed map[string]struct{},
) ([][]graph.Edge, bool, error) {
	var slices [][]graph.Edge
	for _, shardID := range indexShardIDCandidates(from) {
		relationTypes := l.relationTypesForShard(shardID, allowed)
		for index, relationType := range relationTypes {
			if index == 0 {
				continue
			}
			spec, ok := l.catalogEdgeShardSpec(relationType, shardID)
			if !ok || specFormat(spec.Format) != IndexFormatParquet {
				return nil, false, nil
			}
			l.Store.prefetchParquetEdgeShardObject(
				ctx, l.TenantID, l.Version, spec,
			)
		}
		for _, relationType := range relationTypes {
			spec, ok := l.catalogEdgeShardSpec(relationType, shardID)
			if !ok || specFormat(spec.Format) != IndexFormatParquet {
				return nil, false, nil
			}
			byFrom, ok, err := l.cachedParquetOutEdgeMap(ctx, spec)
			if err != nil {
				if ctx.Err() != nil {
					return nil, false, err
				}
				return nil, false, nil
			}
			if !ok {
				return nil, false, nil
			}
			if edges := byFrom[from]; len(edges) > 0 {
				slices = append(slices, edges)
			}
		}
	}
	return slices, true, nil
}

func (l *PersistedIndexLookup) inEdgeVisitSlices(
	ctx context.Context,
	to string,
	allowed map[string]struct{},
) ([][]graph.Edge, bool, error) {
	var slices [][]graph.Edge
	for _, shardID := range indexShardIDCandidates(to) {
		relationTypes := l.reverseRelationTypesForShard(shardID, allowed)
		for index, relationType := range relationTypes {
			if index == 0 {
				continue
			}
			spec, ok := l.reverseEdgeShardSpec(relationType, shardID)
			if !ok || specFormat(spec.Format) != IndexFormatParquet {
				return nil, false, nil
			}
			l.Store.prefetchParquetEdgeShardObject(
				ctx, l.TenantID, l.Version, spec,
			)
		}
		for _, relationType := range relationTypes {
			spec, ok := l.reverseEdgeShardSpec(relationType, shardID)
			if !ok || specFormat(spec.Format) != IndexFormatParquet {
				return nil, false, nil
			}
			byTo, ok, err := l.cachedParquetInEdgeMap(ctx, spec)
			if err != nil {
				if ctx.Err() != nil {
					return nil, false, err
				}
				return nil, false, nil
			}
			if !ok {
				return nil, false, nil
			}
			if edges := byTo[to]; len(edges) > 0 {
				slices = append(slices, edges)
			}
		}
	}
	return slices, true, nil
}

type outEdgeVisitCursor struct {
	edges []graph.Edge
	index int
	order int
}

type outEdgeVisitHeap []outEdgeVisitCursor

func (h outEdgeVisitHeap) Len() int { return len(h) }

func (h outEdgeVisitHeap) Less(left int, right int) bool {
	leftID := h[left].edges[h[left].index].ID
	rightID := h[right].edges[h[right].index].ID
	if leftID == rightID {
		return h[left].order < h[right].order
	}
	return leftID < rightID
}

func (h outEdgeVisitHeap) Swap(left int, right int) {
	h[left], h[right] = h[right], h[left]
}

func (h *outEdgeVisitHeap) Push(value any) {
	*h = append(*h, value.(outEdgeVisitCursor))
}

func (h *outEdgeVisitHeap) Pop() any {
	old := *h
	index := len(old) - 1
	value := old[index]
	old[index] = outEdgeVisitCursor{}
	*h = old[:index]
	return value
}
