package storage

import (
	"container/heap"
	"context"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (l *PersistedIndexLookup) VisitBothEdges(
	ctx context.Context,
	entityID string,
	allowed map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge, string) (bool, error),
) (bool, error) {
	if l == nil || l.Catalog.Version != l.Version ||
		l.ReverseCatalog == nil ||
		l.ReverseCatalog.Version != l.Version {
		return false, nil
	}
	out, ok, err := l.outEdgeVisitSlices(ctx, entityID, allowed)
	if err != nil || !ok {
		return ok, err
	}
	in, ok, err := l.inEdgeVisitSlices(ctx, entityID, allowed)
	if err != nil || !ok {
		return ok, err
	}
	return visitSortedBothEdgeSlices(
		ctx, out, in, startEdgeID, visit,
	)
}

func visitSortedBothEdgeSlices(
	ctx context.Context,
	out [][]graph.Edge,
	in [][]graph.Edge,
	startEdgeID string,
	visit func(graph.Edge, string) (bool, error),
) (bool, error) {
	queue := make(bothEdgeVisitHeap, 0, len(out)+len(in))
	queue = appendBothEdgeVisitCursors(
		queue, out, "out", startEdgeID, 0,
	)
	queue = appendBothEdgeVisitCursors(
		queue, in, "in", startEdgeID, len(out),
	)
	heap.Init(&queue)
	lastKey := ""
	for queue.Len() > 0 {
		if err := objectContextErr(ctx); err != nil {
			return false, err
		}
		cursor := heap.Pop(&queue).(bothEdgeVisitCursor)
		edge := cursor.edges[cursor.index]
		cursor.index++
		if cursor.index < len(cursor.edges) {
			heap.Push(&queue, cursor)
		}
		key := cursor.direction + "\x00" + edge.ID
		if key == lastKey {
			continue
		}
		lastKey = key
		keepGoing, err := visit(
			graph.CopyEdge(edge), cursor.direction,
		)
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func appendBothEdgeVisitCursors(
	queue bothEdgeVisitHeap,
	slices [][]graph.Edge,
	direction string,
	startEdgeID string,
	orderOffset int,
) bothEdgeVisitHeap {
	for order, edges := range slices {
		index := 0
		if startEdgeID != "" {
			index = sort.Search(len(edges), func(index int) bool {
				return edges[index].ID >= startEdgeID
			})
		}
		if index < len(edges) {
			queue = append(queue, bothEdgeVisitCursor{
				edges:     edges,
				index:     index,
				order:     orderOffset + order,
				direction: direction,
			})
		}
	}
	return queue
}

type bothEdgeVisitCursor struct {
	edges     []graph.Edge
	index     int
	order     int
	direction string
}

type bothEdgeVisitHeap []bothEdgeVisitCursor

func (h bothEdgeVisitHeap) Len() int { return len(h) }

func (h bothEdgeVisitHeap) Less(left int, right int) bool {
	leftCursor, rightCursor := h[left], h[right]
	leftEdge := leftCursor.edges[leftCursor.index]
	rightEdge := rightCursor.edges[rightCursor.index]
	if leftEdge.ID != rightEdge.ID {
		return leftEdge.ID < rightEdge.ID
	}
	leftEntity := bothEdgeNeighborID(leftEdge, leftCursor.direction)
	rightEntity := bothEdgeNeighborID(rightEdge, rightCursor.direction)
	if leftEntity != rightEntity {
		return leftEntity < rightEntity
	}
	if leftCursor.direction != rightCursor.direction {
		return leftCursor.direction < rightCursor.direction
	}
	return leftCursor.order < rightCursor.order
}

func (h bothEdgeVisitHeap) Swap(left int, right int) {
	h[left], h[right] = h[right], h[left]
}

func (h *bothEdgeVisitHeap) Push(value any) {
	*h = append(*h, value.(bothEdgeVisitCursor))
}

func (h *bothEdgeVisitHeap) Pop() any {
	old := *h
	index := len(old) - 1
	value := old[index]
	old[index] = bothEdgeVisitCursor{}
	*h = old[:index]
	return value
}

func bothEdgeNeighborID(edge graph.Edge, direction string) string {
	if direction == "in" {
		return edge.From
	}
	return edge.To
}
