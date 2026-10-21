package storage

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

// A cursor retains only the current immutable physical page. Moving a query
// cursor never requires decoding the preceding pages of a logical bucket.
type rangeEdgeCursor struct {
	lookup                     *PersistedIndexLookup
	spec                       EdgeShard
	direction, endpoint, start string
	object, offset             int
	rows                       []graph.Edge
	current                    graph.Edge
	legacyLoaded               bool
	adjacency                  *adjacencyIterator
	adjacencyReady             bool
	payloadObject              int
}

func (s *TenantStore) borrowEdgeRange(ctx context.Context, tenantID string, version int64, spec EdgeShard, n int) ([]graph.Edge, bool, error) {
	object := spec.Objects[n]
	partSpec := edgeRangeSpec(spec, object)
	key := edgeLookupCacheKey(tenantID, version, partSpec, "range_rows") + "\x00" + object.Key
	decoded, ok, err := s.edgeLookupCache.load(ctx, key, func() (map[string][]graph.Edge, bool, error) {
		part, ok, err := s.loadParquetEdgeShardObject(ctx, tenantID, version, partSpec)
		if err != nil || !ok {
			return nil, false, err
		}
		if !edgeShardObjectMatchesCatalog(part, partSpec, version) || !indexTenantMatches(part.TenantID, tenantID) {
			return nil, false, fmt.Errorf("%w: invalid index edge range", ErrNotFound)
		}
		return map[string][]graph.Edge{"": part.Edges}, true, nil
	})
	if err != nil || !ok {
		return nil, false, err
	}
	rows := decoded[""]
	routing, _ := decodeIndexRange(object.Role)
	for _, edge := range rows {
		if !routing.contains(edge.ID) {
			return nil, false, fmt.Errorf("%w: edge is outside its index range", ErrNotFound)
		}
	}
	return rows, true, nil
}

func (c *rangeEdgeCursor) advance(ctx context.Context) (bool, error) {
	if c.spec.AdjacencyRoot != nil {
		if !c.adjacencyReady {
			c.adjacencyReady = true
			root := c.spec.AdjacencyRoot
			// Dense buckets already have exactly the required ID order. Validate
			// the root once, then avoid reading a duplicate stream of IDs.
			if root.First.Endpoint == c.endpoint && root.Last.Endpoint == c.endpoint {
				if _, err := c.lookup.Store.loadAdjacencyNode(ctx, c.lookup.TenantID, *root); err != nil {
					return false, fmt.Errorf("adjacency root: %v", err)
				}
			} else {
				var err error
				c.adjacency, err = c.lookup.Store.seekAdjacency(ctx, c.lookup.TenantID, *root, adjacencyKey{c.endpoint, c.start})
				if err != nil {
					return false, fmt.Errorf("adjacency seek: %v", err)
				}
				c.payloadObject = -1
			}
		}
		if c.adjacency != nil {
			return c.advanceAdjacency(ctx)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		for c.offset < len(c.rows) {
			edge := c.rows[c.offset]
			c.offset++
			if (c.direction == "out" && edge.From == c.endpoint) || (c.direction == "in" && edge.To == c.endpoint) {
				c.current = edge
				return true, nil
			}
			if err := query.ConsumeIndexScanCost(ctx, 1); err != nil {
				return false, err
			}
		}
		c.rows = nil
		if !hasIndexRanges(c.spec.Objects) {
			if c.legacyLoaded {
				return false, nil
			}
			c.legacyLoaded = true
			var byEndpoint map[string][]graph.Edge
			var ok bool
			var err error
			if c.direction == "out" {
				byEndpoint, ok, err = c.lookup.cachedParquetOutEdgeMap(ctx, c.spec)
			} else {
				byEndpoint, ok, err = c.lookup.cachedParquetInEdgeMap(ctx, c.spec)
			}
			if err != nil {
				return false, err
			}
			if !ok {
				return false, ErrNotFound
			}
			c.rows = byEndpoint[c.endpoint]
		} else {
			if c.object >= len(c.spec.Objects) {
				return false, nil
			}
			rows, ok, err := c.lookup.Store.borrowEdgeRange(ctx, c.lookup.TenantID, c.lookup.Version, c.spec, c.object)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, ErrNotFound
			}
			c.object++
			c.rows = rows
		}
		c.offset = sort.Search(len(c.rows), func(i int) bool { return c.rows[i].ID >= c.start })
	}
}

func (c *rangeEdgeCursor) advanceAdjacency(ctx context.Context) (bool, error) {
	id, ok, err := c.adjacency.next(ctx)
	if err != nil {
		return false, fmt.Errorf("adjacency read: %v", err)
	}
	if !ok {
		return false, nil
	}
	if err := query.ConsumeIndexScanCost(ctx, 1); err != nil {
		return false, err
	}
	n := rangeObjectIndex(c.spec.Objects, id)
	if n != c.payloadObject {
		c.rows, ok, err = c.lookup.Store.borrowEdgeRange(ctx, c.lookup.TenantID, c.lookup.Version, c.spec, n)
		if err != nil || !ok {
			return false, fmt.Errorf("adjacency payload unavailable: %v", err)
		}
		c.payloadObject = n
	}
	i := sort.Search(len(c.rows), func(i int) bool { return c.rows[i].ID >= id })
	if i == len(c.rows) || c.rows[i].ID != id {
		return false, fmt.Errorf("adjacency payload ID mismatch")
	}
	e := c.rows[i]
	if c.direction == "out" && e.From != c.endpoint || c.direction == "in" && e.To != c.endpoint {
		return false, fmt.Errorf("adjacency endpoint mismatch")
	}
	c.current = e
	return true, nil
}

type rangeEdgeHeap []*rangeEdgeCursor

func (h rangeEdgeHeap) Len() int { return len(h) }
func (h rangeEdgeHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.current.ID != b.current.ID {
		return a.current.ID < b.current.ID
	}
	x, y := bothEdgeNeighborID(a.current, a.direction), bothEdgeNeighborID(b.current, b.direction)
	if x != y {
		return x < y
	}
	return a.direction < b.direction
}
func (h rangeEdgeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *rangeEdgeHeap) Push(v any)   { *h = append(*h, v.(*rangeEdgeCursor)) }
func (h *rangeEdgeHeap) Pop() any {
	old := *h
	n := len(old) - 1
	v := old[n]
	old[n] = nil
	*h = old[:n]
	return v
}

func (l *PersistedIndexLookup) visitRangeEdges(ctx context.Context, endpoint string, allowed map[string]struct{}, start, direction string, visit func(graph.Edge, string) (bool, error)) (bool, error) {
	if err := l.compileCatalog(); err != nil {
		for _, spec := range l.Catalog.EdgeShards {
			if spec.AdjacencyRoot != nil {
				return false, err
			}
		}
		return false, nil
	}
	if direction != "out" {
		l.reverseOnce.Do(func() {
			_, l.reverseErr = l.Store.compiledScanCatalog(l.TenantID, IndexCatalog{Version: l.Version, EdgeShards: l.ReverseCatalog.EdgeShards}, "")
		})
		if l.reverseErr != nil {
			for _, spec := range l.ReverseCatalog.EdgeShards {
				if spec.AdjacencyRoot != nil {
					return false, l.reverseErr
				}
			}
			return false, nil
		}
	}
	var queue rangeEdgeHeap
	directions := []string{direction}
	if direction == "both" {
		directions = []string{"out", "in"}
	}
	for _, dir := range directions {
		for _, shard := range indexShardIDCandidates(endpoint) {
			types := l.relationTypesForShard(shard, allowed)
			if dir == "in" {
				types = l.reverseRelationTypesForShard(shard, allowed)
			}
			for _, relation := range types {
				spec, ok := l.catalogEdgeShardSpec(relation, shard)
				if dir == "in" {
					spec, ok = l.reverseEdgeShardSpec(relation, shard)
				}
				if !ok || specFormat(spec.Format) != IndexFormatParquet {
					return false, nil
				}
				cursor := &rangeEdgeCursor{lookup: l, spec: spec, direction: dir, endpoint: endpoint, start: start}
				if hasIndexRanges(spec.Objects) {
					cursor.object = rangeObjectIndex(spec.Objects, start)
				}
				found, err := cursor.advance(ctx)
				if errors.Is(err, ErrNotFound) {
					if cursor.spec.AdjacencyRoot != nil {
						return false, fmt.Errorf("adjacency payload unavailable: %v", err)
					}
					return false, nil
				}
				if err != nil {
					return false, err
				}
				if found {
					queue = append(queue, cursor)
				}
			}
		}
	}
	heap.Init(&queue)
	last := ""
	for len(queue) > 0 {
		cursor := heap.Pop(&queue).(*rangeEdgeCursor)
		key := cursor.direction + "\x00" + cursor.current.ID
		if key != last {
			last = key
			keep, err := visit(graph.CopyEdge(cursor.current), cursor.direction)
			if err != nil || !keep {
				return true, err
			}
		}
		found, err := cursor.advance(ctx)
		if errors.Is(err, ErrNotFound) {
			if cursor.spec.AdjacencyRoot != nil {
				return false, fmt.Errorf("adjacency payload unavailable after traversal: %v", err)
			}
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if found {
			heap.Push(&queue, cursor)
		}
	}
	return true, nil
}
