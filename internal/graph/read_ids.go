package graph

import (
	"container/heap"
	"sort"
)

type fieldIndexOrderKey struct {
	kind  string
	field string
	value string
}

// Avoid retaining a cache entry for point-lookup buckets that are cheaper to
// sort than to keep for the lifetime of the graph snapshot.
const minCachedFieldIndexOrder = 32

func (g *Graph) MatchEntityIDs(kind string) []string {
	ids := g.sortedEntityIDs(kind)
	return append([]string(nil), ids...)
}

// VisitEntitiesByID visits the selected kind in stable ID order. The callback
// must treat the entity as read-only and must not retain it after returning.
func (g *Graph) VisitEntitiesByID(kind string, afterID string, visit func(Entity) (bool, error)) error {
	if visit == nil {
		return nil
	}
	ids := g.sortedEntityIDs(kind)
	start := 0
	if afterID != "" {
		start = sort.SearchStrings(ids, afterID)
		for start < len(ids) && ids[start] <= afterID {
			start++
		}
	}
	for _, id := range ids[start:] {
		entity, ok := g.Entities[id]
		if !ok {
			continue
		}
		keepGoing, err := visit(entity)
		if err != nil || !keepGoing {
			return err
		}
	}
	return nil
}

func (g *Graph) sortedEntityIDs(kind string) []string {
	g.entityOrderMu.Lock()
	defer g.entityOrderMu.Unlock()
	if ids, ok := g.entityOrder[kind]; ok {
		return ids
	}
	ids := make([]string, 0, g.KindCount(kind))
	for id, entity := range g.Entities {
		if kind == "" || entity.Kind == kind {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if g.entityOrder == nil {
		g.entityOrder = map[string][]string{}
	}
	g.entityOrder[kind] = ids
	return ids
}

func (g *Graph) invalidateEntityOrder() {
	g.entityOrderMu.Lock()
	g.entityOrder = nil
	g.entityOrderMu.Unlock()
}

func (g *Graph) invalidateFieldIndexOrder() {
	g.fieldIndexOrderMu.Lock()
	g.fieldIndexOrder = nil
	g.fieldIndexOrderMu.Unlock()
}

func (g *Graph) sortedFieldIndexIDs(kind, field, value string) []string {
	valueIDs := g.fieldIndex[kind][field][value]
	if len(valueIDs) < minCachedFieldIndexOrder {
		return sortedKeys(valueIDs)
	}

	cacheKey := fieldIndexOrderKey{kind: kind, field: field, value: value}
	g.fieldIndexOrderMu.Lock()
	defer g.fieldIndexOrderMu.Unlock()
	if ids, ok := g.fieldIndexOrder[cacheKey]; ok {
		return ids
	}
	ids := sortedKeys(valueIDs)
	if g.fieldIndexOrder == nil {
		g.fieldIndexOrder = map[fieldIndexOrderKey][]string{}
	}
	g.fieldIndexOrder[cacheKey] = ids
	return ids
}

func (g *Graph) MatchFieldIndexIDs(kind string, field string, values []any) []string {
	keys := distinctScalarKeys(values)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return append([]string(nil), g.sortedFieldIndexIDs(kind, field, keys[0])...)
	}
	count := 0
	for _, key := range keys {
		count += len(g.fieldIndex[kind][field][key])
	}
	ids := make([]string, 0, count)
	for _, key := range keys {
		ids = append(ids, g.sortedFieldIndexIDs(kind, field, key)...)
	}
	sort.Strings(ids)
	return ids
}

// VisitFieldIndexIDs visits matching IDs in the same stable order returned by
// MatchFieldIndexIDs without allocating a result-sized slice for each query.
func (g *Graph) VisitFieldIndexIDs(kind string, field string, values []any, visit func(string) error) (int, error) {
	if visit == nil {
		return 0, nil
	}
	keys := distinctScalarKeys(values)
	buckets := make([][]string, 0, len(keys))
	for _, key := range keys {
		if ids := g.sortedFieldIndexIDs(kind, field, key); len(ids) > 0 {
			buckets = append(buckets, ids)
		}
	}
	if len(buckets) == 0 {
		return 0, nil
	}
	if len(buckets) == 1 {
		for index, id := range buckets[0] {
			if err := visit(id); err != nil {
				return index + 1, err
			}
		}
		return len(buckets[0]), nil
	}
	if len(buckets) == 2 {
		left, right, visited := 0, 0, 0
		for left < len(buckets[0]) || right < len(buckets[1]) {
			var id string
			if right >= len(buckets[1]) ||
				(left < len(buckets[0]) && buckets[0][left] < buckets[1][right]) {
				id = buckets[0][left]
				left++
			} else {
				id = buckets[1][right]
				right++
			}
			visited++
			if err := visit(id); err != nil {
				return visited, err
			}
		}
		return visited, nil
	}

	queue := make(fieldIndexIDHeap, len(buckets))
	for index, ids := range buckets {
		queue[index] = fieldIndexIDCursor{ids: ids}
	}
	heap.Init(&queue)
	visited := 0
	for queue.Len() > 0 {
		cursor := heap.Pop(&queue).(fieldIndexIDCursor)
		id := cursor.ids[cursor.offset]
		visited++
		if err := visit(id); err != nil {
			return visited, err
		}
		cursor.offset++
		if cursor.offset < len(cursor.ids) {
			heap.Push(&queue, cursor)
		}
	}
	return visited, nil
}

type fieldIndexIDCursor struct {
	ids    []string
	offset int
}

type fieldIndexIDHeap []fieldIndexIDCursor

func (values fieldIndexIDHeap) Len() int { return len(values) }

func (values fieldIndexIDHeap) Less(left, right int) bool {
	return values[left].ids[values[left].offset] < values[right].ids[values[right].offset]
}

func (values fieldIndexIDHeap) Swap(left, right int) {
	values[left], values[right] = values[right], values[left]
}

func (values *fieldIndexIDHeap) Push(value any) {
	*values = append(*values, value.(fieldIndexIDCursor))
}

func (values *fieldIndexIDHeap) Pop() any {
	items := *values
	last := len(items) - 1
	value := items[last]
	items[last] = fieldIndexIDCursor{}
	*values = items[:last]
	return value
}

func (g *Graph) ScanFieldIndexIDs(
	kind string,
	field string,
	match func(string) (bool, error),
) ([]string, error) {
	if match == nil {
		return nil, nil
	}
	ids := make([]string, 0)
	for value, valueIDs := range g.fieldIndex[kind][field] {
		matched, err := match(value)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		for id := range valueIDs {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}
