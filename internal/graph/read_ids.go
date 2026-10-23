package graph

import (
	"container/heap"
	"maps"
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
		entity, ok := g.Entities.Get(id)
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
	if ids, ok := g.entityOrder[kind]; ok {
		g.entityOrderMu.Unlock()
		return ids
	}
	g.entityOrderMu.Unlock()
	ids := make([]string, 0, g.KindCount(kind))
	if kind == "" {
		for id := range g.Entities.Keys() {
			ids = append(ids, id)
		}
	} else if g.kindEntityIDs.Len() == 0 && g.Entities.Len() > 0 {
		// Raw, deliberately unindexed graphs remain a supported scan fallback.
		for id, entity := range g.Entities.All() {
			if entity.Kind == kind {
				ids = append(ids, id)
			}
		}
	} else {
		for id := range g.kindEntityIDs.At(kind).Keys() {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	g.entityOrderMu.Lock()
	defer g.entityOrderMu.Unlock()
	if existing, ok := g.entityOrder[kind]; ok {
		return existing
	}
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
	g.fieldValueOrder = nil
	g.fieldIndexOrderMu.Unlock()
}

func (g *Graph) inheritReadOrder(source *Graph) {
	source.neighborOrderMu.Lock()
	neighborOrder := maps.Clone(source.neighborOrder)
	neighborOrderBytes := source.neighborOrderBytes
	source.neighborOrderMu.Unlock()
	source.entityOrderMu.Lock()
	entityOrder := maps.Clone(source.entityOrder)
	source.entityOrderMu.Unlock()
	source.fieldIndexOrderMu.Lock()
	fieldOrder := maps.Clone(source.fieldIndexOrder)
	valueOrder := maps.Clone(source.fieldValueOrder)
	source.fieldIndexOrderMu.Unlock()
	// Cached slices are immutable; only the maps need their own ownership.
	g.entityOrderMu.Lock()
	g.entityOrder = entityOrder
	g.entityOrderMu.Unlock()
	g.fieldIndexOrderMu.Lock()
	g.fieldIndexOrder = fieldOrder
	g.fieldValueOrder = valueOrder
	g.fieldIndexOrderMu.Unlock()
	g.neighborOrderMu.Lock()
	g.neighborOrder = neighborOrder
	g.neighborOrderBytes = neighborOrderBytes
	g.neighborOrderMu.Unlock()
}

// HasEntityOrder reports whether visiting a kind can reuse its sorted IDs.
func (g *Graph) HasEntityOrder(kind string) bool {
	g.entityOrderMu.Lock()
	_, ok := g.entityOrder[kind]
	g.entityOrderMu.Unlock()
	return ok
}

func (g *Graph) invalidateFieldKeyOrder(kind, field string) {
	g.fieldIndexOrderMu.Lock()
	delete(g.fieldValueOrder, fieldValueOrderKey{kind: kind, field: field})
	g.fieldIndexOrderMu.Unlock()
}

func (g *Graph) invalidateEntityKindOrder(kind string) {
	g.entityOrderMu.Lock()
	delete(g.entityOrder, "")
	delete(g.entityOrder, kind)
	g.entityOrderMu.Unlock()
}

func (g *Graph) invalidateFieldValueOrder(kind, field, value string) {
	g.fieldIndexOrderMu.Lock()
	delete(g.fieldIndexOrder, fieldIndexOrderKey{kind: kind, field: field, value: value})
	g.fieldIndexOrderMu.Unlock()
}

func (g *Graph) sortedFieldIndexIDs(kind, field, value string) []string {
	valueIDs := g.fieldIndex[kind][field].At(value)
	if valueIDs.Len() < minCachedFieldIndexOrder {
		return sortedFieldIDs(valueIDs)
	}

	cacheKey := fieldIndexOrderKey{kind: kind, field: field, value: value}
	g.fieldIndexOrderMu.Lock()
	if ids, ok := g.fieldIndexOrder[cacheKey]; ok {
		g.fieldIndexOrderMu.Unlock()
		return ids
	}
	g.fieldIndexOrderMu.Unlock()
	ids := sortedFieldIDs(valueIDs)
	g.fieldIndexOrderMu.Lock()
	defer g.fieldIndexOrderMu.Unlock()
	if existing, ok := g.fieldIndexOrder[cacheKey]; ok {
		return existing
	}
	if g.fieldIndexOrder == nil {
		g.fieldIndexOrder = map[fieldIndexOrderKey][]string{}
	}
	g.fieldIndexOrder[cacheKey] = ids
	return ids
}

func sortedFieldIDs(values *ShardedMap[struct{}]) []string {
	ids := make([]string, 0, values.Len())
	for id := range values.Keys() {
		ids = append(ids, id)
	}
	sort.Strings(ids)
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
		count += g.fieldIndex[kind][field].At(key).Len()
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

// VisitSortedIDs merges immutable sorted postings, seeking each list to startID
// (inclusive) and releasing no result-sized copies. Duplicate IDs are visited once.
func VisitSortedIDs(buckets [][]string, startID string, visit func(string) (bool, error)) error {
	if visit == nil {
		return nil
	}
	queue := make(fieldIndexIDHeap, 0, len(buckets))
	for _, ids := range buckets {
		offset := sort.SearchStrings(ids, startID)
		if offset < len(ids) {
			queue = append(queue, fieldIndexIDCursor{ids: ids, offset: offset})
		}
	}
	heap.Init(&queue)
	last := ""
	for queue.Len() > 0 {
		cursor := heap.Pop(&queue).(fieldIndexIDCursor)
		id := cursor.ids[cursor.offset]
		cursor.offset++
		if cursor.offset < len(cursor.ids) {
			heap.Push(&queue, cursor)
		}
		if id == last {
			continue
		}
		last = id
		more, err := visit(id)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

// VisitMatchingFieldIDs lends matching IDs in identity order from startID,
// including that ID so callers can validate cursor membership.
func (g *Graph) VisitMatchingFieldIDs(kind, field string, values []any, startID string, visit func(string) (bool, error)) error {
	var buckets [][]string
	for _, key := range distinctScalarKeys(values) {
		buckets = append(buckets, g.sortedFieldIndexIDs(kind, field, key))
	}
	return VisitSortedIDs(buckets, startID, visit)
}

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
	for value, valueIDs := range g.fieldIndex[kind][field].All() {
		matched, err := match(value)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		for id := range valueIDs.Keys() {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}
