package query

import (
	"container/heap"
	"sort"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type boundedEntities struct {
	keep int
	heap boundedEntityHeap
}

type boundedEntityHeap struct {
	items []graph.Entity
	specs []SortSpec
}

func newBoundedEntities(specs []SortSpec, keep int) *boundedEntities {
	if keep < 0 {
		keep = 0
	}
	return &boundedEntities{
		keep: keep,
		heap: boundedEntityHeap{items: make([]graph.Entity, 0, keep), specs: specs},
	}
}

func (entities *boundedEntities) Add(entity graph.Entity) {
	if entities == nil || entities.keep == 0 {
		return
	}
	if entities.heap.Len() < entities.keep {
		heap.Push(&entities.heap, entity)
		return
	}
	if compareEntities(entity, entities.heap.items[0], entities.heap.specs) >= 0 {
		return
	}
	entities.heap.items[0] = entity
	heap.Fix(&entities.heap, 0)
}

func (entities *boundedEntities) Len() int {
	if entities == nil {
		return 0
	}
	return entities.heap.Len()
}

func (entities *boundedEntities) Sorted() []graph.Entity {
	if entities == nil {
		return nil
	}
	sort.SliceStable(entities.heap.items, func(i, j int) bool {
		return compareEntities(entities.heap.items[i], entities.heap.items[j], entities.heap.specs) < 0
	})
	return entities.heap.items
}

func compareEntities(left, right graph.Entity, specs []SortSpec) int {
	for _, spec := range specs {
		cmp := compareAny(entityValue(left, spec.Field), entityValue(right, spec.Field))
		if cmp == 0 {
			continue
		}
		if spec.Desc {
			return -cmp
		}
		return cmp
	}
	return strings.Compare(left.ID, right.ID)
}

func (h boundedEntityHeap) Len() int { return len(h.items) }

func (h boundedEntityHeap) Less(i, j int) bool {
	return compareEntities(h.items[i], h.items[j], h.specs) > 0
}

func (h boundedEntityHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *boundedEntityHeap) Push(value any) {
	h.items = append(h.items, value.(graph.Entity))
}

func (h *boundedEntityHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items[last] = graph.Entity{}
	h.items = h.items[:last]
	return value
}
