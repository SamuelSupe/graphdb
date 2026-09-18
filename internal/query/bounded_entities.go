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
	keys  []any
}

const maxCachedEntitySortKeys = 4096

func newBoundedEntities(specs []SortSpec, keep int) *boundedEntities {
	if keep < 0 {
		keep = 0
	}
	var keys []any
	if len(specs) > 0 && keep <= maxCachedEntitySortKeys {
		keys = make([]any, 0, keep)
	}
	return &boundedEntities{
		keep: keep,
		heap: boundedEntityHeap{
			items: make([]graph.Entity, 0, keep),
			specs: specs,
			keys:  keys,
		},
	}
}

func (entities *boundedEntities) Add(entity *graph.Entity) {
	if entities == nil || entities.keep == 0 {
		return
	}
	if entities.heap.Len() < entities.keep {
		heap.Push(&entities.heap, *entity)
		return
	}
	if entities.heap.compareWithRoot(entity) >= 0 {
		return
	}
	entities.heap.items[0] = *entity
	if entities.heap.keys != nil {
		entities.heap.keys[0] = entityValue(entity, entities.heap.specs[0].Field)
	}
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
	sort.Stable(sort.Reverse(&entities.heap))
	return entities.heap.items
}

func (h boundedEntityHeap) Len() int { return len(h.items) }

func (h boundedEntityHeap) Less(i, j int) bool {
	for k, spec := range h.specs {
		cmp := compareAny(h.sortValue(i, k), h.sortValue(j, k))
		if cmp != 0 {
			if spec.Desc {
				return cmp < 0
			}
			return cmp > 0
		}
	}
	return h.items[i].ID > h.items[j].ID
}

func (h boundedEntityHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	if h.keys != nil {
		h.keys[i], h.keys[j] = h.keys[j], h.keys[i]
	}
}

func (h *boundedEntityHeap) Push(value any) {
	h.items = append(h.items, value.(graph.Entity))
	if h.keys != nil {
		h.keys = append(h.keys, entityValue(&h.items[len(h.items)-1], h.specs[0].Field))
	}
}

func (h *boundedEntityHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items[last] = graph.Entity{}
	h.items = h.items[:last]
	if h.keys != nil {
		h.keys[last] = nil
		h.keys = h.keys[:last]
	}
	return value
}

// The first sort key is used by every comparison. Bound its request-local
// cache independently of deep pagination; other keys still resolve on demand.
func (h boundedEntityHeap) sortValue(index, specIndex int) any {
	if specIndex == 0 && h.keys != nil {
		return h.keys[index]
	}
	return entityValue(&h.items[index], h.specs[specIndex].Field)
}

func (h boundedEntityHeap) compareWithRoot(entity *graph.Entity) int {
	for i, spec := range h.specs {
		cmp := compareAny(entityValue(entity, spec.Field), h.sortValue(0, i))
		if cmp == 0 {
			continue
		}
		if spec.Desc {
			return -cmp
		}
		return cmp
	}
	return strings.Compare(entity.ID, h.items[0].ID)
}
