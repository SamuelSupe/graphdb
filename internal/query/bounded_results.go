package query

import "container/heap"

type boundedResults struct {
	keep int
	heap boundedResultHeap
}

type boundedResultHeap struct {
	items []Result
	specs []SortSpec
}

func newBoundedResults(specs []SortSpec, keep int) *boundedResults {
	if keep < 0 {
		keep = 0
	}
	return &boundedResults{
		keep: keep,
		heap: boundedResultHeap{items: make([]Result, 0, keep), specs: specs},
	}
}

func (results *boundedResults) Add(result Result) {
	if results == nil || results.keep == 0 {
		return
	}
	if results.heap.Len() < results.keep {
		heap.Push(&results.heap, result)
		return
	}
	if compareResults(result, results.heap.items[0], results.heap.specs) >= 0 {
		return
	}
	results.heap.items[0] = result
	heap.Fix(&results.heap, 0)
}

func (results *boundedResults) Len() int {
	if results == nil {
		return 0
	}
	return results.heap.Len()
}

func (results *boundedResults) Sorted() []Result {
	if results == nil {
		return nil
	}
	sortResults(results.heap.items, results.heap.specs)
	return results.heap.items
}

func (h boundedResultHeap) Len() int { return len(h.items) }

func (h boundedResultHeap) Less(i, j int) bool {
	return compareResults(h.items[i], h.items[j], h.specs) > 0
}

func (h boundedResultHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *boundedResultHeap) Push(value any) {
	h.items = append(h.items, value.(Result))
}

func (h *boundedResultHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items[last] = Result{}
	h.items = h.items[:last]
	return value
}
