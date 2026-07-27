package storage

import (
	"container/heap"
	"context"
	"sort"
)

func (s *TenantStore) deadLettersForReplay(
	ctx context.Context,
	tenantID string,
	source string,
	limit int,
) ([]DeadLetter, error) {
	if limit <= 0 {
		return s.ListDeadLetters(ctx, tenantID, source)
	}
	candidates := &deadLetterReplayHeap{}
	err := s.scanDeadLetters(
		ctx,
		tenantID,
		source,
		func(item DeadLetter) error {
			if item.Status == "resolved" || item.Status == "invalid" {
				return nil
			}
			if candidates.Len() < limit {
				heap.Push(candidates, item)
				return nil
			}
			if deadLetterBefore(item, (*candidates)[0]) {
				heap.Pop(candidates)
				heap.Push(candidates, item)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	items := append([]DeadLetter(nil), (*candidates)...)
	sort.Slice(items, func(i, j int) bool {
		return deadLetterBefore(items[i], items[j])
	})
	return items, nil
}

// deadLetterReplayHeap keeps the newest selected item at the root so a
// bounded scan can replace it whenever an older replay candidate is found.
type deadLetterReplayHeap []DeadLetter

func (h deadLetterReplayHeap) Len() int {
	return len(h)
}

func (h deadLetterReplayHeap) Less(i int, j int) bool {
	return deadLetterBefore(h[j], h[i])
}

func (h deadLetterReplayHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *deadLetterReplayHeap) Push(value any) {
	*h = append(*h, value.(DeadLetter))
}

func (h *deadLetterReplayHeap) Pop() any {
	items := *h
	last := len(items) - 1
	value := items[last]
	*h = items[:last]
	return value
}
