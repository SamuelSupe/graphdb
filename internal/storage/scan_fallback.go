package storage

import (
	"container/heap"
	"sort"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type scanCandidate[T any] struct {
	position scanPosition
	value    T
}

type scanCandidateHeap[T any] []scanCandidate[T]

type scanPosition struct {
	group string
	id    string
}

func pageEntityMap(candidates map[string]graph.Entity, version int64, options EntityScanOptions, cursor scanCursor) ([]graph.Entity, string) {
	items := selectBoundedScanCandidates(candidates, normalizedScanLimit(options.Limit)+1, cursor.After,
		func(entity graph.Entity) scanPosition {
			return scanPosition{group: entityShardID(entity.ID), id: entity.ID}
		},
		func(entity graph.Entity) bool { return entityMatchesScan(entity, options) },
	)
	return pageEntities(items, version, options, cursor)
}

func pageEdgeMap(candidates map[string]graph.Edge, version int64, options EdgeScanOptions, cursor scanCursor) ([]graph.Edge, string) {
	items := selectBoundedScanCandidates(candidates, normalizedScanLimit(options.Limit)+1, cursor.After,
		func(edge graph.Edge) scanPosition {
			return scanPosition{group: edge.Type + "\x00" + edgeShardID(edge.From), id: edge.ID}
		},
		func(edge graph.Edge) bool { return edgeMatchesScan(edge, options) },
	)
	return pageEdges(items, version, options, cursor)
}

func selectBoundedScanCandidates[T any](values map[string]T, keep int, after string, positionFor func(T) scanPosition, matches func(T) bool) []T {
	if keep <= 0 {
		return nil
	}
	afterPosition, validAfter := parseScanPosition(after)
	candidates := make(scanCandidateHeap[T], 0, min(len(values), keep))
	for _, value := range values {
		if !matches(value) {
			continue
		}
		position := positionFor(value)
		if after != "" {
			if validAfter {
				if position.compare(afterPosition) <= 0 {
					continue
				}
			} else if scanKey(position.group, position.id) <= after {
				continue
			}
		}
		candidate := scanCandidate[T]{position: position, value: value}
		if len(candidates) < keep {
			heap.Push(&candidates, candidate)
			continue
		}
		if position.compare(candidates[0].position) >= 0 {
			continue
		}
		candidates[0] = candidate
		heap.Fix(&candidates, 0)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].position.compare(candidates[j].position) < 0 })
	selected := make([]T, len(candidates))
	for i := range candidates {
		selected[i] = candidates[i].value
	}
	return selected
}

func parseScanPosition(key string) (scanPosition, bool) {
	separator := strings.LastIndexByte(key, 0)
	if separator < 0 {
		return scanPosition{}, false
	}
	return scanPosition{group: key[:separator], id: key[separator+1:]}, true
}

func (position scanPosition) compare(other scanPosition) int {
	if group := strings.Compare(position.group, other.group); group != 0 {
		return group
	}
	return strings.Compare(position.id, other.id)
}

func (h scanCandidateHeap[T]) Len() int { return len(h) }

func (h scanCandidateHeap[T]) Less(i, j int) bool {
	return h[i].position.compare(h[j].position) > 0
}

func (h scanCandidateHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *scanCandidateHeap[T]) Push(value any) {
	*h = append(*h, value.(scanCandidate[T]))
}

func (h *scanCandidateHeap[T]) Pop() any {
	values := *h
	last := len(values) - 1
	value := values[last]
	var zero scanCandidate[T]
	values[last] = zero
	*h = values[:last]
	return value
}
