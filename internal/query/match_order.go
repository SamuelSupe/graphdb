package query

import (
	"crypto/sha256"
	"sort"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const entityPageShardBuckets = 64

func lazyEntityPageOrder(budget *budget) string {
	pager, ok := entityPageLookup(budget)
	if !ok {
		return EntityPageOrderIdentity
	}
	ordered, ok := pager.(EntityPageOrderLookup)
	if !ok || !validCursorOrder(ordered.EntityPageOrder()) {
		return EntityPageOrderIdentity
	}
	return ordered.EntityPageOrder()
}

func matchPageOrder(cursor cursorState, fallback string) string {
	if validCursorOrder(cursor.Order) {
		return cursor.Order
	}
	return fallback
}

func compareEntityPageOrder(left, right, order string) int {
	if order == EntityPageOrderShard {
		leftShard := entityPageShard(left)
		rightShard := entityPageShard(right)
		if leftShard < rightShard {
			return -1
		}
		if leftShard > rightShard {
			return 1
		}
	}
	return strings.Compare(left, right)
}

func entityPageShard(id string) byte {
	sum := sha256.Sum256([]byte(strings.ToLower(id)))
	return sum[0] % entityPageShardBuckets
}

func orderEntityIDs(ids []string, order string) []string {
	if order != EntityPageOrderShard || len(ids) < 2 {
		return ids
	}
	ordered := append([]string(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool {
		return compareEntityPageOrder(ordered[i], ordered[j], order) < 0
	})
	return ordered
}

func orderEntities(entities []graph.Entity, order string) []graph.Entity {
	if order != EntityPageOrderShard || len(entities) < 2 {
		return entities
	}
	ordered := append([]graph.Entity(nil), entities...)
	sort.Slice(ordered, func(i, j int) bool {
		return compareEntityPageOrder(ordered[i].ID, ordered[j].ID, order) < 0
	})
	return ordered
}
