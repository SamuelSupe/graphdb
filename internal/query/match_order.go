package query

import (
	"crypto/sha256"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
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
	type keyedID struct {
		shard byte
		id    string
	}
	items := make([]keyedID, len(ids))
	for i, id := range ids {
		items[i] = keyedID{shard: entityPageShard(id), id: id}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].shard != items[j].shard {
			return items[i].shard < items[j].shard
		}
		return items[i].id < items[j].id
	})
	ordered := make([]string, len(items))
	for i, item := range items {
		ordered[i] = item.id
	}
	return ordered
}

func orderEntities(entities []graph.Entity, order string) []graph.Entity {
	if order != EntityPageOrderShard || len(entities) < 2 {
		return entities
	}
	type keyedEntity struct {
		shard  byte
		entity graph.Entity
	}
	items := make([]keyedEntity, len(entities))
	for i, entity := range entities {
		items[i] = keyedEntity{shard: entityPageShard(entity.ID), entity: entity}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].shard != items[j].shard {
			return items[i].shard < items[j].shard
		}
		return items[i].entity.ID < items[j].entity.ID
	})
	ordered := make([]graph.Entity, len(items))
	for i, item := range items {
		ordered[i] = item.entity
	}
	return ordered
}

func orderResults(results []Result, order string) []Result {
	if order != EntityPageOrderShard || len(results) < 2 {
		return results
	}
	type keyedResult struct {
		hasEntity bool
		shard     byte
		key       string
		result    Result
	}
	items := make([]keyedResult, len(results))
	for i, res := range results {
		if res.Entity != nil {
			items[i] = keyedResult{
				hasEntity: true,
				shard:     entityPageShard(res.Entity.ID),
				key:       res.Entity.ID,
				result:    res,
			}
		} else {
			items[i] = keyedResult{
				key:    resultIdentity(res),
				result: res,
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].hasEntity && items[j].hasEntity {
			if items[i].shard != items[j].shard {
				return items[i].shard < items[j].shard
			}
			return items[i].key < items[j].key
		}
		return resultIdentity(items[i].result) < resultIdentity(items[j].result)
	})
	ordered := make([]Result, len(items))
	for i, item := range items {
		ordered[i] = item.result
	}
	return ordered
}
