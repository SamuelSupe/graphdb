package storage

import (
	"context"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func estimateFieldLookupBytes(values map[string][]string) int64 {
	size := int64(128)
	for value, ids := range values {
		size += 64 + int64(len(value))
		for _, id := range ids {
			size += 24 + int64(len(id))
		}
	}
	return size
}

func (l *PersistedIndexLookup) VisitFieldIndex(ctx context.Context, kind, field string, values []any, startID string, visit func(string) (bool, error)) (bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return false, nil
	}
	spec, ok := l.catalogFieldSpec(kind, field)
	if !ok || specFormat(spec.Format) != IndexFormatParquet || spec.ContentHash == "" {
		return false, nil
	}
	var buckets [][]string
	seen := map[string]bool{}
	shards, sharded := secondaryIndexShardObjectMap(spec.Objects)
	for _, value := range values {
		keys, supported := secondaryIndexLookupKeys(value)
		if !supported {
			return false, nil
		}
		for _, key := range keys {
			var objects []IndexObject
			if sharded {
				for shard, object := range shards {
					if secondaryIndexShardMayContainValue(shard, key) {
						objects = append(objects, object)
					}
				}
				objects = uniqueIndexObjects(objects)
			} else {
				objects = []IndexObject{{}}
			}
			for _, object := range objects {
				identity := object.Key + "\x00" + object.ContentHash + "\x00" + key
				if seen[identity] {
					continue
				}
				seen[identity] = true
				postings, ok, err := l.fieldPostings(ctx, spec, object, sharded)
				if err != nil {
					if ctx.Err() != nil {
						return false, err
					}
					return false, nil
				}
				if !ok {
					return false, nil
				}
				buckets = append(buckets, postings[key])
			}
		}
	}
	err := graph.VisitSortedIDs(buckets, startID, func(id string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return visit(id)
	})
	return true, err
}

func (l *PersistedIndexLookup) fieldPostings(ctx context.Context, spec IndexSpec, object IndexObject, sharded bool) (map[string][]string, bool, error) {
	cacheKey := l.TenantID + "\x00" + strconv.FormatInt(l.Version, 10) + "\x00" + spec.Kind + "\x00" + spec.Field + "\x00" + spec.ContentHash + "\x00" + object.Key + "\x00" + object.ContentHash
	return l.Store.fieldLookupCache.load(ctx, cacheKey, func() (map[string][]string, bool, error) {
		var index SecondaryIndex
		var ok bool
		var err error
		if sharded {
			index, ok, err = l.Store.loadParquetSecondaryIndexShardObject(ctx, l.TenantID, l.Version, spec, object)
		} else {
			index, ok, err = l.Store.loadParquetSecondaryIndexObject(ctx, l.TenantID, l.Version, spec)
			ok = ok && indexTenantMatches(index.TenantID, l.TenantID) && fieldIndexMatchesCatalog(index, spec, l.Version)
		}
		return index.Values, ok, err
	})
}

func (l *PersistedIndexLookup) VisitFilteredFieldIndex(ctx context.Context, kind, field string, filters []query.Filter, startID string, visit func(string) (bool, error)) (bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return false, nil
	}
	spec, ok := l.catalogFieldSpec(kind, field)
	if !ok || specFormat(spec.Format) != IndexFormatParquet || spec.ContentHash == "" {
		return false, nil
	}
	objects, sharded := secondaryIndexShardObjectsForFilters(spec.Objects, filters)
	if !sharded {
		objects = []IndexObject{{}}
	}
	match := query.IndexScanKeyMatcher(filters)
	var buckets [][]string
	for _, object := range uniqueIndexObjects(objects) {
		postings, ok, err := l.fieldPostings(ctx, spec, object, sharded)
		if err != nil {
			if ctx.Err() != nil {
				return false, err
			}
			return false, nil
		}
		if !ok {
			return false, nil
		}
		for key, ids := range postings {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if match(key) {
				buckets = append(buckets, ids)
			}
		}
	}
	return true, graph.VisitSortedIDs(buckets, startID, func(id string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return visit(id)
	})
}
