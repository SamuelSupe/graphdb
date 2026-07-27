package storage

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type incrementalSecondaryGroup struct {
	original *IndexObject
	roles    []string
	shardID  string
	index    SecondaryIndex
	changed  bool
}

func (s *TenantStore) buildIncrementalSecondaryIndexes(ctx context.Context, tenantID string, previousVersion int64, previous []IndexSpec, before *graph.Graph, after *graph.Graph, entityIDs []string, version int64, now time.Time) ([]incrementalSecondaryIndexWrite, []IndexSpec, error) {
	next := append([]IndexSpec(nil), previous...)
	writes := make([]incrementalSecondaryIndexWrite, 0)
	for specIndex, spec := range previous {
		if !secondaryIndexAffected(spec, before, after, entityIDs) {
			continue
		}
		objects := append([]IndexObject(nil), spec.Objects...)
		shardedObjects, sharded := secondaryIndexShardObjectMap(spec.Objects)
		postings, hasPostings := indexObjectByRole(spec.Objects, "postings")
		groups := map[string]*incrementalSecondaryGroup{}

		loadGroup := func(value string) (*incrementalSecondaryGroup, error) {
			var object IndexObject
			var ok bool
			shardID := secondaryIndexShardID(value)
			if sharded {
				object, ok = secondaryIndexShardObjectForValue(shardedObjects, value)
				if !ok {
					shardID, ok = secondaryIndexNewShardID(shardedObjects, value)
					if !ok {
						return nil, fmt.Errorf("incremental index cannot route new value %q without shadowing an existing shard", value)
					}
				}
			} else if hasPostings {
				object, ok = postings, true
			}
			if !ok {
				key := "new\x00" + shardID
				if group := groups[key]; group != nil {
					return group, nil
				}
				group := &incrementalSecondaryGroup{
					shardID: shardID,
					roles:   []string{secondaryIndexShardRole(shardID)},
					index: SecondaryIndex{
						LayoutVersion: CurrentObjectLayoutVersion,
						TenantID:      tenantID,
						Kind:          spec.Kind,
						Field:         spec.Field,
						Unique:        secondaryIndexSpecUnique(spec),
						Values:        map[string][]string{},
					},
				}
				groups[key] = group
				return group, nil
			}
			key := object.Key + "\x00" + object.ContentHash
			if group := groups[key]; group != nil {
				return group, nil
			}
			index, valid, err := s.loadParquetSecondaryIndexShardObject(ctx, tenantID, previousVersion, spec, object)
			if err != nil {
				return nil, err
			}
			if !valid {
				return nil, fmt.Errorf("incremental index object %q is not readable at version %d", object.Key, previousVersion)
			}
			roles := make([]string, 0, 1)
			for _, candidate := range spec.Objects {
				if candidate.Key == object.Key && candidate.ContentHash == object.ContentHash {
					roles = append(roles, candidate.Role)
				}
			}
			copyObject := object
			group := &incrementalSecondaryGroup{original: &copyObject, roles: roles, shardID: shardID, index: index}
			groups[key] = group
			return group, nil
		}

		entryCount := spec.EntryCount
		distinctValues := spec.DistinctValues
		for _, entityID := range entityIDs {
			oldValue, oldOK := secondaryIndexEntityValue(before, entityID, spec)
			newValue, newOK := secondaryIndexEntityValue(after, entityID, spec)
			if oldOK == newOK && oldValue == newValue {
				continue
			}
			if oldOK {
				group, err := loadGroup(oldValue)
				if err != nil {
					return nil, nil, err
				}
				removed, emptied := removeSecondaryIndexID(group.index.Values, oldValue, entityID)
				if !removed {
					return nil, nil, fmt.Errorf("incremental index %s.%s is missing entity %q", spec.Kind, spec.Field, entityID)
				}
				group.changed = true
				entryCount--
				if emptied {
					distinctValues--
				}
			}
			if newOK {
				group, err := loadGroup(newValue)
				if err != nil {
					return nil, nil, err
				}
				added, created := addSecondaryIndexID(group.index.Values, newValue, entityID)
				if !added {
					return nil, nil, fmt.Errorf("incremental index %s.%s already contains entity %q", spec.Kind, spec.Field, entityID)
				}
				group.changed = true
				entryCount++
				if created {
					distinctValues++
				}
			}
		}

		for _, group := range groups {
			if !group.changed {
				continue
			}
			if group.original != nil {
				objects = removeSecondaryIndexObjectGroup(objects, *group.original)
			}
			if len(group.index.Values) == 0 {
				continue
			}
			group.index.LayoutVersion = CurrentObjectLayoutVersion
			group.index.TenantID = tenantID
			group.index.Version = version
			group.index.UpdatedAt = now
			group.index.cacheVerified = false
			group.index.logicalContentHash = ""
			group.index.cachedObjectGroups = nil
			contentHash := secondaryIndexContentHash(group.index)
			objectKey := s.parquetSecondaryIndexShardVersionKey(tenantID, version, spec.Kind, spec.Field, group.shardID)
			if group.original != nil {
				var ok bool
				objectKey, ok = s.rebaseParquetIndexObject(tenantID, group.original.Key, version)
				if !ok {
					return nil, nil, fmt.Errorf("incremental index object %q is outside the versioned parquet layout", group.original.Key)
				}
			}
			rowCount := secondaryIndexEntryCount(group.index)
			for _, role := range group.roles {
				objects = append(objects, IndexObject{
					Role: role, Key: objectKey, Format: IndexFormatParquet, Codec: parquetSecondaryIndexCodec,
					RowCount: rowCount, ContentHash: contentHash, SchemaHash: parquetSecondaryIndexSchemaHash(),
				})
			}
			writes = append(writes, incrementalSecondaryIndexWrite{Key: objectKey, Index: group.index})
		}
		if len(objects) == 0 {
			empty := SecondaryIndex{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      tenantID,
				Kind:          spec.Kind,
				Field:         spec.Field,
				Unique:        secondaryIndexSpecUnique(spec),
				Values:        map[string][]string{},
				Version:       version,
				UpdatedAt:     now,
			}
			key := s.parquetSecondaryIndexVersionKey(tenantID, version, spec.Kind, spec.Field)
			contentHash := secondaryIndexContentHash(empty)
			objects = append(objects, IndexObject{
				Role: "postings", Key: key, Format: IndexFormatParquet, Codec: parquetSecondaryIndexCodec,
				ContentHash: contentHash, SchemaHash: parquetSecondaryIndexSchemaHash(),
			})
			writes = append(writes, incrementalSecondaryIndexWrite{Key: key, Index: empty})
		}
		sort.Slice(objects, func(i, j int) bool { return objects[i].Role < objects[j].Role })
		nextSpec := spec
		nextSpec.Format = IndexFormatParquet
		nextSpec.Codec = parquetSecondaryIndexCodec
		nextSpec.Objects = objects
		nextSpec.RowCount = entryCount
		nextSpec.EntryCount = entryCount
		nextSpec.DistinctValues = distinctValues
		nextSpec.TopValues = nil
		nextSpec.ContentHash = secondaryIndexObjectSetContentHash(objects)
		nextSpec.SchemaHash = parquetSecondaryIndexSchemaHash()
		nextSpec.UpdatedAt = now
		next[specIndex] = nextSpec
	}
	return writes, next, nil
}

func secondaryIndexNewShardID(shards map[string]IndexObject, valueKey string) (string, bool) {
	base := secondaryIndexShardID(valueKey)
	if !secondaryIndexShardShadowsExisting(shards, base) {
		return base, true
	}
	if !strings.HasPrefix(valueKey, "s:") {
		return "", false
	}
	for prefixBytes := secondaryIndexStringShardPrefixBytes + 1; prefixBytes <= secondaryIndexStringShardMaxBytes; prefixBytes++ {
		candidate := secondaryIndexStringShardID(valueKey, prefixBytes)
		if candidate == base || secondaryIndexShardShadowsExisting(shards, candidate) {
			continue
		}
		return candidate, true
	}
	return "", false
}

func secondaryIndexShardShadowsExisting(shards map[string]IndexObject, candidate string) bool {
	for shardID := range shards {
		if shardID != candidate && strings.HasPrefix(shardID, candidate) {
			return true
		}
	}
	return false
}

func secondaryIndexAffected(spec IndexSpec, before *graph.Graph, after *graph.Graph, entityIDs []string) bool {
	for _, entityID := range entityIDs {
		oldValue, oldOK := secondaryIndexEntityValue(before, entityID, spec)
		newValue, newOK := secondaryIndexEntityValue(after, entityID, spec)
		if oldOK != newOK || oldValue != newValue {
			return true
		}
	}
	return false
}

func secondaryIndexEntityValue(g *graph.Graph, entityID string, spec IndexSpec) (string, bool) {
	if g == nil {
		return "", false
	}
	entity, ok := g.Entities[entityID]
	if !ok || entity.Kind != spec.Kind {
		return "", false
	}
	value, ok := entity.Fields[spec.Field]
	if !ok {
		return "", false
	}
	return secondaryIndexValue(value)
}

func removeSecondaryIndexID(values map[string][]string, value string, entityID string) (bool, bool) {
	ids := values[value]
	index := sort.SearchStrings(ids, entityID)
	if index >= len(ids) || ids[index] != entityID {
		return false, false
	}
	ids = append(ids[:index], ids[index+1:]...)
	if len(ids) == 0 {
		delete(values, value)
		return true, true
	}
	values[value] = ids
	return true, false
}

func addSecondaryIndexID(values map[string][]string, value string, entityID string) (bool, bool) {
	ids, existed := values[value]
	index := sort.SearchStrings(ids, entityID)
	if index < len(ids) && ids[index] == entityID {
		return false, false
	}
	ids = append(ids, "")
	copy(ids[index+1:], ids[index:])
	ids[index] = entityID
	values[value] = ids
	return true, !existed
}

func removeSecondaryIndexObjectGroup(objects []IndexObject, target IndexObject) []IndexObject {
	next := objects[:0]
	for _, object := range objects {
		if object.Key == target.Key && object.ContentHash == target.ContentHash {
			continue
		}
		next = append(next, object)
	}
	return next
}

func (s *TenantStore) rebaseParquetIndexObject(tenantID string, key string, version int64) (string, bool) {
	suffix := s.parquetObjectSuffix(tenantID, key)
	if suffix == key {
		return "", false
	}
	return path.Join(s.parquetVersionPrefix(tenantID, version), suffix), true
}
