package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (s *TenantStore) buildIncrementalEdgeShards(ctx context.Context, tenantID string, previousVersion int64, previous []EdgeShard, before *graph.Graph, after *graph.Graph, edgeIDs []string, version int64, now time.Time) ([]EdgeShardData, []EdgeShard, error) {
	return s.buildIncrementalEdgeShardsFor(
		ctx,
		tenantID,
		previousVersion,
		previous,
		before,
		after,
		edgeIDs,
		version,
		now,
		false,
		func(edge graph.Edge) string {
			return edgeShardID(edge.From)
		},
		func(catalog *IndexCatalog) {
			s.decorateIndexCatalog(catalog, tenantID)
		},
	)
}

func (s *TenantStore) buildIncrementalEdgeShardsFor(ctx context.Context, tenantID string, previousVersion int64, previous []EdgeShard, before *graph.Graph, after *graph.Graph, edgeIDs []string, version int64, now time.Time, reverse bool, shardIDFor func(graph.Edge) string, decorate func(*IndexCatalog)) ([]EdgeShardData, []EdgeShard, error) {
	if before.Version != previousVersion || after.Version != version {
		return nil, nil, fmt.Errorf("incremental edge shards require graph versions %d and %d", previousVersion, version)
	}
	for _, spec := range previous {
		if (hasIndexRanges(spec.Objects) && !s.WriteIndexRangePages) || (!hasIndexRanges(spec.Objects) && spec.Codec != parquetEdgeSingleCodec) {
			return nil, nil, fmt.Errorf("legacy edge shards require a controlled rebuild")
		}
	}
	previousByKey := edgeShardSpecMap(IndexCatalog{EdgeShards: previous})
	changed := map[string]map[string]bool{}
	for _, id := range edgeIDs {
		for _, g := range []*graph.Graph{before, after} {
			if edge, ok := g.Edges.Get(id); ok {
				key := edgeShardTargetKey(edge.Type, shardIDFor(edge))
				if changed[key] == nil {
					changed[key] = map[string]bool{}
				}
				changed[key][id] = true
			}
		}
	}
	if len(changed) == 0 {
		return nil, previous, ctx.Err()
	}
	replacements := map[string]EdgeShard{}
	removed := map[string]bool{}
	var shards []EdgeShardData
	for _, key := range sortedStringKeys(changed) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		relationType, shardID := splitShard(key)
		old, exists := previousByKey[key]
		ranged := hasIndexRanges(old.Objects)
		objects := old.Objects
		if ranged {
			if err := validateRangeObjects(objects, old.EdgeCount, old.ContentHash); err != nil {
				return nil, nil, err
			}
		} else {
			objects = []IndexObject{{}}
		}
		byRange := map[int][]string{}
		for id := range changed[key] {
			n := 0
			if ranged {
				n = rangeObjectIndex(objects, id)
			}
			byRange[n] = append(byRange[n], id)
		}
		var nextObjects []IndexObject
		shrank := map[string]bool{}
		var nextLegacy EdgeShard
		for n, object := range objects {
			ids, touched := byRange[n]
			if !touched {
				nextObjects = append(nextObjects, object)
				continue
			}
			rows := map[string]graph.Edge{}
			if exists {
				partSpec := old
				if ranged {
					partSpec = edgeRangeSpec(old, object)
				}
				part, ok, err := s.loadParquetEdgeShardObject(ctx, tenantID, previousVersion, partSpec)
				if err != nil {
					return nil, nil, err
				}
				if !ok || !edgeShardMatchesCatalog(part, partSpec, previousVersion) || !indexTenantMatches(part.TenantID, tenantID) {
					return nil, nil, fmt.Errorf("previous edge shard is unavailable")
				}
				routing, _ := decodeIndexRange(object.Role)
				for _, edge := range part.Edges {
					if ranged && !routing.contains(edge.ID) {
						return nil, nil, fmt.Errorf("record is outside its previous index range")
					}
					rows[edge.ID] = edge
				}
			}
			for _, id := range ids {
				edge, ok := after.Edges.Get(id)
				endpoint := edge.From
				if reverse {
					endpoint = edge.To
				}
				if ok && edge.Type == relationType && indexShardIDMatches(endpoint, shardID) {
					rows[id] = edge
				} else {
					delete(rows, id)
				}
			}
			if len(rows) == 0 {
				continue
			}
			shard := EdgeShardData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, RelationType: relationType, Shard: shardID, Version: version, UpdatedAt: now, hashCanonical: true, reverse: reverse}
			if ranged {
				shard.pageRole = object.Role
			}
			for _, edge := range rows {
				shard.Edges = append(shard.Edges, edge)
				shard.hashCanonical = shard.hashCanonical && graphEdgeHashCanonical(edge)
			}
			sort.Slice(shard.Edges, func(i, j int) bool { return shard.Edges[i].ID < shard.Edges[j].ID })
			parts := []EdgeShardData{shard}
			var err error
			if s.WriteIndexRangePages {
				parts, err = prepareBoundedEdgeShards(s.rangeEncodeContext(ctx), tenantID, parts)
			}
			if err != nil {
				return nil, nil, err
			}
			for _, part := range parts {
				part.logicalContentHash = edgeShardContentHash(part)
				if part.pageRole == "" {
					nextLegacy = edgePageSpecs([]EdgeShardData{part}, after)[0]
					if old.ContentHash == nextLegacy.ContentHash {
						nextLegacy.Objects = append([]IndexObject(nil), old.Objects...)
					}
					shards = append(shards, part)
				} else {
					next := IndexObject{Role: part.pageRole, RowCount: len(part.Edges), ContentHash: part.logicalContentHash, SchemaHash: parquetEdgeShardSchemaHash()}
					objectID := boundedPageObjectID(shardID, part.logicalContentHash)
					if reverse {
						next.Key = s.reverseEdgeShardVersionKey(tenantID, version, relationType, objectID)
					} else {
						next.Key = s.parquetEdgeShardVersionKey(tenantID, version, relationType, objectID)
					}
					if ranged && object.ContentHash == next.ContentHash && object.SchemaHash == next.SchemaHash {
						next = object
					} else {
						shards = append(shards, part)
					}
					nextObjects = append(nextObjects, next)
					if ranged && len(parts) == 1 && rangePageShrank(object, next) {
						shrank[next.ContentHash] = true
					}
				}
			}
		}
		if len(nextObjects) > 0 {
			if len(shrank) > 0 {
				var err error
				nextObjects, shards, err = s.mergeEdgeRanges(ctx, tenantID, version, old, nextObjects, shards, shrank, reverse)
				if err != nil {
					return nil, nil, err
				}
			}
			routing, _ := decodeIndexRange(nextObjects[0].Role)
			routing.Lower = ""
			nextObjects[0].Role = encodeRangeRole(routing)
			spec := EdgeShard{RelationType: relationType, ImpactDirection: relationImpactDirection(after, relationType), Shard: shardID, Format: IndexFormatParquet, Objects: nextObjects, UpdatedAt: now}
			for _, object := range nextObjects {
				spec.EdgeCount += object.RowCount
			}
			if err := s.updateEdgeAdjacency(ctx, tenantID, old, &spec, before, after, changed[key], version, reverse); err != nil {
				return nil, nil, err
			}
			replacements[key] = spec
		} else if nextLegacy.EdgeCount > 0 {
			replacements[key] = nextLegacy
		} else {
			removed[key] = true
		}
	}
	mini := IndexCatalog{Version: version}
	for _, key := range sortedEdgeShardSpecKeys(replacements) {
		mini.EdgeShards = append(mini.EdgeShards, replacements[key])
	}
	decorate(&mini)
	replacements = edgeShardSpecMap(mini)
	next := make([]EdgeShard, 0, len(previous)+len(replacements))
	for _, spec := range previous {
		key := edgeShardTargetKey(spec.RelationType, spec.Shard)
		if removed[key] {
			continue
		}
		if replacement, ok := replacements[key]; ok {
			if !hasIndexRanges(replacement.Objects) && replacement.ContentHash == spec.ContentHash && replacement.SchemaHash == spec.SchemaHash {
				replacement.Objects = append([]IndexObject(nil), spec.Objects...)
			}
			next = append(next, replacement)
			delete(replacements, key)
		} else {
			next = append(next, spec)
		}
	}
	for _, key := range sortedEdgeShardSpecKeys(replacements) {
		next = append(next, replacements[key])
	}
	return shards, next, ctx.Err()
}

func sortedEdgeShardSpecKeys(values map[string]EdgeShard) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
