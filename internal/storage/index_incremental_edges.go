package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
		func(edge graph.Edge) string {
			return edgeShardID(edge.From)
		},
		func(catalog *IndexCatalog) {
			s.decorateIndexCatalog(catalog, tenantID)
		},
	)
}

func (s *TenantStore) buildIncrementalEdgeShardsFor(
	ctx context.Context,
	tenantID string,
	previousVersion int64,
	previous []EdgeShard,
	before *graph.Graph,
	after *graph.Graph,
	edgeIDs []string,
	version int64,
	now time.Time,
	shardIDFor func(graph.Edge) string,
	decorate func(*IndexCatalog),
) ([]EdgeShardData, []EdgeShard, error) {
	if before.Version != previousVersion || after.Version != version {
		return nil, nil, fmt.Errorf("incremental edge shards require graph versions %d and %d", previousVersion, version)
	}
	changedByKey := map[string][]string{}
	for _, edgeID := range edgeIDs {
		if edge, ok := before.Edges[edgeID]; ok {
			key := edgeShardTargetKey(edge.Type, shardIDFor(edge))
			changedByKey[key] = append(changedByKey[key], edgeID)
		}
		if edge, ok := after.Edges[edgeID]; ok {
			key := edgeShardTargetKey(edge.Type, shardIDFor(edge))
			changedByKey[key] = append(changedByKey[key], edgeID)
		}
	}
	keys := sortedStringKeys(changedByKey)
	if len(keys) == 0 {
		return nil, previous, ctx.Err()
	}
	edgesByKey := make(map[string][]graph.Edge, len(keys))
	checked := 0
	for _, edge := range after.Edges {
		checked++
		if checked&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		key := edgeShardTargetKey(edge.Type, shardIDFor(edge))
		if _, changed := changedByKey[key]; changed {
			edgesByKey[key] = append(edgesByKey[key], graph.CopyEdge(edge))
		}
	}
	shards := make([]EdgeShardData, 0, len(keys))
	rawSpecs := make([]EdgeShard, 0, len(keys))
	removed := map[string]struct{}{}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		relationType, shardID := splitShard(key)
		edges := edgesByKey[key]
		if len(edges) == 0 {
			removed[key] = struct{}{}
			continue
		}
		shard := EdgeShardData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			RelationType:  relationType,
			Shard:         shardID,
			Edges:         edges,
			Version:       version,
			UpdatedAt:     now,
		}
		sort.Slice(shard.Edges, func(i, j int) bool { return shard.Edges[i].ID < shard.Edges[j].ID })
		shard.logicalContentHash = edgeShardContentHash(shard)
		shards = append(shards, shard)
		rawSpecs = append(rawSpecs, EdgeShard{
			RelationType:    relationType,
			ImpactDirection: relationImpactDirection(after, relationType),
			Shard:           shardID,
			EdgeCount:       len(shard.Edges),
			ContentHash:     shard.logicalContentHash,
			UpdatedAt:       now,
		})
	}

	mini := IndexCatalog{Version: version, EdgeShards: rawSpecs}
	decorate(&mini)
	decorated := edgeShardSpecMap(mini)
	next := make([]EdgeShard, 0, len(previous)+len(decorated))
	for _, spec := range previous {
		key := edgeShardTargetKey(spec.RelationType, spec.Shard)
		if _, deleted := removed[key]; deleted {
			continue
		}
		if replacement, ok := decorated[key]; ok {
			if replacement.ContentHash == spec.ContentHash && replacement.SchemaHash == spec.SchemaHash {
				replacement.Objects = append([]IndexObject(nil), spec.Objects...)
			}
			next = append(next, replacement)
			delete(decorated, key)
			continue
		}
		next = append(next, spec)
	}
	for _, key := range sortedEdgeShardSpecKeys(decorated) {
		next = append(next, decorated[key])
	}
	return shards, next, nil
}

func sortedEdgeShardSpecKeys(values map[string]EdgeShard) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
