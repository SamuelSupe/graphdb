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
			s.decorateIndexCatalog(
				catalog,
				tenantID,
				IndexFormatParquet,
			)
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
	previousByKey := edgeShardSpecMap(IndexCatalog{EdgeShards: previous})
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
	shards := make([]EdgeShardData, 0, len(keys))
	rawSpecs := make([]EdgeShard, 0, len(keys))
	removed := map[string]struct{}{}
	for _, key := range keys {
		relationType, shardID := splitShard(key)
		spec, existed := previousByKey[key]
		shard := EdgeShardData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, RelationType: relationType, Shard: shardID}
		if existed {
			loaded, ok, err := s.loadParquetEdgeShardObject(ctx, tenantID, previousVersion, spec)
			if err != nil {
				return nil, nil, err
			}
			if !ok || !edgeShardMatchesCatalog(loaded, spec, previousVersion) {
				return nil, nil, fmt.Errorf("incremental edge shard %s/%s is not readable at version %d", relationType, shardID, previousVersion)
			}
			shard = loaded
		}
		edges := make(map[string]graph.Edge, len(shard.Edges)+len(changedByKey[key]))
		for _, edge := range shard.Edges {
			edges[edge.ID] = edge
		}
		for _, edgeID := range changedByKey[key] {
			delete(edges, edgeID)
			if edge, ok := after.Edges[edgeID]; ok &&
				edge.Type == relationType &&
				shardIDFor(edge) == shardID {
				edges[edgeID] = graph.CopyEdge(edge)
			} else if !existed {
				if old, ok := before.Edges[edgeID]; ok &&
					old.Type == relationType &&
					shardIDFor(old) == shardID {
					return nil, nil, fmt.Errorf("incremental edge shard %s/%s is missing from the previous catalog", relationType, shardID)
				}
			}
		}
		if len(edges) == 0 {
			removed[key] = struct{}{}
			continue
		}
		shard = EdgeShardData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			RelationType:  relationType,
			Shard:         shardID,
			Edges:         make([]graph.Edge, 0, len(edges)),
			Version:       version,
			UpdatedAt:     now,
		}
		for _, edge := range edges {
			shard.Edges = append(shard.Edges, edge)
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
