package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"graphdb/internal/graph"
)

func (s *TenantStore) writeEdgeShards(ctx context.Context, tenantID string, g *graph.Graph, version int64) error {
	return s.writeParquetEdgeShards(ctx, tenantID, buildEdgeShards(g, version))
}

func (s *TenantStore) writeEdgeShardsWithFormat(ctx context.Context, tenantID string, shards []EdgeShardData, format string) error {
	normalized, err := normalizeIndexFormat(format)
	if err != nil {
		return err
	}
	if normalized != IndexFormatParquet {
		return fmt.Errorf("unsupported index format %q", format)
	}
	return s.writeParquetEdgeShards(ctx, tenantID, shards)
}

func buildEdgeShards(g *graph.Graph, version int64) []EdgeShardData {
	counts := edgeShardCounts(g)
	now := time.Now().UTC()
	shards := newEdgeShardBuckets(counts, version, now)
	for _, edge := range g.Edges {
		appendEdgeShard(shards, edge)
	}
	return finishEdgeShards(shards)
}

func buildEdgeShardsFromEdges(edges []graph.Edge, version int64) []EdgeShardData {
	counts := make(map[string]int, len(edges))
	for _, edge := range edges {
		counts[edge.Type+"\x00"+edgeShardID(edge.From)]++
	}
	now := time.Now().UTC()
	shards := newEdgeShardBuckets(counts, version, now)
	for _, edge := range edges {
		appendEdgeShard(shards, edge)
	}
	return finishEdgeShards(shards)
}

func newEdgeShardBuckets(counts map[string]int, version int64, now time.Time) map[string]EdgeShardData {
	shards := make(map[string]EdgeShardData, len(counts))
	for key, count := range counts {
		relationType, shardID := splitShard(key)
		shards[key] = EdgeShardData{
			LayoutVersion: CurrentObjectLayoutVersion,
			RelationType:  relationType,
			Shard:         shardID,
			Edges:         make([]graph.Edge, 0, count),
			Version:       version,
			UpdatedAt:     now,
			hashCanonical: true,
		}
	}
	return shards
}

func appendEdgeShard(shards map[string]EdgeShardData, edge graph.Edge) {
	shardID := edgeShardID(edge.From)
	key := edge.Type + "\x00" + shardID
	shard := shards[key]
	shard.Edges = append(shard.Edges, edge)
	shard.hashCanonical = shard.hashCanonical && graphEdgeHashCanonical(edge)
	shards[key] = shard
}

func finishEdgeShards(shards map[string]EdgeShardData) []EdgeShardData {
	items := make([]EdgeShardData, 0, len(shards))
	for _, shard := range shards {
		sort.Slice(shard.Edges, func(i, j int) bool { return shard.Edges[i].ID < shard.Edges[j].ID })
		items = append(items, shard)
	}
	return items
}

func edgeShardPackIDs(shards []EdgeShard) map[string]string {
	items := make([]indexPackItem, 0, len(shards))
	for _, shard := range shards {
		items = append(items, indexPackItem{ID: shard.Shard, Group: shard.RelationType, Rows: shard.EdgeCount})
	}
	return indexPackMap(planIndexPacks(items))
}

func edgeShardDataPackGroups(shards []EdgeShardData) []edgeShardDataPackGroup {
	items := make([]indexPackItem, 0, len(shards))
	byKey := map[string]EdgeShardData{}
	for _, shard := range shards {
		key := shard.RelationType + "\x00" + shard.Shard
		items = append(items, indexPackItem{ID: shard.Shard, Group: shard.RelationType, Rows: len(shard.Edges)})
		byKey[key] = shard
	}
	groups := planIndexPacks(items)
	out := make([]edgeShardDataPackGroup, 0, len(groups))
	for _, group := range groups {
		packed := edgeShardDataPackGroup{ID: group.ID}
		for _, item := range group.Items {
			packed.Shards = append(packed.Shards, byKey[item.Group+"\x00"+item.ID])
		}
		out = append(out, packed)
	}
	return out
}

type edgeShardDataPackGroup struct {
	ID     string
	Shards []EdgeShardData
}

func mergeEdgeShardPack(group edgeShardDataPackGroup) EdgeShardData {
	if len(group.Shards) == 1 {
		return group.Shards[0]
	}
	first := group.Shards[0]
	merged := EdgeShardData{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      first.TenantID,
		RelationType:  first.RelationType,
		Shard:         group.ID,
		Version:       first.Version,
		UpdatedAt:     first.UpdatedAt,
		hashCanonical: true,
	}
	for _, shard := range group.Shards {
		merged.Edges = append(merged.Edges, shard.Edges...)
		merged.hashCanonical = merged.hashCanonical && shard.hashCanonical
	}
	sort.Slice(merged.Edges, func(i, j int) bool { return merged.Edges[i].ID < merged.Edges[j].ID })
	return merged
}

func edgeShardCounts(g *graph.Graph) map[string]int {
	shards := map[string]int{}
	for _, edge := range g.Edges {
		shards[edge.Type+"\x00"+edgeShardID(edge.From)]++
	}
	return shards
}

func edgeShardID(from string) string {
	return hashedIndexShardID(from)
}
