package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
	edges := make([]graph.Edge, 0, len(g.Edges))
	for _, edge := range g.Edges {
		edges = append(edges, edge)
	}
	return buildEdgeShardsFromEdges(edges, version)
}

func buildEdgeShardsFromEdges(edges []graph.Edge, version int64) []EdgeShardData {
	now := time.Now().UTC()
	shards := map[string]EdgeShardData{}
	for _, edge := range edges {
		shardID := edgeShardID(edge.From)
		key := edge.Type + "\x00" + shardID
		shard := shards[key]
		shard.LayoutVersion = CurrentObjectLayoutVersion
		shard.RelationType = edge.Type
		shard.Shard = shardID
		shard.Version = version
		shard.UpdatedAt = now
		shard.Edges = append(shard.Edges, edge)
		shards[key] = shard
	}
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
	}
	for _, shard := range group.Shards {
		merged.Edges = append(merged.Edges, shard.Edges...)
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
