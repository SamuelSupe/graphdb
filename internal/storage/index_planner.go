package storage

import "graphdb/internal/query"

func (catalog IndexCatalog) PlannerStats() query.PlannerStats {
	stats := query.PlannerStats{Version: catalog.Version}
	for _, index := range catalog.Indexes {
		stat := query.PlannerIndexStat{
			Kind:           index.Kind,
			Field:          index.Field,
			Status:         index.Status,
			EntryCount:     index.EntryCount,
			DistinctValues: index.DistinctValues,
		}
		for _, value := range index.TopValues {
			stat.TopValues = append(stat.TopValues, query.PlannerValueStat{Value: value.Value, Count: value.Count})
		}
		stats.Indexes = append(stats.Indexes, stat)
	}
	for _, shard := range catalog.EdgeShards {
		stats.EdgeShards = append(stats.EdgeShards, query.PlannerEdgeStat{
			RelationType: shard.RelationType,
			Shard:        shard.Shard,
			EdgeCount:    shard.EdgeCount,
		})
	}
	for _, page := range catalog.EntityPages {
		stats.EntityPages = append(stats.EntityPages, query.PlannerEntityPageStat{
			Shard:       page.Shard,
			EntityCount: page.EntityCount,
		})
	}
	return stats
}
