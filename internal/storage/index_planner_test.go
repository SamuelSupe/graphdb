package storage

import "testing"

func TestPlannerStatsMarksEmptyForwardEdgeIndexAvailable(t *testing.T) {
	stats := (IndexCatalog{
		Version:     9,
		EdgeShards:  []EdgeShard{},
		EntityPages: []EntityPageSpec{{Shard: "00", EntityCount: 1}},
	}).PlannerStats()

	if !stats.ForwardEdgeIndexAvailable {
		t.Fatal("forward edge index should be available for an empty current catalog")
	}
	if stats.ReverseEdgeIndexAvailable {
		t.Fatal("main index catalog must not claim reverse index availability")
	}
}
