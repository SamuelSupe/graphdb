package query

import (
	"context"
	"errors"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestLazyImpactUsesCatalogCardinalityForAdmission(t *testing.T) {
	g := graph.New()
	g.Version = 7
	stats := PlannerStats{
		Version: 7,
		EdgeShards: []PlannerEdgeStat{{
			RelationType:    "depends_on",
			Shard:           plannerEdgeShardID("node:start"),
			EdgeCount:       500,
			ImpactDirection: "forward",
		}},
		EntityPages: []PlannerEntityPageStat{{
			Shard: "00", EntityCount: 501,
		}},
	}
	request := Request{
		Op:        "impact",
		ID:        "node:start",
		Depth:     1,
		CostLimit: 100,
	}

	plan := PlanQueryWithStats(g, request, stats)
	if plan.EstimatedRows != 500 || plan.EstimatedCost != 500 {
		t.Fatalf(
			"impact plan rows/cost = %d/%d, want 500/500",
			plan.EstimatedRows,
			plan.EstimatedCost,
		)
	}
	_, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		request,
		ExecuteOptions{PlannerStats: stats},
	)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("admission err = %v, want ErrLimitExceeded", err)
	}
}
