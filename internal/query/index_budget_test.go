package query

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type oversizedIndexBatchLookup struct {
	ids             []string
	batchCalls      int
	materializedIDs int
}

func (l *oversizedIndexBatchLookup) MatchFieldIndex(
	context.Context,
	string,
	string,
	[]any,
) ([]string, bool, error) {
	return append([]string(nil), l.ids...), true, nil
}

func (*oversizedIndexBatchLookup) OutEdges(
	context.Context,
	string,
	map[string]struct{},
) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (l *oversizedIndexBatchLookup) GetEntity(
	_ context.Context,
	id string,
	_ []string,
) (graph.Entity, bool, error) {
	return indexedBudgetEntity(id), true, nil
}

func (l *oversizedIndexBatchLookup) GetEntities(
	_ context.Context,
	ids []string,
	_ []string,
) (map[string]graph.Entity, bool, error) {
	l.batchCalls++
	l.materializedIDs += len(ids)
	entities := make(map[string]graph.Entity, len(ids))
	for _, id := range ids {
		entities[id] = indexedBudgetEntity(id)
	}
	return entities, true, nil
}

func indexedBudgetEntity(id string) graph.Entity {
	return graph.Entity{
		ID:     id,
		Kind:   "host",
		Fields: graph.Fields{"hostname": "shared"},
	}
}

func TestCompleteIndexQueryChecksActualCardinalityBeforeBatchMaterialization(
	t *testing.T,
) {
	g := graph.New()
	g.Version = 1
	lookup := &oversizedIndexBatchLookup{}
	for i := 0; i < 8; i++ {
		lookup.ids = append(lookup.ids, fmt.Sprintf("host:%02d", i))
	}
	_, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		Request{
			Op:        "match",
			Kind:      "host",
			Where:     []Filter{{Field: "hostname", Op: "eq", Value: "shared"}},
			Aggregate: []Aggregation{{Op: "count"}},
			CostLimit: 3,
		},
		ExecuteOptions{
			PlannerStats: PlannerStats{
				Version: 1,
				Indexes: []PlannerIndexStat{{
					Kind:           "host",
					Field:          "hostname",
					Status:         "ready",
					EntryCount:     100,
					DistinctValues: 100,
				}},
				EntityPages: []PlannerEntityPageStat{{
					Shard:       "00",
					EntityCount: 100,
				}},
			},
			IndexLookup:  lookup,
			EntityLookup: lookup,
		},
	)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err = %v, want ErrLimitExceeded", err)
	}
	if lookup.batchCalls != 0 || lookup.materializedIDs != 0 {
		t.Fatalf(
			"batch calls=%d materialized=%d, want no entity batch read",
			lookup.batchCalls,
			lookup.materializedIDs,
		)
	}
}

func TestLazyCompleteKindScanUsesEntityPageCardinalityForAdmission(
	t *testing.T,
) {
	g := graph.New()
	g.Version = 7
	lookup := &pageScanLookup{}
	stats := PlannerStats{
		Version: 7,
		EntityPages: []PlannerEntityPageStat{
			{Shard: "00", EntityCount: 80},
			{Shard: "01", EntityCount: 70},
		},
	}
	request := Request{
		Op:        "match",
		Kind:      "host",
		Aggregate: []Aggregation{{Op: "count"}},
		CostLimit: 100,
	}

	plan := PlanQueryWithStats(g, request, stats)
	if plan.EstimatedRows != 150 || plan.EstimatedCost != 300 {
		t.Fatalf(
			"plan rows/cost = %d/%d, want 150/300",
			plan.EstimatedRows,
			plan.EstimatedCost,
		)
	}
	_, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		request,
		ExecuteOptions{
			PlannerStats: stats,
			IndexLookup:  lookup,
			EntityLookup: lookup,
		},
	)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("admission err = %v, want ErrLimitExceeded", err)
	}
	if lookup.calls != 0 {
		t.Fatalf("entity page scans = %d, want none before admission", lookup.calls)
	}
}

func TestLazyUnfilteredKindPageEstimatesOnlyLimitLookahead(t *testing.T) {
	g := graph.New()
	g.Version = 7
	stats := PlannerStats{
		Version: 7,
		EntityPages: []PlannerEntityPageStat{{
			Shard:       "00",
			EntityCount: 250000,
		}},
	}

	plan := PlanQueryWithStats(g, Request{
		Op:    "match",
		Kind:  "host",
		Limit: 10,
	}, stats)
	if plan.EstimatedRows != 11 || plan.EstimatedCost != 11 {
		t.Fatalf(
			"plan rows/cost = %d/%d, want 11/11",
			plan.EstimatedRows,
			plan.EstimatedCost,
		)
	}
}
