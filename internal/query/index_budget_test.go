package query

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type oversizedIndexBatchLookup struct {
	ids             []string
	batchCalls      int
	materializedIDs int
}

type seekingFieldLookup struct {
	oversizedIndexBatchLookup
	fullCalls int
	visited   int
}

func (l *seekingFieldLookup) MatchFieldIndex(ctx context.Context, kind, field string, values []any) ([]string, bool, error) {
	l.fullCalls++
	return l.oversizedIndexBatchLookup.MatchFieldIndex(ctx, kind, field, values)
}

func (l *seekingFieldLookup) VisitFieldIndex(ctx context.Context, _, _ string, _ []any, afterID string, visit func(string) (bool, error)) (bool, error) {
	for _, id := range l.ids[sort.SearchStrings(l.ids, afterID):] {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		l.visited++
		more, err := visit(id)
		if err != nil || !more {
			return true, err
		}
	}
	return true, nil
}

func TestIndexedMatchCursorUsesSeekingVisitor(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			g := graph.New()
			g.Version = 1
			lookup := &seekingFieldLookup{}
			for i := range 10000 {
				lookup.ids = append(lookup.ids, fmt.Sprintf("host:%05d", i))
			}
			request := Request{Op: "match", Kind: "host", Where: []Filter{{Field: "hostname", Op: "eq", Value: "shared"}}, Limit: 3}
			request.Cursor = encodeCursor(cursorState{Version: 1, After: "entity:host:09000", Query: cursorQueryHash(request), Order: EntityPageOrderIdentity})
			options := ExecuteOptions{IndexLookup: lookup, EntityLookup: lookup, PlannerStats: PlannerStats{Version: 1, Indexes: []PlannerIndexStat{{Kind: "host", Field: "hostname", Status: "ready", EntryCount: 10000, DistinctValues: 1}}, EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: 10000}}}}
			var results []Result
			if stream {
				used, err := StreamContextWithOptions(context.Background(), g, request, options, func(value any) error {
					if result, ok := value.(Result); ok {
						results = append(results, result)
					}
					return nil
				})
				if err != nil || !used {
					t.Fatalf("stream used=%v err=%v", used, err)
				}
			} else {
				response, err := ExecuteContextWithOptions(context.Background(), g, request, options)
				if err != nil {
					t.Fatal(err)
				}
				results = response.Results
			}
			if len(results) != 3 || results[0].Entity.ID != "host:09001" {
				t.Fatalf("page: %+v", results)
			}
			if lookup.fullCalls != 0 || lookup.visited > 5 || lookup.visited == 0 {
				t.Fatalf("deep page materialized complete matches: full=%d visited=%d", lookup.fullCalls, lookup.visited)
			}
		})
	}
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
