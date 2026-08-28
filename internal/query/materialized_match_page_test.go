package query

import (
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestMaterializedKindPageUsesFullScanAdmissionCost(t *testing.T) {
	g := graph.New()
	g.Version = 1
	for i := 0; i < 150; i++ {
		id := fmt.Sprintf("host:%03d", i)
		g.Entities[id] = graph.Entity{ID: id, Kind: "host"}
	}
	request := Request{
		Op:        "match",
		Kind:      "host",
		Limit:     1,
		CostLimit: 100,
	}
	plan := PlanQuery(g, request)
	if plan.EstimatedRows != 150 || plan.EstimatedCost != 150 {
		t.Fatalf(
			"plan rows/cost = %d/%d, want 150/150",
			plan.EstimatedRows,
			plan.EstimatedCost,
		)
	}
	if _, err := Execute(g, request); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("execute err = %v, want ErrLimitExceeded", err)
	}
}

func TestMaterializedKindPagePreservesFilteredCursorOrder(t *testing.T) {
	g := graph.New()
	g.Version = 1
	for _, entity := range []graph.Entity{
		{ID: "host:e", Kind: "host", Fields: graph.Fields{"active": true}},
		{ID: "service:b", Kind: "service", Fields: graph.Fields{"active": true}},
		{ID: "host:c", Kind: "host", Fields: graph.Fields{"active": true}},
		{ID: "host:b", Kind: "host", Fields: graph.Fields{"active": false}},
		{ID: "host:a", Kind: "host", Fields: graph.Fields{"active": true}},
	} {
		g.Entities[entity.ID] = entity
	}
	request := Request{
		Op:        "match",
		Kind:      "host",
		Where:     []Filter{{Field: "active", Op: "eq", Value: true}},
		Limit:     2,
		CostLimit: 10,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatal(err)
	}
	if got := resultEntityIDs(first.Results); len(got) != 2 ||
		got[0] != "host:a" || got[1] != "host:c" {
		t.Fatalf("first page = %#v", got)
	}
	if first.NextCursor == "" {
		t.Fatal("first page missing cursor")
	}

	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatal(err)
	}
	if got := resultEntityIDs(second.Results); len(got) != 1 ||
		got[0] != "host:e" {
		t.Fatalf("second page = %#v", got)
	}
	if second.NextCursor != "" {
		t.Fatalf("second page cursor = %q, want empty", second.NextCursor)
	}
}

func TestMaterializedKindPageStopsAfterRequestedKindWindow(t *testing.T) {
	g := graph.New()
	entities := make([]graph.Entity, 0, 1005)
	for i := 0; i < 1000; i++ {
		entities = append(entities, graph.Entity{
			ID: fmt.Sprintf("service:%04d", i), Kind: "service",
		})
	}
	for i := 0; i < 5; i++ {
		entities = append(entities, graph.Entity{
			ID: fmt.Sprintf("host:%04d", i), Kind: "host",
		})
	}
	if err := g.ApplyCommit(graph.Commit{
		ID: "seed", Version: 1,
		Mutations: graph.Mutations{UpsertEntities: entities},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := Execute(g, Request{
		Op: "match", Kind: "host", Limit: 2, CostLimit: 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resultEntityIDs(response.Results); len(got) != 2 ||
		got[0] != "host:0000" || got[1] != "host:0001" {
		t.Fatalf("results = %#v", got)
	}
	if response.Stats.Scanned != 3 {
		t.Fatalf("scanned = %d, want one page plus lookahead", response.Stats.Scanned)
	}
}

func TestMaterializedKindPageLegacyCursorPastEndIsEmpty(t *testing.T) {
	g := graph.New()
	g.Version = 1
	g.Entities["host:a"] = graph.Entity{ID: "host:a", Kind: "host"}
	response, err := Execute(g, Request{
		Op:        "match",
		Kind:      "host",
		Cursor:    "10",
		CostLimit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 0 || response.NextCursor != "" {
		t.Fatalf("response = %#v, want empty terminal page", response)
	}
}

func resultEntityIDs(results []Result) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		if result.Entity != nil {
			ids = append(ids, result.Entity.ID)
		}
	}
	return ids
}
