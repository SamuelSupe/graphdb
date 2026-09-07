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

func TestMaterializedKindPageWarmOrderUsesBoundedAdmission(t *testing.T) {
	g := graph.New()
	entities := make([]graph.Entity, 0, 5)
	for i := 0; i < 5; i++ {
		entities = append(entities, graph.Entity{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		})
	}
	if err := g.ApplyCommit(graph.Commit{
		ID: "warm-kind-page", Version: 1,
		Mutations: graph.Mutations{UpsertEntities: entities},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	request := Request{
		Op:        "match",
		Kind:      "host",
		Limit:     1,
		CostLimit: 2,
		Profile:   true,
	}
	coldPlan := PlanQuery(g, request)
	if coldPlan.EstimatedRows != 5 || coldPlan.EstimatedCost != 5 {
		t.Fatalf("cold plan rows/cost = %d/%d, want 5/5", coldPlan.EstimatedRows, coldPlan.EstimatedCost)
	}
	if _, err := Execute(g, request); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("cold execute err = %v, want ErrLimitExceeded", err)
	}

	if ids := g.MatchEntityIDs("host"); len(ids) != 5 {
		t.Fatalf("warm ids = %d, want 5", len(ids))
	}
	warmPlan := PlanQuery(g, request)
	if warmPlan.EstimatedRows != 2 || warmPlan.EstimatedCost != 2 {
		t.Fatalf("warm plan rows/cost = %d/%d, want 2/2", warmPlan.EstimatedRows, warmPlan.EstimatedCost)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatalf("warm execute: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:000" {
		t.Fatalf("warm results = %#v", response.Results)
	}
	if response.Stats.Scanned != 2 {
		t.Fatalf("warm scanned = %d, want 2", response.Stats.Scanned)
	}
	if response.NextCursor == "" {
		t.Fatal("warm first page missing cursor")
	}

	nextRequest := request
	nextRequest.Cursor = response.NextCursor
	nextPlan := PlanQuery(g, nextRequest)
	if nextPlan.EstimatedRows != 2 || nextPlan.EstimatedCost != 2 {
		t.Fatalf("warm next-page plan rows/cost = %d/%d, want 2/2", nextPlan.EstimatedRows, nextPlan.EstimatedCost)
	}
	nextResponse, err := Execute(g, nextRequest)
	if err != nil {
		t.Fatalf("warm next-page execute: %v", err)
	}
	if len(nextResponse.Results) != 1 || nextResponse.Results[0].Entity.ID != "host:001" {
		t.Fatalf("warm next-page results = %#v", nextResponse.Results)
	}

	legacyRequest := request
	legacyRequest.Cursor = "1"
	legacyPlan := PlanQuery(g, legacyRequest)
	if legacyPlan.EstimatedRows != 5 || legacyPlan.EstimatedCost != 5 {
		t.Fatalf("legacy plan rows/cost = %d/%d, want 5/5", legacyPlan.EstimatedRows, legacyPlan.EstimatedCost)
	}
	if _, err := Execute(g, legacyRequest); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("legacy execute err = %v, want ErrLimitExceeded", err)
	}

	shardRequest := request
	shardRequest.Cursor = encodeCursor(cursorState{
		Version: g.Version,
		After:   "entity:host:000",
		Offset:  1,
		Query:   cursorQueryHash(request),
		Order:   EntityPageOrderShard,
	})
	shardPlan := PlanQuery(g, shardRequest)
	if shardPlan.EstimatedRows != 5 || shardPlan.EstimatedCost != 5 {
		t.Fatalf("shard-order plan rows/cost = %d/%d, want 5/5", shardPlan.EstimatedRows, shardPlan.EstimatedCost)
	}
	if _, err := Execute(g, shardRequest); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("shard-order execute err = %v, want ErrLimitExceeded", err)
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
