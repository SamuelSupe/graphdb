package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestEntityPredicateEnforcedAcrossAllQueryOps(t *testing.T) {
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "c1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{
				{Name: "service", Fields: map[string]graph.FieldSpec{"env": {Type: "string"}}},
			},
			UpsertRelationTypes: []graph.RelationType{
				{Name: "depends_on", FromKinds: []string{"service"}, ToKinds: []string{"service"}, ImpactDirection: "reverse"},
			},
			UpsertEntities: []graph.Entity{
				{ID: "svc:a", Kind: "service", Fields: graph.Fields{"env": "prod"}},
				{ID: "svc:secret", Kind: "service", Fields: graph.Fields{"env": "restricted"}},
				{ID: "svc:b", Kind: "service", Fields: graph.Fields{"env": "prod"}},
				{ID: "svc:c", Kind: "service", Fields: graph.Fields{"env": "prod"}},
			},
			UpsertEdges: []graph.Edge{
				{ID: "e1", Type: "depends_on", From: "svc:a", To: "svc:secret"},
				{ID: "e2", Type: "depends_on", From: "svc:secret", To: "svc:b"},
				{ID: "e3", Type: "depends_on", From: "svc:a", To: "svc:c"},
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	opts := ExecuteOptions{
		EntityPredicate: func(e graph.Entity) bool {
			return e.Fields["env"] != "restricted"
		},
	}

	// 1. match
	resp, err := ExecuteContextWithOptions(context.Background(), g, Request{Op: "match", Kind: "service"}, opts)
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	for _, r := range resp.Results {
		if r.Entity != nil && r.Entity.ID == "svc:secret" {
			t.Fatalf("match leaked restricted entity")
		}
	}

	// 2. neighbors (direct neighbor filtered)
	resp, err = ExecuteContextWithOptions(context.Background(), g, Request{Op: "neighbors", ID: "svc:a", Direction: "out"}, opts)
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Entity.ID != "svc:c" {
		t.Fatalf("neighbors = %+v, want only svc:c", resp.Results)
	}
	// neighbors starting from restricted entity should fail as not found
	if _, err := ExecuteContextWithOptions(context.Background(), g, Request{Op: "neighbors", ID: "svc:secret", Direction: "out"}, opts); err == nil {
		t.Fatalf("neighbors on restricted start entity should fail")
	}

	// 3. traverse (intermediate svc:secret must block reaching svc:b)
	resp, err = ExecuteContextWithOptions(context.Background(), g, Request{Op: "traverse", ID: "svc:a", Direction: "out", Depth: 3}, opts)
	if err != nil {
		t.Fatalf("traverse: %v", err)
	}
	for _, r := range resp.Results {
		for _, e := range r.Path.Entities {
			if e.ID == "svc:secret" || e.ID == "svc:b" {
				t.Fatalf("traverse leaked restricted path: %+v", r.Path.Entities)
			}
		}
	}

	// 4. shortest_path (path via svc:secret must not be found)
	resp, err = ExecuteContextWithOptions(context.Background(), g, Request{Op: "shortest_path", ID: "svc:a", TargetID: "svc:b", Direction: "out", Depth: 3}, opts)
	if err != nil {
		t.Fatalf("shortest_path: %v", err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("shortest_path leaked path through restricted entity: %+v", resp.Results)
	}

	// 5. impact (must not traverse through svc:secret)
	resp, err = ExecuteContextWithOptions(context.Background(), g, Request{Op: "impact", ID: "svc:b", Depth: 3}, opts)
	if err != nil {
		t.Fatalf("impact: %v", err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("impact leaked path through restricted entity: %+v", resp.Results)
	}

	// 6. pattern (must not match path through svc:secret)
	resp, err = ExecuteContextWithOptions(context.Background(), g, Request{
		Op:   "pattern",
		Kind: "service",
		Path: PathFilter{
			Steps: []PathStep{
				{RelationTypes: []string{"depends_on"}, Direction: "out"},
				{RelationTypes: []string{"depends_on"}, Direction: "out"},
			},
		},
	}, opts)
	if err != nil {
		t.Fatalf("pattern: %v", err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("pattern leaked path through restricted entity: %+v", resp.Results)
	}
}

func TestPlannerFanoutScalesByShardEntitiesOnLargeGraph(t *testing.T) {
	g := graph.New()
	g.Version = 10
	// Simulate 200,000 entities and 1,000,000 edges evenly distributed across 64 shards.
	// Average per-node degree is 5, while each shard holds ~15,625 edges.
	stats := PlannerStats{Version: 10}
	for i := 0; i < 64; i++ {
		shard := fmt.Sprintf("%02x", i)
		stats.EntityPages = append(stats.EntityPages, PlannerEntityPageStat{
			Shard:       shard,
			EntityCount: 3125,
		})
		stats.EdgeShards = append(stats.EdgeShards, PlannerEdgeStat{
			RelationType: "depends_on",
			Shard:        shard,
			EdgeCount:    15625,
		})
	}
	plan := PlanQueryWithStats(g, Request{
		Op:        "traverse",
		ID:        "svc:hot",
		Direction: "out",
		Depth:     2,
	}, stats)
	if plan.EstimatedCost > defaultQueryCostLimit {
		t.Fatalf("depth-2 traverse on 1M-edge avg-degree-5 graph falsely exceeded cost limit: estimated_cost=%d limit=%d", plan.EstimatedCost, defaultQueryCostLimit)
	}
	if plan.EstimatedCost > 100 {
		t.Fatalf("expected realistic per-node fanout cost (~30), got %d", plan.EstimatedCost)
	}
}

func TestCompileRequestNormalizesImpactDepthInPlanner(t *testing.T) {
	g := graph.New()
	plan := PlanQueryWithStats(g, Request{
		Op: "impact",
		ID: "svc:a",
	}, PlannerStats{})
	if plan.Op != "impact" || plan.Strategy != "impact-bfs" {
		t.Fatalf("unexpected impact plan: %+v", plan)
	}
	compiled, err := CompileRequest(Request{Op: "impact", ID: "svc:a"})
	if err != nil {
		t.Fatalf("CompileRequest: %v", err)
	}
	if compiled.Normalized.Depth != 4 || compiled.Normalized.DirectionStrategy != "impact" {
		t.Fatalf("expected impact depth=4 and strategy=impact, got %+v", compiled.Normalized)
	}
}
