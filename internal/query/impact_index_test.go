package query

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestImpactQueryUsesRelationSemanticsInsteadOfOutEdgeIndex(t *testing.T) {
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "impact-reverse-relation",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name:            "owned_by",
				FromKind:        "service",
				ToKind:          "team",
				Directed:        true,
				ImpactDirection: "reverse",
				Cardinality:     graph.ManyToMany,
			}},
			UpsertEntities: []graph.Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "team:platform", Kind: "team"},
			},
			UpsertEdges: []graph.Edge{{
				ID:   "edge:api-owner",
				Type: "owned_by",
				From: "service:api",
				To:   "team:platform",
			}},
		},
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	lookup := &adjacencyLookup{
		entities: map[string]graph.Entity{},
		edges:    map[string][]graph.Edge{},
	}

	response, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		Request{
			Op:        "impact",
			ID:        "team:platform",
			Direction: "out",
			Depth:     1,
			Limit:     10,
		},
		ExecuteOptions{IndexLookup: lookup},
	)
	if err != nil {
		t.Fatalf("impact query: %v", err)
	}
	if len(response.Results) != 1 ||
		response.Results[0].Path == nil ||
		pathEnd(*response.Results[0].Path).ID != "service:api" {
		t.Fatalf("impact results = %#v", response.Results)
	}
	if len(lookup.calls) != 0 {
		t.Fatalf("impact query used out-edge index: calls=%v", lookup.calls)
	}
}

func TestLazyReadRequiresEveryPathStepDirection(t *testing.T) {
	stats := PlannerStats{
		Version:     1,
		EdgeShards:  []PlannerEdgeStat{{RelationType: "links", Shard: "00", EdgeCount: 1}},
		EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: 2}},
	}
	request := Request{
		Op:        "shortest_path",
		ID:        "node:start",
		TargetID:  "node:target",
		Direction: "out",
		Path: PathFilter{Steps: []PathStep{
			{Direction: "out"},
			{Direction: "in"},
		}},
	}
	if SupportsLazyRead(request, stats) {
		t.Fatal("lazy read accepted a path requiring a missing reverse index")
	}
	if SupportsLazyRead(Request{
		Op: "traverse", ID: "node:start",
	}, stats) {
		t.Fatal("lazy read treated an unspecified traverse direction as out-only")
	}
	stats.ReverseEdgeShards = []PlannerEdgeStat{{
		RelationType: "links",
		Shard:        "00",
		EdgeCount:    1,
	}}
	if !SupportsLazyRead(request, stats) {
		t.Fatal("lazy read rejected a path with all required directions indexed")
	}
	if SupportsLazyRead(Request{
		Op: "impact", ID: "node:start", Direction: "out",
	}, stats) {
		t.Fatal("lazy read accepted impact without relation impact semantics")
	}
	stats.EdgeShards[0].ImpactDirection = "forward"
	if !SupportsLazyRead(Request{
		Op: "impact", ID: "node:start", Direction: "out",
	}, stats) {
		t.Fatal("lazy read rejected forward impact with an outgoing index")
	}
	stats.EdgeShards[0].ImpactDirection = "reverse"
	stats.ReverseEdgeShards = nil
	if SupportsLazyRead(Request{
		Op: "impact", ID: "node:start", Direction: "out",
	}, stats) {
		t.Fatal("lazy read accepted reverse impact without a reverse index")
	}
	stats.ReverseEdgeShards = []PlannerEdgeStat{{
		RelationType:    "links",
		ImpactDirection: "reverse",
		Shard:           "00",
		EdgeCount:       1,
	}}
	if !SupportsLazyRead(Request{
		Op: "impact", ID: "node:start", Direction: "out",
	}, stats) {
		t.Fatal("lazy read rejected reverse impact with a reverse index")
	}
}
