package query

import (
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestNeighborsGroupByIsNotSkippedByEarlyPaging(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "neighbors-group",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:start", Kind: "service"},
				{ID: "host:a", Kind: "host", Fields: graph.Fields{"region": "east"}},
				{ID: "host:b", Kind: "host", Fields: graph.Fields{"region": "west"}},
			},
			UpsertEdges: []graph.Edge{
				{ID: "runs:a", Type: "runs_on", From: "service:start", To: "host:a"},
				{ID: "runs:b", Type: "runs_on", From: "service:start", To: "host:b"},
			},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	response, err := Execute(g, Request{
		Op:        "neighbors",
		ID:        "service:start",
		Direction: "out",
		GroupBy:   []string{"region"},
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 1 || response.NextCursor == "" {
		t.Fatalf("page = %#v cursor=%q, want one result and next cursor", response.Results, response.NextCursor)
	}
	if len(response.Groups) != 2 {
		t.Fatalf("groups = %#v, want east and west", response.Groups)
	}
	for _, group := range response.Groups {
		if group.Aggregates["count"] != 1 {
			t.Fatalf("group = %#v, want default count 1", group)
		}
	}
}

func TestNeighborSortAndAggregationCanUseEdgeFields(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID: "neighbor-edge-fields", Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:start", Kind: "service"},
				{ID: "host:a", Kind: "host", Fields: graph.Fields{"rank": 1}},
				{ID: "host:b", Kind: "host", Fields: graph.Fields{"rank": 2, "optional": nil}},
			},
			UpsertEdges: []graph.Edge{
				{Type: "runs_on", From: "service:start", To: "host:a", Fields: graph.Fields{"weight": 2, "rank": 20, "optional": 5}},
				{Type: "runs_on", From: "service:start", To: "host:b", Fields: graph.Fields{"weight": 4, "rank": 10, "optional": 100}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := Execute(g, Request{
		Op: "neighbors", ID: "service:start", Direction: "out",
		Sort: []SortSpec{{Field: "weight", Desc: true}}, GroupBy: []string{"type"},
		Aggregate: []Aggregation{{Op: "sum", Field: "weight"}, {Op: "sum", Field: "rank"}, {Op: "sum", Field: "optional"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Aggregates["sum_weight"] != float64(6) || response.Aggregates["sum_rank"] != float64(3) {
		t.Fatalf("aggregates = %#v, want edge weight sum 6 and entity rank sum 3", response.Aggregates)
	}
	if response.Aggregates["sum_optional"] != float64(5) {
		t.Fatalf("null entity field yielded to the edge: %+v", response.Aggregates)
	}
	if len(response.Groups) != 1 || response.Groups[0].Key["type"] != "runs_on" {
		t.Fatalf("groups = %#v, want runs_on edge type", response.Groups)
	}
	if len(response.Results) != 2 || response.Results[0].Entity.ID != "host:b" || response.Results[1].Entity.ID != "host:a" {
		t.Fatalf("results = %#v, want host:b then host:a by edge weight", response.Results)
	}
}
