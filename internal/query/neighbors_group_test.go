package query

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
