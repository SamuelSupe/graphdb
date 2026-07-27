package query

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestShortestPathFromEntityToItselfReturnsZeroHopPath(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "zero-hop",
		Version: 1,
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "service:self", Kind: "service",
		}}},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	response, err := Execute(g, Request{
		Op:       "shortest_path",
		ID:       "service:self",
		TargetID: "service:self",
		Depth:    4,
	})
	if err != nil {
		t.Fatalf("shortest path: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Path == nil {
		t.Fatalf("results = %#v, want one zero-hop path", response.Results)
	}
	path := response.Results[0].Path
	if len(path.Edges) != 0 ||
		len(path.Entities) != 1 ||
		path.Entities[0].ID != "service:self" {
		t.Fatalf("path = %#v, want only service:self", path)
	}

	constrained, err := Execute(g, Request{
		Op:       "shortest_path",
		ID:       "service:self",
		TargetID: "service:self",
		Path:     PathFilter{Steps: []PathStep{{}}},
	})
	if err != nil {
		t.Fatalf("constrained shortest path: %v", err)
	}
	if len(constrained.Results) != 0 {
		t.Fatalf("zero-hop path ignored required step: %#v", constrained.Results)
	}
}
