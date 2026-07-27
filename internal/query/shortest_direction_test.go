package query

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestShortestPathAppliesDirectionForEachStep(t *testing.T) {
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "mixed-direction-path",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name:        "links",
				FromKind:    "node",
				ToKind:      "node",
				Directed:    true,
				Cardinality: graph.ManyToMany,
			}},
			UpsertEntities: []graph.Entity{
				{ID: "node:start", Kind: "node"},
				{ID: "node:middle", Kind: "node"},
				{ID: "node:target", Kind: "node"},
			},
			UpsertEdges: []graph.Edge{
				{
					ID:   "edge:start-middle",
					Type: "links",
					From: "node:start",
					To:   "node:middle",
				},
				{
					ID:   "edge:target-middle",
					Type: "links",
					From: "node:target",
					To:   "node:middle",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	response, err := Execute(g, Request{
		Op:        "shortest_path",
		ID:        "node:start",
		TargetID:  "node:target",
		Direction: "out",
		Depth:     2,
		Path: PathFilter{Steps: []PathStep{
			{Direction: "out", RelationTypes: []string{"links"}},
			{Direction: "in", RelationTypes: []string{"links"}},
		}},
	})
	if err != nil {
		t.Fatalf("shortest path: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Path == nil {
		t.Fatalf("results = %#v, want one mixed-direction path", response.Results)
	}
	path := response.Results[0].Path
	if len(path.Entities) != 3 ||
		path.Entities[0].ID != "node:start" ||
		path.Entities[1].ID != "node:middle" ||
		path.Entities[2].ID != "node:target" {
		t.Fatalf("path entities = %#v", path.Entities)
	}
}
