package graph

import "testing"

func TestStandardRelationsAllowCMDBCrossKindEdges(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "standard-rel",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "host:app-01", Kind: "host"},
			},
			UpsertEdges: []Edge{{
				ID:   "edge:api-host",
				Type: "runs_on",
				From: "service:api",
				To:   "host:app-01",
			}},
		},
	})
	if err != nil {
		t.Fatalf("standard relation edge: %v", err)
	}
	relations := g.ListRelationTypes()
	found := false
	for _, relation := range relations {
		if relation.Name == "runs_on" && relation.Standard && relation.ImpactDirection == "forward" {
			found = true
		}
	}
	if !found {
		t.Fatal("standard runs_on relation not listed with semantics")
	}
}
