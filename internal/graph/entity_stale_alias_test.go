package graph

import "testing"

func TestSourceStaleDeleteKeepsEntityWithObservedAlias(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-first-alias",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID: "host:1", Kind: "host",
				Source: "aws", ExternalID: "i-old",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "seed-second-alias",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID: "host:1", Kind: "host",
				Source: "aws", ExternalID: "i-current",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := g.ApplyCommit(Commit{
		ID:      "full-sync",
		Version: 3,
		Mutations: Mutations{
			MarkSourceStale: []SourceStaleRequest{{
				Source:              "aws",
				ObservedExternalIDs: []string{"i-current"},
				Action:              "delete",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	entity, ok := g.GetEntity("host:1")
	if !ok {
		t.Fatal("observed source alias did not protect entity from stale delete")
	}
	for _, source := range entity.Sources {
		if source.Source == "aws" && source.Stale {
			t.Fatalf("observed entity source was marked stale: %#v", entity.Sources)
		}
	}
}
