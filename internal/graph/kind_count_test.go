package graph

import "testing"

func TestKindCountTracksStorageCopyMutations(t *testing.T) {
	source := New()
	if err := source.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "node:a", Kind: "node"},
				{ID: "node:b", Kind: "node"},
				{ID: "team:a", Kind: "team"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	next, _, err := source.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "mutate",
		Version: 2,
		Mutations: Mutations{
			DeleteEntities: []string{"node:a"},
			UpsertEntities: []Entity{
				{ID: "team:a", Kind: "team", Fields: Fields{"state": "ready"}},
				{ID: "service:a", Kind: "service"},
			},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if got := next.KindCount("node"); got != 1 {
		t.Fatalf("next node count = %d", got)
	}
	if got := next.KindCount("team"); got != 1 {
		t.Fatalf("next team count = %d", got)
	}
	if got := next.KindCount("service"); got != 1 {
		t.Fatalf("next service count = %d", got)
	}
	if got := source.KindCount("node"); got != 2 {
		t.Fatalf("source node count = %d", got)
	}
	if got := source.KindCount("service"); got != 0 {
		t.Fatalf("source service count = %d", got)
	}
}
