package graph

import "testing"

func TestEntityAliasIndexStorageCopyIsolation(t *testing.T) {
	source := New()
	source.Version = 1
	source.Entities["node:current"] = Entity{
		ID:         "node:current",
		Kind:       "node",
		MergedFrom: []string{"node:legacy"},
	}
	source.rebuildIndexes()

	next, _, err := source.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "delete-legacy-alias",
		Version: 2,
		Mutations: Mutations{
			DeleteEntities: []string{"node:legacy"},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved := next.ResolveEntityReference("node:legacy"); resolved != "" {
		t.Fatalf("next alias = %q", resolved)
	}
	if resolved := source.ResolveEntityReference("node:legacy"); resolved != "node:current" {
		t.Fatalf("source alias after copy = %q", resolved)
	}
}

func TestEntityAliasIndexKeepsAmbiguousAliasUnresolved(t *testing.T) {
	g := New()
	g.Entities["node:a"] = Entity{
		ID: "node:a", Kind: "node",
		MergedFrom: []string{"node:legacy"},
	}
	g.Entities["node:b"] = Entity{
		ID: "node:b", Kind: "node",
		MergedFrom: []string{"node:legacy"},
	}
	g.rebuildIndexes()

	if resolved := g.ResolveEntityReference("node:legacy"); resolved != "" {
		t.Fatalf("ambiguous alias resolved to %q", resolved)
	}
}
