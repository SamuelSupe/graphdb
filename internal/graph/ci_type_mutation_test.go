package graph

import "testing"

func TestParentCITypeUpdateRevalidatesDescendantEntities(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-inheritance",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{
				{Name: "asset"},
				{Name: "host", Extends: []string{"asset"}},
			},
			UpsertEntities: []Entity{{
				ID: "host:a", Kind: "host",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "tighten-parent",
		Version: 2,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "asset",
				Fields: map[string]FieldSpec{
					"serial": {Type: "string", Required: true},
				},
			}},
		},
	})
	if err == nil {
		t.Fatal("parent schema update accepted an invalid descendant entity")
	}
	if g.Version != 1 {
		t.Fatalf("failed schema update changed version to %d", g.Version)
	}
}

func TestEffectiveFieldsPreservesParentOrderWithSharedAncestor(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "ordered-diamond",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{
			{
				Name: "root",
				Fields: map[string]FieldSpec{
					"value": {Type: "string", Default: "root"},
				},
			},
			{
				Name:    "left",
				Extends: []string{"root"},
				Fields: map[string]FieldSpec{
					"value": {Type: "string", Default: "left"},
				},
			},
			{
				Name:    "right",
				Extends: []string{"root"},
				Fields: map[string]FieldSpec{
					"value": {Type: "string", Default: "right"},
				},
			},
			{
				Name:    "leaf",
				Extends: []string{"left", "right"},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	fields, err := g.EffectiveFields("leaf")
	if err != nil {
		t.Fatal(err)
	}
	if got := fields["value"].Default; got != "right" {
		t.Fatalf("leaf default = %#v, want later parent override", got)
	}
}

func TestCITypeUpdateRebuildsOnlyAffectedIdentityKinds(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-identities",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{
					ID: "host:a", Kind: "host",
					Source: "agent", ExternalID: "host-a",
					Fields: Fields{"hostname": "app-a"},
				},
				{
					ID: "service:a", Kind: "service",
					Source: "catalog", ExternalID: "service-a",
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := g.ApplyCommit(Commit{
		ID:      "add-host-identity",
		Version: 2,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"hostname": {Type: "string"},
				},
				IdentityKeys: []IdentityKey{{
					Name: "hostname", Fields: []string{"hostname"},
				}},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	hostID, _, err := g.findEntityByIdentity(Entity{
		Kind: "host", Fields: Fields{"hostname": "app-a"},
	})
	if err != nil || hostID != "host:a" {
		t.Fatalf("host identity resolved to %q, err=%v", hostID, err)
	}
	serviceID, _, err := g.findEntityByIdentity(Entity{
		Kind: "service", Source: "catalog", ExternalID: "service-a",
	})
	if err != nil || serviceID != "service:a" {
		t.Fatalf("service identity resolved to %q, err=%v", serviceID, err)
	}
}

func TestUnusedCITypeUpdateKeepsExistingIndexes(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-indexes",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID: "node:a", Kind: "node",
				Source: "agent", ExternalID: "node-a",
				Fields: Fields{"name": "a"},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	next, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "add-unused-type",
		Version: 2,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{Name: "unused"}},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	matches := next.MatchFieldIndex("node", "name", []any{"a"})
	if len(matches) != 1 || matches[0].ID != "node:a" {
		t.Fatalf("field index returned %#v", matches)
	}
	id, _, err := next.findEntityByIdentity(Entity{
		Kind: "node", Source: "agent", ExternalID: "node-a",
	})
	if err != nil || id != "node:a" {
		t.Fatalf("source identity resolved to %q, err=%v", id, err)
	}
}
