package graph

import "testing"

func TestUpsertRejectsConflictingIdentityOwners(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-conflicting-identity-owners",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"hostname": {Type: "string"},
				},
				IdentityKeys: []IdentityKey{{
					Name:   "hostname",
					Fields: []string{"hostname"},
				}},
			}},
			UpsertEntities: []Entity{
				{
					ID: "host:a", Kind: "host",
					Source: "aws", ExternalID: "i-a",
					Fields: Fields{"hostname": "shared"},
				},
				{
					ID: "host:b", Kind: "host",
					Source: "agent", ExternalID: "agent-b",
					Fields: Fields{"hostname": "other"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "conflicting-identity-owners",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID: "host:incoming", Kind: "host",
				Source: "agent", ExternalID: "agent-b",
				Fields: Fields{"hostname": "shared"},
			}},
		},
	})
	if err == nil {
		t.Fatal("upsert accepted identities owned by different entities")
	}
	if g.Version != 1 || g.Entities.Len() != 2 {
		t.Fatalf(
			"failed upsert changed graph version/entities: %d/%d",
			g.Version,
			g.Entities.Len(),
		)
	}
	id, _, err := g.findEntityByIdentity(Entity{
		Kind: "host", Source: "agent", ExternalID: "agent-b",
	})
	if err != nil || id != "host:b" {
		t.Fatalf("source identity resolved to %q, err=%v", id, err)
	}
}

func TestIdentityComponentsCannotAliasThroughDelimiters(t *testing.T) {
	for _, source := range []bool{false, true} {
		g := New()
		a := Entity{ID: "first", Kind: "host", Fields: Fields{"a": "foo|b=s:bar", "b": "baz"}}
		b := Entity{ID: "second", Kind: "host", Fields: Fields{"a": "foo", "b": "bar|b=s:baz"}}
		schema := CIType{Name: "host", IdentityKeys: []IdentityKey{{Name: "pair", Fields: []string{"a", "b"}}}}
		if source {
			schema.IdentityKeys = nil
			a.Source, a.ExternalID = "alpha|external_id=beta", "gamma"
			b.Source, b.ExternalID = "alpha", "beta|external_id=gamma"
		}
		if err := g.ApplyCommit(Commit{Version: 1, Mutations: Mutations{UpsertCITypes: []CIType{schema}, UpsertEntities: []Entity{a}}}); err != nil {
			t.Fatal(err)
		}
		copy, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{Version: 2, Mutations: Mutations{UpsertEntities: []Entity{b}}}, ApplyOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if g.Entities.Len() != 1 || copy.Entities.Len() != 2 {
			t.Fatalf("source=%v: counts %d / %d", source, g.Entities.Len(), copy.Entities.Len())
		}
		restored, err := FromSnapshot(copy.Snapshot())
		if err != nil {
			t.Fatal(err)
		}
		b.ID = "same-identity"
		if err := restored.ApplyCommit(Commit{Version: 3, Mutations: Mutations{UpsertEntities: []Entity{b}}}); err != nil {
			t.Fatal(err)
		}
		if restored.Entities.Len() != 2 {
			t.Fatalf("identical identity stopped merging: %d", restored.Entities.Len())
		}
		first, _ := restored.GetEntity("first")
		if first.Fields["a"] != a.Fields["a"] {
			t.Fatalf("unrelated entity overwritten: %+v", first)
		}
	}
}
