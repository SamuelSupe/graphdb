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
	if g.Version != 1 || len(g.Entities) != 2 {
		t.Fatalf(
			"failed upsert changed graph version/entities: %d/%d",
			g.Version,
			len(g.Entities),
		)
	}
	id, _, err := g.findEntityByIdentity(Entity{
		Kind: "host", Source: "agent", ExternalID: "agent-b",
	})
	if err != nil || id != "host:b" {
		t.Fatalf("source identity resolved to %q, err=%v", id, err)
	}
}
