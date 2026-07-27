package graph

import "testing"

func TestUpsertRejectsIdentityCollisionFormedAfterFieldMerge(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-composite-upsert",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"region":   {Type: "string"},
					"hostname": {Type: "string"},
				},
				IdentityKeys: []IdentityKey{{
					Name:   "region-hostname",
					Fields: []string{"region", "hostname"},
				}},
			}},
			UpsertEntities: []Entity{
				{
					ID: "host:a", Kind: "host",
					Source: "aws", ExternalID: "i-a",
					Fields: Fields{"region": "us-east"},
				},
				{
					ID: "host:b", Kind: "host",
					Fields: Fields{
						"region":   "us-east",
						"hostname": "app-1",
					},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "complete-composite-identity",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID: "host:a-update", Kind: "host",
				Source: "aws", ExternalID: "i-a",
				Fields: Fields{"hostname": "app-1"},
			}},
		},
	})
	if err == nil {
		t.Fatal("upsert accepted a composite identity collision")
	}
	a, _ := g.GetEntity("host:a")
	if _, changed := a.Fields["hostname"]; changed {
		t.Fatalf("failed upsert changed target entity: %#v", a)
	}
	id, _, err := g.findEntityByIdentity(Entity{
		Kind: "host",
		Fields: Fields{
			"region":   "us-east",
			"hostname": "app-1",
		},
	})
	if err != nil || id != "host:b" {
		t.Fatalf("composite identity resolved to %q, err=%v", id, err)
	}
}
