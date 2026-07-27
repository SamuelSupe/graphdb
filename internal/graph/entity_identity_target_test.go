package graph

import "testing"

func TestUpsertRejectsExistingIDResolvingToAnotherEntity(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-identity-targets",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"hostname": {Type: "string"},
					"owner":    {Type: "string"},
				},
				IdentityKeys: []IdentityKey{{
					Name:   "hostname",
					Fields: []string{"hostname"},
				}},
			}},
			UpsertRelationTypes: []RelationType{{
				Name:        "link",
				FromKind:    "host",
				ToKind:      "host",
				Cardinality: ManyToMany,
			}},
			UpsertEntities: []Entity{
				{
					ID: "host:a", Kind: "host",
					Fields: Fields{"hostname": "shared"},
				},
				{
					ID: "host:b", Kind: "host",
					Fields: Fields{"hostname": "other"},
				},
				{ID: "host:sink", Kind: "host"},
			},
			UpsertEdges: []Edge{{
				Type: "link", From: "host:b", To: "host:sink",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "ambiguous-identity-update",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID: "host:b", Kind: "host",
				Fields: Fields{
					"hostname": "shared",
					"owner":    "platform",
				},
			}},
		},
	})
	if err == nil {
		t.Fatal("ambiguous identity update was accepted")
	}
	a, _ := g.GetEntity("host:a")
	if _, changed := a.Fields["owner"]; changed {
		t.Fatalf("failed update changed canonical target: %#v", a)
	}
	b, _ := g.GetEntity("host:b")
	if b.Fields["hostname"] != "other" {
		t.Fatalf("failed update changed incoming ID entity: %#v", b)
	}
	neighbors := g.Neighbors("host:b", "out", "link")
	if len(neighbors) != 1 || neighbors[0].Entity.ID != "host:sink" {
		t.Fatalf("failed update changed existing edges: %#v", neighbors)
	}
}
