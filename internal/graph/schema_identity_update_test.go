package graph

import "testing"

func TestCITypeUpdateRejectsExistingIdentityCollisions(t *testing.T) {
	for _, strategy := range []string{"merge", "reject"} {
		t.Run(strategy, func(t *testing.T) {
			g := New()
			if err := g.ApplyCommit(Commit{
				ID:      "seed-duplicates",
				Version: 1,
				Mutations: Mutations{UpsertEntities: []Entity{
					{ID: "host:a", Kind: "host", Fields: Fields{"hostname": "shared"}},
					{ID: "host:b", Kind: "host", Fields: Fields{"hostname": "shared"}},
				}},
			}); err != nil {
				t.Fatal(err)
			}

			err := g.ApplyCommit(Commit{
				ID:      "add-identity-rule",
				Version: 2,
				Mutations: Mutations{UpsertCITypes: []CIType{{
					Name: "host",
					Fields: map[string]FieldSpec{
						"hostname": {Type: "string"},
					},
					IdentityKeys: []IdentityKey{{
						Name:     "hostname",
						Fields:   []string{"hostname"},
						Strategy: strategy,
					}},
				}}},
			})
			if err == nil {
				t.Fatalf("schema update accepted an existing %s identity collision", strategy)
			}
			if g.Version != 1 {
				t.Fatalf("failed schema update changed graph version to %d", g.Version)
			}
			if _, exists := g.CITypes["host"]; exists {
				t.Fatal("failed schema update changed ci types")
			}
		})
	}
}
