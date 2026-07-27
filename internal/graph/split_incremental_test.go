package graph

import "testing"

func TestSplitRejectsExistingReplacementID(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-split-id-conflict",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{
					ID: "host:source", Kind: "host",
					Fields: Fields{"owner": "source"},
				},
				{
					ID: "host:existing", Kind: "host",
					Fields: Fields{"owner": "existing"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "split-id-conflict",
		Version: 2,
		Mutations: Mutations{
			SplitEntities: []SplitRequest{{
				SourceID: "host:source",
				Entities: []Entity{{
					ID: "host:existing", Kind: "host",
					Fields: Fields{"owner": "replacement"},
				}},
			}},
		},
	})
	if err == nil {
		t.Fatal("split overwrote an existing replacement ID")
	}
	existing, ok := g.GetEntity("host:existing")
	if !ok || existing.Fields["owner"] != "existing" {
		t.Fatalf("existing entity changed after failed split: %#v", existing)
	}
	if _, ok := g.GetEntity("host:source"); !ok {
		t.Fatal("failed split removed source entity")
	}
}

func TestSplitIndexesEachReplacementBeforeValidatingNext(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-split-identities",
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
			UpsertEntities: []Entity{{
				ID: "host:source", Kind: "host",
				Fields: Fields{"hostname": "source"},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "split-duplicate-replacements",
		Version: 2,
		Mutations: Mutations{
			SplitEntities: []SplitRequest{{
				SourceID: "host:source",
				Entities: []Entity{
					{
						ID: "host:left", Kind: "host",
						Fields: Fields{"hostname": "shared"},
					},
					{
						ID: "host:right", Kind: "host",
						Fields: Fields{"hostname": "shared"},
					},
				},
			}},
		},
	})
	if err == nil {
		t.Fatal("split accepted duplicate identities between replacements")
	}
	if _, ok := g.GetEntity("host:source"); !ok {
		t.Fatal("failed split removed source entity")
	}
}
