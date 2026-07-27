package graph

import "testing"

func TestUniqueScalarIndexAllowsSelfUpdateAndRejectsPeer(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "asset",
				Fields: map[string]FieldSpec{
					"serial": {Type: "string", Unique: true},
				},
			}},
			UpsertEntities: []Entity{{
				ID: "asset:first", Kind: "asset",
				Fields: Fields{"serial": "serial-1"},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "self-update",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "asset:first", Kind: "asset",
			Fields: Fields{"serial": "serial-1", "state": "ready"},
		}}},
	}); err != nil {
		t.Fatalf("self update: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "duplicate",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "asset:second", Kind: "asset",
			Fields: Fields{"serial": "serial-1"},
		}}},
	}); err == nil {
		t.Fatal("duplicate scalar unique value was accepted")
	}
}

func TestUniqueComplexFieldKeepsDeepEqualityFallback(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "asset",
				Fields: map[string]FieldSpec{
					"identity": {Type: "object", Unique: true},
				},
			}},
			UpsertEntities: []Entity{{
				ID: "asset:first", Kind: "asset",
				Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-1"}},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "duplicate",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "asset:second", Kind: "asset",
			Fields: Fields{"identity": map[string]any{"id": "i-1", "provider": "aws"}},
		}}},
	}); err == nil {
		t.Fatal("duplicate complex unique value was accepted")
	}
}

func TestUniqueComplexFieldRejectsDuplicateInSameBatch(t *testing.T) {
	g := graphWithComplexUniqueField(t)
	err := g.ApplyCommit(Commit{
		ID:      "duplicates",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{
			{
				ID: "asset:first", Kind: "asset",
				Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-1"}},
			},
			{
				ID: "asset:second", Kind: "asset",
				Fields: Fields{"identity": map[string]any{"id": "i-1", "provider": "aws"}},
			},
		}},
	})
	if err == nil {
		t.Fatal("duplicate complex unique value in one batch was accepted")
	}
}

func TestUniqueComplexFieldTracksUpdatesWithinBatch(t *testing.T) {
	t.Run("reuse released value", func(t *testing.T) {
		g := graphWithComplexUniqueField(t)
		seedComplexUniqueEntities(t, g)
		if err := g.ApplyCommit(Commit{
			ID:      "reuse",
			Version: 3,
			Mutations: Mutations{UpsertEntities: []Entity{
				{
					ID: "asset:first", Kind: "asset",
					Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-3"}},
				},
				{
					ID: "asset:second", Kind: "asset",
					Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-1"}},
				},
			}},
		}); err != nil {
			t.Fatalf("reuse released complex value: %v", err)
		}
	})

	t.Run("reject newly claimed value", func(t *testing.T) {
		g := graphWithComplexUniqueField(t)
		seedComplexUniqueEntities(t, g)
		err := g.ApplyCommit(Commit{
			ID:      "collision",
			Version: 3,
			Mutations: Mutations{UpsertEntities: []Entity{
				{
					ID: "asset:first", Kind: "asset",
					Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-3"}},
				},
				{
					ID: "asset:second", Kind: "asset",
					Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-3"}},
				},
			}},
		})
		if err == nil {
			t.Fatal("complex value claimed earlier in the batch was accepted")
		}
	})
}

func TestUniqueComplexFieldSchemaUpdateRejectsExistingDuplicates(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "asset",
				Fields: map[string]FieldSpec{
					"identity": {Type: "object"},
				},
			}},
			UpsertEntities: []Entity{
				{
					ID: "asset:first", Kind: "asset",
					Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-1"}},
				},
				{
					ID: "asset:second", Kind: "asset",
					Fields: Fields{"identity": map[string]any{"id": "i-1", "provider": "aws"}},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	err := g.ApplyCommit(Commit{
		ID:      "enable-unique",
		Version: 2,
		Mutations: Mutations{UpsertCITypes: []CIType{{
			Name: "asset",
			Fields: map[string]FieldSpec{
				"identity": {Type: "object", Unique: true},
			},
		}}},
	})
	if err == nil {
		t.Fatal("schema update accepted existing duplicate complex values")
	}
}

func graphWithComplexUniqueField(t *testing.T) *Graph {
	t.Helper()
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{
			Name: "asset",
			Fields: map[string]FieldSpec{
				"identity": {Type: "object", Unique: true},
			},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

func seedComplexUniqueEntities(t *testing.T, g *Graph) {
	t.Helper()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{
			{
				ID: "asset:first", Kind: "asset",
				Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-1"}},
			},
			{
				ID: "asset:second", Kind: "asset",
				Fields: Fields{"identity": map[string]any{"provider": "aws", "id": "i-2"}},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
}
