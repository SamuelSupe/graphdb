package graph

import "testing"

func TestCISchemaDefaultsValidationAndIdentityMerge(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "cmdb",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"hostname": {Type: "string", Required: true, Unique: true, Indexed: true},
					"region":   {Type: "string", Default: "unknown"},
				},
				IdentityKeys: []IdentityKey{{
					Name:   "hostname",
					Fields: []string{"hostname"},
				}},
			}},
			UpsertEntities: []Entity{
				{
					ID:         "host:a1",
					Kind:       "host",
					Fields:     Fields{"hostname": "app-01"},
					Source:     "aws",
					ExternalID: "i-1",
					Confidence: 0.8,
					SourceRank: 10,
				},
				{
					ID:         "host:duplicate",
					Kind:       "host",
					Fields:     Fields{"hostname": "app-01", "region": "us-east-1"},
					Source:     "agent",
					ExternalID: "agent-1",
					Confidence: 0.9,
					SourceRank: 20,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("apply cmdb commit: %v", err)
	}
	if len(g.Entities) != 1 {
		t.Fatalf("entities = %d, want deduped 1", len(g.Entities))
	}
	entity, ok := g.GetEntity("host:a1")
	if !ok {
		t.Fatal("canonical entity host:a1 missing")
	}
	if entity.Fields["region"] != "us-east-1" {
		t.Fatalf("region = %#v, want higher priority source value", entity.Fields["region"])
	}
	if len(entity.Sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(entity.Sources))
	}
	if len(entity.MergedFrom) != 1 || entity.MergedFrom[0] != "host:duplicate" {
		t.Fatalf("merged_from = %#v", entity.MergedFrom)
	}
}

func TestCISchemaRejectsMissingRequiredAndUniqueViolation(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "service",
				Fields: map[string]FieldSpec{
					"name": {Type: "string", Required: true, Unique: true},
				},
			}},
			UpsertEntities: []Entity{{ID: "svc:1", Kind: "service"}},
		},
	})
	if err == nil {
		t.Fatal("expected missing required field error")
	}

	err = g.ApplyCommit(Commit{
		ID:      "unique",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "service",
				Fields: map[string]FieldSpec{
					"name": {Type: "string", Required: true, Unique: true},
				},
			}},
			UpsertEntities: []Entity{
				{ID: "svc:1", Kind: "service", Fields: Fields{"name": "api"}},
				{ID: "svc:2", Kind: "service", Fields: Fields{"name": "api"}},
			},
		},
	})
	if err == nil {
		t.Fatal("expected unique violation")
	}
}

func TestNumericSchemaEnumAndUniqueUseJSONNumberSemantics(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "numeric-schema",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"cpu": {Type: "number", Unique: true, Enum: []any{8}},
				},
			}},
			UpsertEntities: []Entity{{ID: "host:a", Kind: "host", Fields: Fields{"cpu": 8.0}}},
		},
	})
	if err != nil {
		t.Fatalf("numeric enum should accept equivalent JSON number: %v", err)
	}
	err = g.ApplyCommit(Commit{
		ID:        "numeric-unique",
		Version:   2,
		Mutations: Mutations{UpsertEntities: []Entity{{ID: "host:b", Kind: "host", Fields: Fields{"cpu": 8}}}},
	})
	if err == nil {
		t.Fatal("expected unique violation for equivalent numeric values")
	}
}

func TestUniqueFieldDistinguishesMissingFromExplicitNull(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "optional-unique-null",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "service",
				Fields: map[string]FieldSpec{
					"external_id": {Type: "any", Unique: true},
				},
			}},
			UpsertEntities: []Entity{
				{ID: "svc:missing", Kind: "service"},
				{ID: "svc:null", Kind: "service", Fields: Fields{"external_id": nil}},
			},
		},
	})
	if err != nil {
		t.Fatalf("missing field should not collide with explicit null: %v", err)
	}

	err = g.ApplyCommit(Commit{
		ID:      "duplicate-null",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "svc:null-2", Kind: "service", Fields: Fields{"external_id": nil}}},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate explicit null unique violation")
	}
}

func TestMatchEntitiesTreatsEquivalentNumericValuesAsEqual(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "numeric-match",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "host:a", Kind: "host", Fields: Fields{"cpu": 8}}},
		},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	results := g.MatchEntities("host", Fields{"cpu": 8.0})
	if len(results) != 1 || results[0].ID != "host:a" {
		t.Fatalf("results = %#v, want host:a", results)
	}
}

func TestCITypeInheritanceAppliesParentFields(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "inheritance",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{
				{
					Name: "asset",
					Fields: map[string]FieldSpec{
						"env": {Type: "string", Default: "prod", Enum: []any{"prod", "staging"}},
					},
				},
				{
					Name:    "host",
					Extends: []string{"asset"},
					Fields: map[string]FieldSpec{
						"hostname": {Type: "string", Required: true},
					},
				},
			},
			UpsertEntities: []Entity{{ID: "host:1", Kind: "host", Fields: Fields{"hostname": "app-01"}}},
		},
	})
	if err != nil {
		t.Fatalf("apply inherited ci type: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["env"] != "prod" {
		t.Fatalf("inherited default env = %#v, want prod", entity.Fields["env"])
	}
}

func TestCITypeDiamondInheritanceIsNotTreatedAsCycle(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "diamond-inheritance",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{
				{Name: "asset", Fields: map[string]FieldSpec{"env": {Type: "string", Default: "prod"}}},
				{Name: "compute", Extends: []string{"asset"}, Fields: map[string]FieldSpec{"cpu": {Type: "number", Default: 4}}},
				{Name: "networked", Extends: []string{"asset"}, Fields: map[string]FieldSpec{"ip": {Type: "string"}}},
				{Name: "host", Extends: []string{"compute", "networked"}, Fields: map[string]FieldSpec{"hostname": {Type: "string", Required: true}}},
			},
			UpsertEntities: []Entity{{ID: "host:1", Kind: "host", Fields: Fields{"hostname": "app-01", "ip": "10.0.0.1"}}},
		},
	})
	if err != nil {
		t.Fatalf("apply diamond inheritance: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["env"] != "prod" || entity.Fields["cpu"] != 4 {
		t.Fatalf("inherited fields = %#v", entity.Fields)
	}
}

func TestCITypeInheritanceCycleStillRejected(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "inheritance-cycle",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{
				{Name: "asset", Extends: []string{"host"}},
				{Name: "host", Extends: []string{"asset"}},
			},
			UpsertEntities: []Entity{{ID: "host:1", Kind: "host"}},
		},
	})
	if err == nil {
		t.Fatal("expected inheritance cycle error")
	}
}

func TestCITypeInvalidInheritanceRejectedWithoutEntities(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "missing-parent",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{Name: "host", Extends: []string{"asset"}}},
		},
	})
	if err == nil {
		t.Fatal("expected missing parent error")
	}

	err = g.ApplyCommit(Commit{
		ID:      "cycle",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{
				{Name: "asset", Extends: []string{"host"}},
				{Name: "host", Extends: []string{"asset"}},
			},
		},
	})
	if err == nil {
		t.Fatal("expected inheritance cycle error")
	}
}

func TestCITypeDeleteReferencedParentRejected(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{
				{Name: "asset"},
				{Name: "host", Extends: []string{"asset"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed schema: %v", err)
	}
	err = g.ApplyCommit(Commit{
		ID:      "delete-parent",
		Version: 2,
		Mutations: Mutations{
			DeleteCITypes: []string{"asset"},
		},
	})
	if err == nil {
		t.Fatal("expected referenced parent delete error")
	}
}

func TestCITypeSchemaTighteningValidatesExistingEntities(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "host:a", Kind: "host", Fields: Fields{"serial": "same", "cpu": "eight"}},
				{ID: "host:b", Kind: "host", Fields: Fields{"serial": "same"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed entities: %v", err)
	}

	cases := []struct {
		name   string
		fields map[string]FieldSpec
	}{
		{name: "required", fields: map[string]FieldSpec{"owner": {Type: "string", Required: true}}},
		{name: "type", fields: map[string]FieldSpec{"cpu": {Type: "number"}}},
		{name: "unique", fields: map[string]FieldSpec{"serial": {Type: "string", Unique: true}}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			copyGraph := g.Clone()
			err := copyGraph.ApplyCommit(Commit{
				ID:      "tighten-" + tt.name,
				Version: 2,
				Mutations: Mutations{
					UpsertCITypes: []CIType{{Name: "host", Fields: tt.fields}},
				},
			})
			if err == nil {
				t.Fatalf("expected %s schema tightening error", tt.name)
			}
			if copyGraph.Version != 1 {
				t.Fatalf("graph version changed to %d after failed schema tightening", copyGraph.Version)
			}
		})
	}
}

func TestIdentityRejectStrategyAndConfidenceThreshold(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "identity-rules",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "pod",
				Fields: map[string]FieldSpec{
					"uid": {Type: "string", Required: true},
				},
				IdentityKeys: []IdentityKey{{
					Name:                "uid",
					Fields:              []string{"uid"},
					Strategy:            "reject",
					ConfidenceThreshold: 0.7,
				}},
			}},
			UpsertEntities: []Entity{
				{ID: "pod:low-a", Kind: "pod", Fields: Fields{"uid": "u1"}, Confidence: 0.5},
				{ID: "pod:low-b", Kind: "pod", Fields: Fields{"uid": "u1"}, Confidence: 0.5},
			},
		},
	})
	if err != nil {
		t.Fatalf("low-confidence entities should not identity-match: %v", err)
	}
	err = g.ApplyCommit(Commit{
		ID:      "identity-reject",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "pod:high-a", Kind: "pod", Fields: Fields{"uid": "u2"}, Confidence: 0.9},
			{ID: "pod:high-b", Kind: "pod", Fields: Fields{"uid": "u2"}, Confidence: 0.9},
		}},
	})
	if err == nil {
		t.Fatal("expected reject strategy to reject duplicate high-confidence identity")
	}
}

func TestIdentityConfidenceThresholdIgnoresSourcePriority(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "identity-rules",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "pod",
				Fields: map[string]FieldSpec{
					"uid": {Type: "string", Required: true},
				},
				IdentityKeys: []IdentityKey{{
					Name:                "uid",
					Fields:              []string{"uid"},
					Strategy:            "reject",
					ConfidenceThreshold: 0.7,
				}},
			}},
			UpsertEntities: []Entity{
				{ID: "pod:low-a", Kind: "pod", Fields: Fields{"uid": "u1"}, SourceRank: 1000, Confidence: 0.5},
				{ID: "pod:low-b", Kind: "pod", Fields: Fields{"uid": "u1"}, SourceRank: 1000, Confidence: 0.5},
			},
		},
	})
	if err != nil {
		t.Fatalf("source priority should not satisfy confidence threshold: %v", err)
	}
	if len(g.Entities) != 2 {
		t.Fatalf("entities = %d, want low-confidence identities kept separate", len(g.Entities))
	}
}
