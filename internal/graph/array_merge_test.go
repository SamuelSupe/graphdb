package graph

import "testing"

func TestArrayFieldAppendUniqueAndOverrideMarker(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
			"tags": {Type: "array", MergeStrategy: FieldMergeAppendUnique},
		}}}},
	}); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "manual",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", SourceRank: 1000,
			Fields: Fields{"tags": []any{"abc", "bcd"}},
		}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-append",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent", SourceRank: 10,
			Fields: Fields{"tags": []any{"bcd", "aaa"}},
		}}},
	}, ApplyOptions{}); err != nil {
		t.Fatalf("append: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	assertFieldSlice(t, entity.Fields["tags"], []any{"abc", "bcd", "aaa"})
	if owner := entity.FieldSources["tags"]; owner.Source != "manual" || owner.Priority != 1000 {
		t.Fatalf("owner was downgraded: %#v", owner)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-force",
		Version: 4,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent", SourceRank: 10,
			Fields: Fields{"tags!": []any{"forced"}},
		}}},
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("force replace: %v", err)
	}
	entity, _ = g.GetEntity("host:1")
	assertFieldSlice(t, entity.Fields["tags"], []any{"abc", "bcd", "aaa"})
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "tags" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestArrayFieldOverrideMarkerWinsWithinPayload(t *testing.T) {
	g := New()
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "same-payload",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
				"tags": {Type: "array", MergeStrategy: FieldMergeAppendUnique},
			}}},
			UpsertEntities: []Entity{{
				ID: "host:1", Kind: "host", Fields: Fields{"tags": []any{"normal"}, "tags!": []any{"forced"}},
			}},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	assertFieldSlice(t, entity.Fields["tags"], []any{"forced"})
	if _, ok := entity.Fields["tags!"]; ok {
		t.Fatalf("override marker leaked into fields: %#v", entity.Fields)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "tags" || report.Suppressed[0].AliasField != "tags" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestArrayFieldOverrideMarkerBeforeSourceAlias(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		Sources:      []SourcePolicyItem{{Name: "aws", Priority: 50}},
		FieldAliases: []FieldAliasRule{{Source: "aws", Kind: "host", Aliases: map[string]string{"privateIpAddress": "private_ip"}}},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
			"private_ip": {Type: "array", MergeStrategy: FieldMergeAppendUnique},
		}}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "seed",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"private_ip": []any{"10.0.0.1"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "replace-alias",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"privateIpAddress!": []any{"10.0.0.2"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("replace alias: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	assertFieldSlice(t, entity.Fields["private_ip"], []any{"10.0.0.2"})
	if _, ok := entity.Fields["privateIpAddress"]; ok {
		t.Fatalf("alias leaked into fields: %#v", entity.Fields)
	}
}

func TestArrayFieldMergeEntitiesUsesAppendUnique(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
				"tags": {Type: "array", MergeStrategy: FieldMergeAppendUnique},
			}}},
			UpsertEntities: []Entity{
				{ID: "host:target", Kind: "host", SourceRank: 1000, Fields: Fields{"tags": []any{"a", "b"}}},
				{ID: "host:source", Kind: "host", SourceRank: 10, Fields: Fields{"tags": []any{"b", "c"}}},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:        "merge",
		Version:   2,
		Mutations: Mutations{MergeEntities: []MergeRequest{{TargetID: "host:target", SourceIDs: []string{"host:source"}}}},
	}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	entity, _ := g.GetEntity("host:target")
	assertFieldSlice(t, entity.Fields["tags"], []any{"a", "b", "c"})
}

func TestArrayFieldMergeStrategyValidation(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "bad",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
			"owner": {Type: "string", MergeStrategy: FieldMergeAppendUnique},
		}}}},
	})
	if err == nil {
		t.Fatal("expected append_unique on non-array field to fail")
	}
}

func TestArrayFieldAppendUniqueEmptyAndTypeValidation(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
			"tags": {Type: "array", MergeStrategy: FieldMergeAppendUnique},
		}}}},
	}); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "empty-new",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:empty", Kind: "host", Fields: Fields{"tags": []any{}},
		}}},
	}); err != nil {
		t.Fatalf("empty new: %v", err)
	}
	entity, _ := g.GetEntity("host:empty")
	assertFieldSlice(t, entity.Fields["tags"], []any{})

	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:a", Kind: "host", Fields: Fields{"tags": []any{"abc"}},
		}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "empty-existing",
		Version: 4,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:a", Kind: "host", Fields: Fields{"tags": []any{}},
		}}},
	}); err != nil {
		t.Fatalf("empty existing: %v", err)
	}
	entity, _ = g.GetEntity("host:a")
	assertFieldSlice(t, entity.Fields["tags"], []any{"abc"})

	err := g.ApplyCommit(Commit{
		ID:      "bad-value",
		Version: 5,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:bad", Kind: "host", Fields: Fields{"tags": "not-array"},
		}}},
	})
	if err == nil {
		t.Fatal("expected non-array value for array field to fail")
	}
}

func assertFieldSlice(t *testing.T, got any, want []any) {
	t.Helper()
	values, ok := got.([]any)
	if !ok {
		t.Fatalf("value = %#v, want []any", got)
	}
	if len(values) != len(want) {
		t.Fatalf("value = %#v, want %#v", values, want)
	}
	for i := range values {
		if !fieldValuesEqual(values[i], want[i]) {
			t.Fatalf("value = %#v, want %#v", values, want)
		}
	}
}
