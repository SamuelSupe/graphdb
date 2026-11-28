package graph

import (
	"reflect"
	"testing"
)

func TestRound7OverrideAliasWinsWhenCanonicalAbsent(t *testing.T) {
	policy := SourcePolicy{
		FieldAliases: []FieldAliasRule{
			{Source: "cmdb", Kind: "host", Aliases: map[string]string{"a_tags": "tags", "z_tags": "tags"}},
		},
	}
	entity := Entity{
		ID:     "h1",
		Kind:   "host",
		Source: "cmdb",
		Fields: Fields{
			"a_tags": []any{"old"},
			"z_tags": []any{"override"},
		},
		FieldWriteModes: map[string]string{
			"z_tags": FieldMergeReplace,
		},
	}
	conflicts := applyFieldAliasesToEntity(&entity, policy)
	if !reflect.DeepEqual(entity.Fields["tags"], []any{"override"}) {
		t.Fatalf("expected override alias z_tags! to win over a_tags, got %#v", entity.Fields["tags"])
	}
	if entity.FieldWriteModes["tags"] != FieldMergeReplace {
		t.Fatalf("expected FieldMergeReplace mode to be moved to canonical field, got %#v", entity.FieldWriteModes)
	}
	if _, exists := entity.Fields["a_tags"]; exists {
		t.Fatalf("expected losing alias a_tags to be removed")
	}
	if _, exists := entity.Fields["z_tags"]; exists {
		t.Fatalf("expected winning alias z_tags to be removed")
	}
	if len(conflicts) != 1 || conflicts[0].AliasField != "a_tags" {
		t.Fatalf("expected 1 conflict for ignored a_tags, got %+v", conflicts)
	}
}
