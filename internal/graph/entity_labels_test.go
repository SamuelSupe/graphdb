package graph

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestEntityLabelsJSONUsesCompatibleReservedField(t *testing.T) {
	var entity Entity
	if err := json.Unmarshal([]byte(`{"id":"doc:1","kind":"document","labels":["beta"," alpha ","beta"]}`), &entity); err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha", "beta"}
	if got := EntityLabels(entity); !reflect.DeepEqual(got, want) {
		t.Fatalf("labels = %#v, want %#v", got, want)
	}
	if got := entity.Fields[ReservedLabelsField]; !reflect.DeepEqual(got, []any{"alpha", "beta"}) {
		t.Fatalf("reserved labels = %#v", got)
	}

	encoded, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wire["labels"], []any{"alpha", "beta"}) {
		t.Fatalf("top-level labels = %#v", wire["labels"])
	}
	fields := wire["fields"].(map[string]any)
	if !reflect.DeepEqual(fields[ReservedLabelsField], []any{"alpha", "beta"}) {
		t.Fatalf("compatible fields = %#v", fields)
	}
}

func TestEntityLabelsJSONRejectsInvalidOrConflictingValues(t *testing.T) {
	tests := []string{
		`{"id":"doc:1","kind":"document","labels":[""]}`,
		`{"id":"doc:1","kind":"document","labels":"document"}`,
		`{"id":"doc:1","kind":"document","labels":["document"],"fields":{"__graphdb_labels":["article"]}}`,
	}
	for _, input := range tests {
		var entity Entity
		if err := json.Unmarshal([]byte(input), &entity); err == nil {
			t.Fatalf("json.Unmarshal(%s) unexpectedly succeeded", input)
		}
	}
}

func TestEntityLabelsPreservesLegacyInvalidReservedField(t *testing.T) {
	var entity Entity
	if err := json.Unmarshal([]byte(`{"id":"legacy:1","kind":"legacy","fields":{"__graphdb_labels":"legacy-value"}}`), &entity); err != nil {
		t.Fatal(err)
	}
	if entity.Fields[ReservedLabelsField] != "legacy-value" {
		t.Fatalf("legacy field changed: %#v", entity.Fields)
	}
	if labels := EntityLabels(entity); labels != nil {
		t.Fatalf("invalid legacy field exposed as labels: %#v", labels)
	}
}

func TestEntityJSONValueMatchesMarshalJSONWithEscapingAndSources(t *testing.T) {
	now := time.Date(2026, 9, 7, 8, 9, 10, 123456789, time.UTC)
	entity := Entity{
		ID:   "doc:escaped",
		Kind: "document",
		Fields: Fields{
			"title":             "<admin>&\"",
			ReservedLabelsField: []any{"zeta", "alpha"},
		},
		FieldSources: map[string]FieldSource{
			"title": {Source: "catalog", Priority: 80, Confidence: 0.9, Version: 4, UpdatedAt: now},
		},
		Source:     "catalog",
		ExternalID: "doc-escaped",
		Version:    4,
		UpdatedAt:  now,
	}

	type legacyEntityAlias Entity
	legacyReference := struct {
		legacyEntityAlias
		Labels []string `json:"labels,omitempty"`
	}{
		legacyEntityAlias: legacyEntityAlias(entity),
		Labels:            []string{"alpha", "zeta"},
	}
	expected, err := json.Marshal(legacyReference)
	if err != nil {
		t.Fatalf("marshal independent entity reference: %v", err)
	}
	legacy, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("marshal entity: %v", err)
	}
	if !bytes.Equal(legacy, expected) {
		t.Fatalf("MarshalJSON differs from independent reference:\nlegacy=%s\nreference=%s", legacy, expected)
	}
	value, err := json.Marshal(entity.JSONValue())
	if err != nil {
		t.Fatalf("marshal entity JSONValue: %v", err)
	}
	if !bytes.Equal(value, expected) {
		t.Fatalf("JSONValue differs from independent reference:\nvalue=%s\nreference=%s", value, expected)
	}
	if !bytes.Contains(value, []byte(`\u003cadmin\u003e\u0026`)) {
		t.Fatalf("JSONValue did not preserve HTML escaping: %s", value)
	}

	var wire map[string]any
	if err := json.Unmarshal(value, &wire); err != nil {
		t.Fatalf("decode JSONValue: %v", err)
	}
	if !reflect.DeepEqual(wire["labels"], []any{"alpha", "zeta"}) {
		t.Fatalf("labels = %#v", wire["labels"])
	}
	fields, ok := wire["fields"].(map[string]any)
	if !ok || fields["title"] != `<admin>&"` {
		t.Fatalf("fields = %#v", wire["fields"])
	}
	fieldSources, ok := wire["field_sources"].(map[string]any)
	if !ok || fieldSources["title"] == nil {
		t.Fatalf("field_sources = %#v", wire["field_sources"])
	}
}
