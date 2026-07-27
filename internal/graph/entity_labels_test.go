package graph

import (
	"encoding/json"
	"reflect"
	"testing"
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
