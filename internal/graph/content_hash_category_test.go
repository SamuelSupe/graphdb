package graph

import (
	"reflect"
	"testing"
)

func TestUpdateLogicalHashCategoryBatchMergesInKeyOrder(t *testing.T) {
	category := logicalHashCategory{
		keys:    []string{"b", "d", "f"},
		encoded: [][]byte{[]byte(`"old-b"`), []byte(`"old-d"`), []byte(`"old-f"`)},
	}
	values := map[string]any{
		"a": "new-a",
		"d": "new-d",
		"e": "new-e",
	}
	touched := map[string]trackedFingerprint{
		"a": {},
		"b": {},
		"d": {},
		"e": {},
	}
	if err := updateLogicalHashCategoryBatch(
		&category,
		"entity",
		touched,
		func(key string) (any, bool) {
			value, exists := values[key]
			return value, exists
		},
	); err != nil {
		t.Fatalf("update category: %v", err)
	}

	if want := []string{"a", "d", "e", "f"}; !reflect.DeepEqual(category.keys, want) {
		t.Fatalf("keys = %v, want %v", category.keys, want)
	}
	got := make([]string, len(category.encoded))
	for i := range category.encoded {
		got[i] = string(category.encoded[i])
	}
	if want := []string{`"new-a"`, `"new-d"`, `"new-e"`, `"old-f"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("encoded = %v, want %v", got, want)
	}
}
