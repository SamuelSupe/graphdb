package graph

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestNormalizeFieldsFastPathMatchesJSONNormalization(t *testing.T) {
	tests := []Fields{
		nil,
		{},
		{
			"nil":    nil,
			"string": "value",
			"bool":   true,
			"float":  1.25,
			"int":    42,
			"int8":   int8(-8),
			"int16":  int16(-16),
			"int32":  int32(-32),
			"int64":  int64(math.MinInt64),
			"uint":   uint(42),
			"uint8":  uint8(8),
			"uint16": uint16(16),
			"uint32": uint32(32),
			"uint64": uint64(math.MaxUint64),
		},
		{
			"object": map[string]any{
				"nested": Fields{"value": 7},
				"array":  []any{"a", 2, false, nil},
			},
		},
		{
			"strings": []string{"a", "b"},
			"ints":    []int{1, 2},
			"int64s":  []int64{3, 4},
			"floats":  []float64{1.5, 2.5},
			"bools":   []bool{true, false},
		},
		{
			"nil_object":  map[string]any(nil),
			"nil_array":   []any(nil),
			"nil_strings": []string(nil),
			"nil_ints":    []int(nil),
		},
		{
			"invalid_utf8_value": string([]byte{0xff}),
			"invalid_utf8_array": []string{string([]byte{0xfd})},
			"object": map[string]any{
				string([]byte{0xfe}): "value",
			},
		},
	}
	for i, fields := range tests {
		got, err := normalizeFields(fields)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		want, err := normalizeFieldsJSONReference(fields)
		if err != nil {
			t.Fatalf("reference case %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("case %d mismatch:\ngot  %#v\nwant %#v", i, got, want)
		}
	}
}

func TestNormalizeFieldsFallsBackToJSON(t *testing.T) {
	type customField struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	fields := Fields{
		"custom": customField{Name: "node", Count: 3},
		"raw":    json.RawMessage(`{"enabled":true}`),
	}
	got, err := normalizeFields(fields)
	if err != nil {
		t.Fatal(err)
	}
	want, err := normalizeFieldsJSONReference(fields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback mismatch:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestNormalizeFieldsFastPathPreservesJSONErrors(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	for name, fields := range map[string]Fields{
		"nan":      {"value": math.NaN()},
		"infinite": {"value": math.Inf(1)},
		"channel":  {"value": make(chan int)},
		"cycle":    {"value": cycle},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeFields(fields); err == nil {
				t.Fatal("expected JSON compatibility error")
			}
		})
	}
}

func normalizeFieldsJSONReference(fields Fields) (Fields, error) {
	if fields == nil {
		return Fields{}, nil
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	var decoded Fields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	if decoded == nil {
		return Fields{}, nil
	}
	return decoded, nil
}
