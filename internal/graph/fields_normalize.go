package graph

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

const fastFieldNormalizeMaxDepth = 64

func normalizeFields(fields Fields) (Fields, error) {
	if fields == nil {
		return Fields{}, nil
	}
	decoded := make(Fields, len(fields))
	for field, value := range fields {
		if !utf8.ValidString(field) {
			return normalizeFieldsWithJSON(fields)
		}
		normalized, ok := normalizeJSONValueFast(value, 0)
		if !ok {
			return normalizeFieldsWithJSON(fields)
		}
		decoded[field] = normalized
	}
	return validateNormalizedFields(decoded)
}

func normalizeFieldsWithJSON(fields Fields) (Fields, error) {
	data, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	var decoded Fields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	if decoded == nil {
		decoded = Fields{}
	}
	return validateNormalizedFields(decoded)
}

func validateNormalizedFields(fields Fields) (Fields, error) {
	for field := range fields {
		if strings.TrimSpace(field) == "" {
			return nil, fmt.Errorf("field name is required")
		}
	}
	return fields, nil
}

func normalizeJSONValueFast(value any, depth int) (any, bool) {
	if depth > fastFieldNormalizeMaxDepth {
		return nil, false
	}
	switch typed := value.(type) {
	case nil, bool:
		return typed, true
	case string:
		if !utf8.ValidString(typed) {
			return nil, false
		}
		return typed, true
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, false
		}
		return typed, true
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case Fields:
		if typed == nil {
			return nil, true
		}
		return normalizeJSONObjectFast(map[string]any(typed), depth+1)
	case map[string]any:
		if typed == nil {
			return nil, true
		}
		return normalizeJSONObjectFast(typed, depth+1)
	case []any:
		if typed == nil {
			return nil, true
		}
		return normalizeJSONArrayFast(typed, depth+1)
	case []string:
		if typed == nil {
			return nil, true
		}
		out := make([]any, len(typed))
		for i, item := range typed {
			if !utf8.ValidString(item) {
				return nil, false
			}
			out[i] = item
		}
		return out, true
	case []int:
		if typed == nil {
			return nil, true
		}
		return normalizeIntegerSlice(typed), true
	case []int64:
		if typed == nil {
			return nil, true
		}
		return normalizeIntegerSlice(typed), true
	case []float64:
		if typed == nil {
			return nil, true
		}
		out := make([]any, len(typed))
		for i, item := range typed {
			if math.IsNaN(item) || math.IsInf(item, 0) {
				return nil, false
			}
			out[i] = item
		}
		return out, true
	case []bool:
		if typed == nil {
			return nil, true
		}
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func normalizeJSONObjectFast(
	values map[string]any,
	depth int,
) (map[string]any, bool) {
	out := make(map[string]any, len(values))
	for key, value := range values {
		if !utf8.ValidString(key) {
			return nil, false
		}
		normalized, ok := normalizeJSONValueFast(value, depth)
		if !ok {
			return nil, false
		}
		out[key] = normalized
	}
	return out, true
}

func normalizeJSONArrayFast(values []any, depth int) ([]any, bool) {
	out := make([]any, len(values))
	for i, value := range values {
		normalized, ok := normalizeJSONValueFast(value, depth)
		if !ok {
			return nil, false
		}
		out[i] = normalized
	}
	return out, true
}

func normalizeIntegerSlice[T ~int | ~int64](values []T) []any {
	if values == nil {
		return nil
	}
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = float64(value)
	}
	return out
}
