package main

import (
	"fmt"

	"graphdb/internal/graph"
)

func pass(format string, args ...any) {
	fmt.Printf("PASS "+format+"\n", args...)
}

func mustMutations(value any) graph.Mutations {
	mutations, ok := value.(graph.Mutations)
	if !ok {
		panic("expected graph.Mutations")
	}
	return mutations
}

func mapValue(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}

func arrayValue(value any) []any {
	if typed, ok := value.([]any); ok {
		return typed
	}
	return nil
}

func stringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}

func boolValue(value any) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}
	return false
}

func intValue(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return 0
	}
}

func nestedString(root map[string]any, path ...string) string {
	var current any = root
	for _, key := range path {
		current = mapValue(current)[key]
	}
	return stringValue(current)
}

func stringArray(value any) []string {
	items := arrayValue(value)
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, stringValue(item))
	}
	return out
}

func sameStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func hasSuppressedField(body map[string]any, field string) bool {
	for _, item := range arrayValue(body["suppressed"]) {
		if stringValue(mapValue(item)["field"]) == field {
			return true
		}
	}
	return false
}
