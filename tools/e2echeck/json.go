package main

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
