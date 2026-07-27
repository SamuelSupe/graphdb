package graph

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func scalarKey(value any) (string, bool) {
	switch v := value.(type) {
	case nil:
		return "null", true
	case string:
		return "s:" + v, true
	case bool:
		if v {
			return "b:true", true
		}
		return "b:false", true
	case float64:
		return fmt.Sprintf("n:%g", v), true
	case int:
		return fmt.Sprintf("n:%d", v), true
	case int64:
		return fmt.Sprintf("n:%d", v), true
	case json.Number:
		value, err := v.Float64()
		if err != nil {
			return "", false
		}
		return fmt.Sprintf("n:%g", value), true
	default:
		return "", false
	}
}

func validFieldType(fieldType string) bool {
	switch fieldType {
	case "any", "string", "number", "bool", "object", "array":
		return true
	default:
		return false
	}
}

func valueMatchesType(value any, fieldType string) bool {
	if fieldType == "" || fieldType == "any" {
		return true
	}
	switch fieldType {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case float64, int, int64, json.Number:
			return true
		default:
			return false
		}
	case "bool":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		if ok {
			return true
		}
		_, ok = value.(Fields)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}

func valueInEnum(value any, enum []any) bool {
	for _, allowed := range enum {
		if fieldValuesEqual(value, allowed) {
			return true
		}
	}
	return false
}

func fieldValuesEqual(left any, right any) bool {
	leftKey, leftOK := scalarKey(left)
	rightKey, rightOK := scalarKey(right)
	if leftOK && rightOK {
		return leftKey == rightKey
	}
	return reflect.DeepEqual(left, right)
}
