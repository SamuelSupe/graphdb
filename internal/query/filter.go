package query

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func requestFilters(request Request) []Filter {
	filters := append([]Filter(nil), request.Where...)
	for field, value := range request.Filters {
		filters = append(filters, Filter{Field: field, Op: "eq", Value: value})
	}
	return filters
}

func requestEntityMatches(request Request, entity graph.Entity) bool {
	if !entityMatches(entity, request.Where) {
		return false
	}
	for field, expected := range request.Filters {
		actual, exists := entityFilterValue(entity, field)
		if !exists || !valuesEqual(actual, expected) {
			return false
		}
	}
	return entityExprMatches(entity, request.WhereExpr)
}

func equalityFilters(filters []Filter) graph.Fields {
	out := graph.Fields{}
	for _, filter := range filters {
		if filter.Op == "" || filter.Op == "eq" {
			if field, ok := indexedField(filter.Field); ok {
				out[field] = filter.Value
			}
		}
	}
	return out
}

func indexedField(field string) (string, bool) {
	if field == "labels" {
		return graph.ReservedLabelsField, true
	}
	switch field {
	case "", "id", "kind", "source", "external_id", "confidence", "source_priority", "created_at", "updated_at":
		return "", false
	}
	if strings.HasPrefix(field, "identity.") {
		return "", false
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		return name, name != ""
	}
	return field, true
}

func entityMatches(entity graph.Entity, filters []Filter) bool {
	for _, filter := range filters {
		actual, exists := entityFilterValue(entity, filter.Field)
		if !filterMatches(actual, exists, filter) {
			return false
		}
	}
	return true
}

func entityExprMatches(entity graph.Entity, expr *FilterExpr) bool {
	if expr == nil {
		return true
	}
	return filterExprMatches(expr, func(filter Filter) bool {
		actual, exists := entityFilterValue(entity, filter.Field)
		return filterMatches(actual, exists, filter)
	})
}

func edgeMatches(edge graph.Edge, filters []Filter) bool {
	for _, filter := range filters {
		actual, exists := edgeFilterValue(edge, filter.Field)
		if !filterMatches(actual, exists, filter) {
			return false
		}
	}
	return true
}

func requestEdgeMatches(request Request, edge graph.Edge) bool {
	if !edgeMatches(edge, request.EdgeWhere) {
		return false
	}
	return edgeExprMatches(edge, request.EdgeWhereExpr)
}

func edgeExprMatches(edge graph.Edge, expr *FilterExpr) bool {
	if expr == nil {
		return true
	}
	return filterExprMatches(expr, func(filter Filter) bool {
		actual, exists := edgeFilterValue(edge, filter.Field)
		return filterMatches(actual, exists, filter)
	})
}

func filterExprMatches(expr *FilterExpr, leaf func(Filter) bool) bool {
	if expr == nil {
		return true
	}
	op := strings.ToLower(expr.Op)
	if op == "" && expr.Field != "" {
		op = "eq"
	}
	switch op {
	case "and":
		for i := range expr.Children {
			if !filterExprMatches(&expr.Children[i], leaf) {
				return false
			}
		}
		return true
	case "or":
		for i := range expr.Children {
			if filterExprMatches(&expr.Children[i], leaf) {
				return true
			}
		}
		return false
	case "not":
		if len(expr.Children) == 0 {
			return true
		}
		return !filterExprMatches(&expr.Children[0], leaf)
	default:
		return leaf(Filter{Field: expr.Field, Op: op, Value: expr.Value})
	}
}

func filterMatches(actual any, exists bool, filter Filter) bool {
	op := filter.Op
	if op == "" {
		op = "eq"
	}
	if op == "exists" {
		want, ok := filter.Value.(bool)
		if !ok {
			want = true
		}
		return exists == want
	}
	if !exists {
		return false
	}
	switch op {
	case "eq":
		return valuesEqual(actual, filter.Value)
	case "neq":
		return !valuesEqual(actual, filter.Value)
	case "in":
		for _, value := range anySlice(filter.Value) {
			if valuesEqual(actual, value) {
				return true
			}
		}
		return false
	case "gt", "gte", "lt", "lte":
		return compareFilter(actual, filter.Value, op)
	case "prefix":
		return strings.HasPrefix(fmt.Sprint(actual), fmt.Sprint(filter.Value))
	case "contains":
		if values, ok := reflectSlice(actual); ok {
			for _, value := range values {
				if valuesEqual(value, filter.Value) {
					return true
				}
			}
			return false
		}
		return strings.Contains(strings.ToLower(fmt.Sprint(actual)), strings.ToLower(fmt.Sprint(filter.Value)))
	case "fuzzy":
		return fuzzyMatch(filterText(actual), filterText(filter.Value))
	default:
		return false
	}
}

func edgeFilterValue(edge graph.Edge, field string) (any, bool) {
	switch field {
	case "", "id":
		return edge.ID, true
	case "type", "relation_type":
		return edge.Type, true
	case "from":
		return edge.From, true
	case "to":
		return edge.To, true
	case "source":
		return edge.Source, true
	case "external_id":
		return edge.ExternalID, true
	case "confidence":
		return edge.Confidence, true
	case "source_priority":
		return edge.SourceRank, true
	case "created_at":
		return edge.CreatedAt.Format(timeSortLayout), true
	case "updated_at":
		return edge.UpdatedAt.Format(timeSortLayout), true
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		if name == "" {
			return nil, false
		}
		value, ok := edge.Fields[name]
		return value, ok
	}
	value, ok := edge.Fields[field]
	return value, ok
}

func entityFilterValue(entity graph.Entity, field string) (any, bool) {
	if field == "labels" {
		if value, ok := entity.Fields[graph.ReservedLabelsField]; ok {
			return value, true
		}
	}
	switch field {
	case "", "id":
		return entity.ID, true
	case "kind":
		return entity.Kind, true
	case "source":
		return entity.Source, true
	case "external_id":
		return entity.ExternalID, true
	case "confidence":
		return entity.Confidence, true
	case "source_priority":
		return entity.SourceRank, true
	case "created_at":
		return entity.CreatedAt.Format(timeSortLayout), true
	case "updated_at":
		return entity.UpdatedAt.Format(timeSortLayout), true
	}
	if strings.HasPrefix(field, "identity.") {
		name := strings.TrimPrefix(field, "identity.")
		if name == "" {
			return nil, false
		}
		value, ok := entity.Identity[name]
		return value, ok
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		if name == "" {
			return nil, false
		}
		value, ok := entity.Fields[name]
		return value, ok
	}
	value, ok := entity.Fields[field]
	return value, ok
}

func entityValue(entity graph.Entity, field string) any {
	if field == "labels" {
		if value, ok := entity.Fields[graph.ReservedLabelsField]; ok {
			return value
		}
	}
	switch field {
	case "", "id":
		return entity.ID
	case "kind":
		return entity.Kind
	case "source":
		return entity.Source
	case "external_id":
		return entity.ExternalID
	case "confidence":
		return entity.Confidence
	case "source_priority":
		return entity.SourceRank
	case "created_at":
		return entity.CreatedAt.Format(timeSortLayout)
	case "updated_at":
		return entity.UpdatedAt.Format(timeSortLayout)
	}
	if strings.HasPrefix(field, "identity.") {
		name := strings.TrimPrefix(field, "identity.")
		if name == "" {
			return nil
		}
		return entity.Identity[name]
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		if name == "" {
			return nil
		}
		return entity.Fields[name]
	}
	return entity.Fields[field]
}

func compareFilter(actual any, expected any, op string) bool {
	left, leftOK := asFloat(actual)
	right, rightOK := asFloat(expected)
	if leftOK && rightOK {
		return compareFloat(left, right, op)
	}
	leftText := fmt.Sprint(actual)
	rightText := fmt.Sprint(expected)
	switch op {
	case "gt":
		return leftText > rightText
	case "gte":
		return leftText >= rightText
	case "lt":
		return leftText < rightText
	case "lte":
		return leftText <= rightText
	default:
		return false
	}
}

func valuesEqual(left any, right any) bool {
	leftNumber, leftOK := asFloat(left)
	rightNumber, rightOK := asFloat(right)
	if leftOK && rightOK {
		return leftNumber == rightNumber
	}
	return reflect.DeepEqual(left, right)
}

func compareFloat(left, right float64, op string) bool {
	switch op {
	case "gt":
		return left > right
	case "gte":
		return left >= right
	case "lt":
		return left < right
	case "lte":
		return left <= right
	default:
		return false
	}
}

func anySlice(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case []string:
		out := make([]any, len(typed))
		for i, value := range typed {
			out[i] = value
		}
		return out
	default:
		if reflectedSlice, ok := reflectSlice(value); ok {
			return reflectedSlice
		}
		return []any{value}
	}
}

func reflectSlice(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
	default:
		return nil, false
	}
	out := make([]any, reflected.Len())
	for i := 0; i < reflected.Len(); i++ {
		out[i] = reflected.Index(i).Interface()
	}
	return out, true
}

func indexableFilterValues(values []any) bool {
	for _, value := range values {
		if !indexableFilterValue(value) {
			return false
		}
	}
	return true
}

func indexableFilterValue(value any) bool {
	switch value.(type) {
	case nil, string, bool, float64, int, int64, json.Number:
		return true
	default:
		return false
	}
}

func asFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case json.Number:
		value, err := typed.Float64()
		return value, err == nil
	default:
		return 0, false
	}
}

func fuzzyMatch(text, pattern string) bool {
	if pattern == "" {
		return true
	}
	patternOffset := 0
	want, size := utf8.DecodeRuneInString(pattern)
	want = unicode.ToLower(want)
	for _, ch := range text {
		if unicode.ToLower(ch) != want {
			continue
		}
		patternOffset += size
		if patternOffset == len(pattern) {
			return true
		}
		want, size = utf8.DecodeRuneInString(pattern[patternOffset:])
		want = unicode.ToLower(want)
	}
	return false
}

func filterText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}
