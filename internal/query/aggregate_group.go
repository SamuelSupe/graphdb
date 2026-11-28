package query

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type groupAccumulator struct {
	groupBy        []string
	specs          []Aggregation
	groups         map[string]*aggregateGroupState
	remaining      *int
	remainingBytes *int
	row            []any
	encoded        []byte
}

type aggregateGroupState struct {
	key map[string]any
	acc *aggregateAccumulator
}

func newGroupAccumulator(groupBy []string, specs []Aggregation) *groupAccumulator {
	if len(groupBy) == 0 {
		return nil
	}
	if len(specs) == 0 {
		specs = []Aggregation{{Op: "count"}}
	}
	remaining := maxAggregateBuckets
	remainingBytes := maxAggregateTotalKeyBytes
	row := make([]any, len(groupBy)*2)
	for i, field := range groupBy {
		row[2*i] = field
	}
	return &groupAccumulator{groupBy: append([]string(nil), groupBy...), specs: append([]Aggregation(nil), specs...), groups: map[string]*aggregateGroupState{}, remaining: &remaining, remainingBytes: &remainingBytes, row: row}
}

func newQueryAggregators(request Request) (*aggregateAccumulator, *groupAccumulator) {
	aggregate := newAggregateAccumulator(request.Aggregate)
	groups := newGroupAccumulator(request.GroupBy, request.Aggregate)
	if aggregate != nil && groups != nil {
		groups.remaining = aggregate.remaining
		groups.remainingBytes = aggregate.remainingBytes
	}
	return aggregate, groups
}

func (a *groupAccumulator) add(result Result) error {
	if a == nil {
		return nil
	}
	group, err := a.groupForValues(func(field string) any { return resultValue(result, field) })
	if err != nil {
		return err
	}
	return group.acc.add(result)
}

func (a *groupAccumulator) addEntity(entity *graph.Entity) error {
	if a == nil {
		return nil
	}
	group, err := a.groupForValues(func(field string) any { return entityValue(entity, field) })
	if err != nil {
		return err
	}
	return group.acc.addEntity(entity)
}

func (a *groupAccumulator) groupForValues(valueFor func(string) any) (*aggregateGroupState, error) {
	for i, field := range a.groupBy {
		val := valueFor(field)
		if text, ok := val.(string); ok && len(text) > maxAggregateKeyBytes {
			return nil, fmt.Errorf("%w: group_by key exceeds %d bytes", ErrLimitExceeded, maxAggregateKeyBytes)
		}
		a.row[2*i+1] = val
	}
	identity, err := a.groupIdentity()
	if err != nil {
		return nil, err
	}
	if group := a.groups[identity]; group != nil {
		return group, nil
	}
	if len(a.groups) >= maxAggregateBuckets || *a.remaining == 0 {
		return nil, fmt.Errorf("%w: group_by supports at most %d buckets", ErrLimitExceeded, maxAggregateBuckets)
	}
	cost := len(identity) * 2
	if a.remainingBytes != nil {
		if *a.remainingBytes < cost {
			return nil, fmt.Errorf("%w: group_by key memory exceeds %d bytes", ErrLimitExceeded, maxAggregateTotalKeyBytes)
		}
		*a.remainingBytes -= cost
	}
	*a.remaining--
	key := make(map[string]any, len(a.groupBy))
	for i, field := range a.groupBy {
		key[field] = cloneGroupKeyValue(a.row[i*2+1])
	}
	group := &aggregateGroupState{key: key, acc: newAggregateAccumulator(a.specs)}
	group.acc.remaining = a.remaining
	group.acc.remainingBytes = a.remainingBytes
	a.groups[identity] = group
	return group, nil
}

func (a *groupAccumulator) groupIdentity() (string, error) {
	encoded := append(a.encoded[:0], '[')
	for i, value := range a.row {
		if i != 0 {
			encoded = append(encoded, ',')
		}
		next, ok := appendGroupScalar(encoded, value)
		if !ok {
			a.encoded = encoded[:0]
			data, err := json.Marshal(a.row)
			if err != nil {
				formatted := fmt.Sprintf("%#v", a.row)
				if len(formatted) > maxAggregateKeyBytes {
					return "", fmt.Errorf("%w: group_by key exceeds %d bytes", ErrLimitExceeded, maxAggregateKeyBytes)
				}
				return formatted, nil
			}
			if len(data) > maxAggregateKeyBytes {
				return "", fmt.Errorf("%w: group_by key exceeds %d bytes", ErrLimitExceeded, maxAggregateKeyBytes)
			}
			return string(data), nil
		}
		if len(next) > maxAggregateKeyBytes {
			a.encoded = encoded[:0]
			return "", fmt.Errorf("%w: group_by key exceeds %d bytes", ErrLimitExceeded, maxAggregateKeyBytes)
		}
		encoded = next
	}
	encoded = append(encoded, ']')
	if len(encoded) > maxAggregateKeyBytes {
		a.encoded = encoded[:0]
		return "", fmt.Errorf("%w: group_by key exceeds %d bytes", ErrLimitExceeded, maxAggregateKeyBytes)
	}
	a.encoded = encoded
	return string(a.encoded), nil
}

// Keep the same JSON bytes used to order groups. Encoding cases that need
// JSON escaping or exponent notation still go through encoding/json.
func appendGroupScalar(dst []byte, value any) ([]byte, bool) {
	switch value := value.(type) {
	case nil:
		return append(dst, "null"...), true
	case bool:
		return strconv.AppendBool(dst, value), true
	case string:
		for i := 0; i < len(value); i++ {
			c := value[i]
			if c < 0x20 || c >= 0x80 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
				return nil, false
			}
		}
		dst = append(dst, '"')
		return append(append(dst, value...), '"'), true
	case int:
		return strconv.AppendInt(dst, int64(value), 10), true
	case int64:
		return strconv.AppendInt(dst, value, 10), true
	case float64:
		absolute := math.Abs(value)
		if value == 0 || absolute >= 1e-6 && absolute < 1e21 {
			return strconv.AppendFloat(dst, value, 'f', -1, 64), true
		}
	}
	return nil, false
}

func cloneGroupKeyValue(value any) any {
	switch typed := value.(type) {
	case nil, bool, string, float64, float32, int, int64, int32, uint, uint64, uint32:
		return typed
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneGroupKeyValue(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, v := range typed {
			out[k] = cloneGroupKeyValue(v)
		}
		return out
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return typed
		}
		var copied any
		if err := json.Unmarshal(data, &copied); err != nil {
			return typed
		}
		return copied
	}
}

func (a *groupAccumulator) results(having []Filter, havingExpr *FilterExpr) []AggregateGroup {
	if a == nil {
		return nil
	}
	keys := make([]string, 0, len(a.groups))
	for key := range a.groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]AggregateGroup, 0, len(keys))
	for _, key := range keys {
		group := a.groups[key]
		item := AggregateGroup{Key: group.key, Aggregates: group.acc.results()}
		if !aggregateGroupMatches(item, having, havingExpr) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func aggregateGroups(results []Result, groupBy []string, specs []Aggregation, having []Filter, havingExpr *FilterExpr) ([]AggregateGroup, error) {
	acc := newGroupAccumulator(groupBy, specs)
	for _, result := range results {
		if err := acc.add(result); err != nil {
			return nil, err
		}
	}
	return acc.results(having, havingExpr), nil
}

func aggregateGroupMatches(group AggregateGroup, filters []Filter, expr *FilterExpr) bool {
	for _, filter := range filters {
		actual, exists := aggregateGroupValue(group, filter.Field)
		if !filterMatches(actual, exists, filter) {
			return false
		}
	}
	if expr == nil {
		return true
	}
	return filterExprMatches(expr, func(filter Filter) bool {
		actual, exists := aggregateGroupValue(group, filter.Field)
		return filterMatches(actual, exists, filter)
	})
}

func aggregateGroupValue(group AggregateGroup, field string) (any, bool) {
	if value, ok := group.Key[field]; ok {
		return value, true
	}
	value, ok := group.Aggregates[field]
	return value, ok
}
