package query

import (
	"encoding/json"
	"fmt"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type groupAccumulator struct {
	groupBy []string
	specs   []Aggregation
	groups  map[string]*aggregateGroupState
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
	return &groupAccumulator{groupBy: append([]string(nil), groupBy...), specs: append([]Aggregation(nil), specs...), groups: map[string]*aggregateGroupState{}}
}

func (a *groupAccumulator) add(result Result) error {
	if a == nil {
		return nil
	}
	key, identity := aggregateGroupKey(result, a.groupBy)
	group, err := a.groupFor(key, identity)
	if err != nil {
		return err
	}
	return group.acc.add(result)
}

func (a *groupAccumulator) addEntity(entity graph.Entity) error {
	if a == nil {
		return nil
	}
	key, identity := aggregateGroupEntityKey(entity, a.groupBy)
	group, err := a.groupFor(key, identity)
	if err != nil {
		return err
	}
	return group.acc.addEntity(entity)
}

func (a *groupAccumulator) groupFor(key map[string]any, identity string) (*aggregateGroupState, error) {
	group := a.groups[identity]
	if group == nil {
		if len(a.groups) >= maxAggregateBuckets {
			return nil, fmt.Errorf("%w: group_by supports at most %d buckets", ErrLimitExceeded, maxAggregateBuckets)
		}
		group = &aggregateGroupState{key: key, acc: newAggregateAccumulator(a.specs)}
		a.groups[identity] = group
	}
	return group, nil
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

func aggregateGroupKey(result Result, fields []string) (map[string]any, string) {
	return aggregateGroupKeyValues(fields, func(field string) any {
		return resultValue(result, field)
	})
}

func aggregateGroupEntityKey(entity graph.Entity, fields []string) (map[string]any, string) {
	return aggregateGroupKeyValues(fields, func(field string) any {
		return entityValue(entity, field)
	})
}

func aggregateGroupKeyValues(fields []string, valueFor func(string) any) (map[string]any, string) {
	key := make(map[string]any, len(fields))
	identityParts := make([]any, 0, len(fields)*2)
	for _, field := range fields {
		value := valueFor(field)
		key[field] = value
		identityParts = append(identityParts, field, value)
	}
	identity, err := json.Marshal(identityParts)
	if err != nil {
		return key, fmt.Sprintf("%#v", identityParts)
	}
	return key, string(identity)
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
