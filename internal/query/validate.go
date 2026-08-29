package query

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

const (
	maxQueryPredicates       = 128
	maxFilterExpressionNodes = 256
	maxFilterExpressionDepth = 32
	maxInFilterValues        = 1024
	maxQueryFilterValueBytes = 256 << 10
	maxQuerySortSpecs        = 32
	maxQueryAggregations     = 32
	maxQueryGroupFields      = 32
	maxQueryProjectionFields = 256
	maxQueryPathSteps        = 16
	maxQuerySelectorValues   = 256
	maxTextQueryBytes        = 64 << 10
	maxQueryValueNodes       = 4096
)

type requestShapeValidator struct {
	predicates       int
	expressionNodes  int
	inValues         int
	filterValueBytes int
}

func validateRequest(request Request) error {
	switch request.Op {
	case "match", "pattern", "neighbors", "traverse", "impact", "shortest_path":
	default:
		return fmt.Errorf("%w: unsupported op %q", ErrInvalid, request.Op)
	}
	if err := validateRequestControls(request); err != nil {
		return err
	}
	if err := validateDirectionStrategy(request.DirectionStrategy); err != nil {
		return err
	}
	if err := validateRequestShapeLimits(request); err != nil {
		return err
	}
	shape := requestShapeValidator{}
	for _, filter := range append(append(requestFilters(request), request.EdgeWhere...), request.Path.EndWhere...) {
		if err := shape.validateFilter(filter); err != nil {
			return err
		}
	}
	if err := shape.validateFilterExpr(request.WhereExpr, 1); err != nil {
		return err
	}
	if err := shape.validateFilterExpr(request.EdgeWhereExpr, 1); err != nil {
		return err
	}
	if err := shape.validateFilterExpr(request.Path.EndWhereExpr, 1); err != nil {
		return err
	}
	for _, step := range request.Path.Steps {
		if err := validateDirection(step.Direction); err != nil {
			return err
		}
		for _, filter := range append(step.Where, step.EdgeWhere...) {
			if err := shape.validateFilter(filter); err != nil {
				return err
			}
		}
		if err := shape.validateFilterExpr(step.WhereExpr, 1); err != nil {
			return err
		}
		if err := shape.validateFilterExpr(step.EdgeWhereExpr, 1); err != nil {
			return err
		}
	}
	for _, filter := range request.Having {
		if err := shape.validateFilter(filter); err != nil {
			return err
		}
	}
	if err := shape.validateFilterExpr(request.HavingExpr, 1); err != nil {
		return err
	}
	for _, aggregation := range request.Aggregate {
		if err := validateAggregation(aggregation); err != nil {
			return err
		}
	}
	if request.Op == "pattern" {
		if request.Kind == "" {
			return fmt.Errorf("%w: pattern requires a start kind", ErrInvalid)
		}
		if len(request.Path.Steps) == 0 {
			return fmt.Errorf("%w: pattern requires at least one path step", ErrInvalid)
		}
		if len(request.Path.Steps) > 8 {
			return fmt.Errorf("%w: pattern supports at most 8 path steps", ErrInvalid)
		}
		if request.Depth != 0 && request.Depth != len(request.Path.Steps) {
			return fmt.Errorf("%w: pattern depth must equal its path step count", ErrInvalid)
		}
	}
	return nil
}

func (v *requestShapeValidator) validateFilterExpr(expr *FilterExpr, depth int) error {
	if expr == nil {
		return nil
	}
	if depth > maxFilterExpressionDepth {
		return fmt.Errorf("%w: filter expression depth exceeds %d", ErrInvalid, maxFilterExpressionDepth)
	}
	v.expressionNodes++
	if v.expressionNodes > maxFilterExpressionNodes {
		return fmt.Errorf("%w: filter expressions support at most %d nodes", ErrInvalid, maxFilterExpressionNodes)
	}
	op := strings.ToLower(expr.Op)
	if op == "" && expr.Field != "" {
		op = "eq"
	}
	switch op {
	case "and", "or":
		if expr.Field != "" || expr.Value != nil {
			return fmt.Errorf("%w: %s expression cannot contain field or value", ErrInvalid, op)
		}
		if len(expr.Children) == 0 {
			return fmt.Errorf("%w: %s expression requires children", ErrInvalid, op)
		}
		for i := range expr.Children {
			if err := v.validateFilterExpr(&expr.Children[i], depth+1); err != nil {
				return err
			}
		}
		return nil
	case "not":
		if expr.Field != "" || expr.Value != nil {
			return fmt.Errorf("%w: not expression cannot contain field or value", ErrInvalid)
		}
		if len(expr.Children) != 1 {
			return fmt.Errorf("%w: not expression requires exactly one child", ErrInvalid)
		}
		return v.validateFilterExpr(&expr.Children[0], depth+1)
	default:
		if expr.Field == "" {
			return fmt.Errorf("%w: filter expression requires field", ErrInvalid)
		}
		if len(expr.Children) != 0 {
			return fmt.Errorf("%w: filter leaf cannot contain children", ErrInvalid)
		}
		return v.validateFilter(Filter{Field: expr.Field, Op: op, Value: expr.Value})
	}
}

func (v *requestShapeValidator) validateFilter(filter Filter) error {
	if err := validateFilter(filter); err != nil {
		return err
	}
	v.predicates++
	if v.predicates > maxQueryPredicates {
		return fmt.Errorf("%w: query supports at most %d predicates", ErrInvalid, maxQueryPredicates)
	}
	if filter.Op == "in" {
		count := reflect.ValueOf(filter.Value).Len()
		if count > maxInFilterValues-v.inValues {
			return fmt.Errorf("%w: in filters support at most %d total values", ErrInvalid, maxInFilterValues)
		}
		v.inValues += count
	}
	if err := validateJSONValueShape(filter.Value); err != nil {
		return fmt.Errorf("%w: filter value %v", ErrInvalid, err)
	}
	encoded, err := json.Marshal(filter.Value)
	if err != nil {
		return fmt.Errorf("%w: filter value must be valid JSON: %v", ErrInvalid, err)
	}
	if len(encoded) > maxQueryFilterValueBytes-v.filterValueBytes {
		return fmt.Errorf("%w: filter values exceed %d encoded bytes", ErrInvalid, maxQueryFilterValueBytes)
	}
	v.filterValueBytes += len(encoded)
	return nil
}

func validateJSONValueShape(value any) error {
	type pendingValue struct {
		value any
		depth int
	}
	stack := []pendingValue{{value: value, depth: 1}}
	nodes := 0
	for len(stack) > 0 {
		last := len(stack) - 1
		item := stack[last]
		stack = stack[:last]
		nodes++
		if nodes > maxQueryValueNodes {
			return fmt.Errorf("contains more than %d values", maxQueryValueNodes)
		}
		if item.depth > maxFilterExpressionDepth {
			return fmt.Errorf("nesting exceeds %d", maxFilterExpressionDepth)
		}
		switch typed := item.value.(type) {
		case map[string]any:
			for _, child := range typed {
				stack = append(stack, pendingValue{value: child, depth: item.depth + 1})
			}
		case []any:
			for _, child := range typed {
				stack = append(stack, pendingValue{value: child, depth: item.depth + 1})
			}
		}
	}
	return nil
}

func validateRequestShapeLimits(request Request) error {
	limits := []struct {
		name  string
		count int
		max   int
	}{
		{name: "sort", count: len(request.Sort), max: maxQuerySortSpecs},
		{name: "aggregate", count: len(request.Aggregate), max: maxQueryAggregations},
		{name: "group_by", count: len(request.GroupBy), max: maxQueryGroupFields},
		{name: "project", count: len(request.Project), max: maxQueryProjectionFields},
		{name: "path.steps", count: len(request.Path.Steps), max: maxQueryPathSteps},
	}
	for _, limit := range limits {
		if limit.count > limit.max {
			return fmt.Errorf("%w: %s supports at most %d items", ErrInvalid, limit.name, limit.max)
		}
	}
	selectorCount := len(request.RelationTypes) + len(request.Path.RelationTypes) + len(request.Path.NodeKinds)
	for _, step := range request.Path.Steps {
		if len(step.RelationTypes) > maxQuerySelectorValues-selectorCount {
			return fmt.Errorf("%w: path selectors support at most %d total values", ErrInvalid, maxQuerySelectorValues)
		}
		selectorCount += len(step.RelationTypes)
		if len(step.NodeKinds) > maxQuerySelectorValues-selectorCount {
			return fmt.Errorf("%w: path selectors support at most %d total values", ErrInvalid, maxQuerySelectorValues)
		}
		selectorCount += len(step.NodeKinds)
	}
	if selectorCount > maxQuerySelectorValues {
		return fmt.Errorf("%w: path selectors support at most %d total values", ErrInvalid, maxQuerySelectorValues)
	}
	return nil
}

func validateRequestControls(request Request) error {
	if request.Limit < 0 {
		return fmt.Errorf("%w: limit must be >= 0", ErrInvalid)
	}
	if request.TimeoutMS < 0 {
		return fmt.Errorf("%w: timeout_ms must be >= 0", ErrInvalid)
	}
	if request.CostLimit < 0 {
		return fmt.Errorf("%w: cost_limit must be >= 0", ErrInvalid)
	}
	if request.CostLimit > maxQueryCostLimit {
		return fmt.Errorf("%w: cost_limit must be <= %d", ErrInvalid, maxQueryCostLimit)
	}
	if request.Depth < 0 {
		return fmt.Errorf("%w: depth must be >= 0", ErrInvalid)
	}
	if request.Path.MaxPaths < 0 {
		return fmt.Errorf("%w: path.max_paths must be >= 0", ErrInvalid)
	}
	if (len(request.Having) > 0 || request.HavingExpr != nil) && len(request.GroupBy) == 0 {
		return fmt.Errorf("%w: having requires group_by", ErrInvalid)
	}
	return nil
}

func validateDirectionStrategy(strategy string) error {
	switch strategy {
	case "", "impact":
		return nil
	default:
		return fmt.Errorf("%w: unsupported direction_strategy %q", ErrInvalid, strategy)
	}
}

func validateFilter(filter Filter) error {
	op := filter.Op
	switch op {
	case "", "eq", "neq", "in", "gt", "gte", "lt", "lte", "prefix", "contains", "fuzzy", "exists":
	default:
		return fmt.Errorf("%w: unsupported filter op %q", ErrInvalid, filter.Op)
	}
	if op == "exists" && filter.Value != nil {
		if _, ok := filter.Value.(bool); !ok {
			return fmt.Errorf("%w: exists filter value must be boolean", ErrInvalid)
		}
	}
	if op == "in" && !isFilterListValue(filter.Value) {
		return fmt.Errorf("%w: in filter value must be an array", ErrInvalid)
	}
	return nil
}

func isFilterListValue(value any) bool {
	if value == nil {
		return false
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

func validateAggregation(aggregation Aggregation) error {
	switch aggregation.Op {
	case "count", "count_by", "sum", "avg", "min", "max":
	default:
		return fmt.Errorf("%w: unsupported aggregate op %q", ErrInvalid, aggregation.Op)
	}
	switch aggregation.Op {
	case "count":
		return nil
	case "count_by", "sum", "avg", "min", "max":
		if aggregation.Field == "" {
			return fmt.Errorf("%w: aggregate %q requires field", ErrInvalid, aggregation.Op)
		}
		return nil
	default:
		return nil
	}
}
