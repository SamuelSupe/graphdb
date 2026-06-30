package query

import (
	"fmt"
	"reflect"
	"strings"
)

func validateRequest(request Request) error {
	switch request.Op {
	case "match", "neighbors", "traverse", "impact", "shortest_path":
	default:
		return fmt.Errorf("%w: unsupported op %q", ErrInvalid, request.Op)
	}
	if err := validateRequestControls(request); err != nil {
		return err
	}
	if err := validateDirectionStrategy(request.DirectionStrategy); err != nil {
		return err
	}
	for _, filter := range append(append(requestFilters(request), request.EdgeWhere...), request.Path.EndWhere...) {
		if err := validateFilter(filter); err != nil {
			return err
		}
	}
	if err := validateFilterExpr(request.WhereExpr); err != nil {
		return err
	}
	if err := validateFilterExpr(request.EdgeWhereExpr); err != nil {
		return err
	}
	if err := validateFilterExpr(request.Path.EndWhereExpr); err != nil {
		return err
	}
	for _, step := range request.Path.Steps {
		for _, filter := range append(step.Where, step.EdgeWhere...) {
			if err := validateFilter(filter); err != nil {
				return err
			}
		}
		if err := validateFilterExpr(step.WhereExpr); err != nil {
			return err
		}
		if err := validateFilterExpr(step.EdgeWhereExpr); err != nil {
			return err
		}
	}
	for _, filter := range request.Having {
		if err := validateFilter(filter); err != nil {
			return err
		}
	}
	if err := validateFilterExpr(request.HavingExpr); err != nil {
		return err
	}
	for _, aggregation := range request.Aggregate {
		if err := validateAggregation(aggregation); err != nil {
			return err
		}
	}
	return nil
}

func validateFilterExpr(expr *FilterExpr) error {
	if expr == nil {
		return nil
	}
	op := strings.ToLower(expr.Op)
	if op == "" && expr.Field != "" {
		op = "eq"
	}
	switch op {
	case "and", "or":
		if len(expr.Children) == 0 {
			return fmt.Errorf("%w: %s expression requires children", ErrInvalid, op)
		}
		for i := range expr.Children {
			if err := validateFilterExpr(&expr.Children[i]); err != nil {
				return err
			}
		}
		return nil
	case "not":
		if len(expr.Children) != 1 {
			return fmt.Errorf("%w: not expression requires exactly one child", ErrInvalid)
		}
		return validateFilterExpr(&expr.Children[0])
	default:
		if expr.Field == "" {
			return fmt.Errorf("%w: filter expression requires field", ErrInvalid)
		}
		return validateFilter(Filter{Field: expr.Field, Op: op, Value: expr.Value})
	}
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
