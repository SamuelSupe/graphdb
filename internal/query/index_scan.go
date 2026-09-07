package query

import (
	"context"
	"fmt"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func scanFieldIndexIDs(ctx context.Context, scanner FieldIndexScanLookup, kind string, field string, filters []Filter) ([]string, bool, error) {
	fieldFilters := filtersForIndexField(field, filters)
	var entries map[string][]string
	var ok bool
	var err error
	if filtered, supportsFilteredScan := scanner.(FieldIndexFilterScanLookup); supportsFilteredScan {
		entries, ok, err = filtered.ScanFieldIndexWithFilters(ctx, kind, field, fieldFilters)
	} else {
		entries, ok, err = scanner.ScanFieldIndex(ctx, kind, field)
	}
	if err != nil || !ok {
		return nil, ok, err
	}
	matchesKey := newIndexScanKeyMatcher(fieldFilters)
	seen := map[string]struct{}{}
	for key, ids := range entries {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if !matchesKey(key) {
			continue
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, true, nil
}

func scanRuntimeFieldIndexIDs(
	ctx context.Context,
	g *graph.Graph,
	kind string,
	field string,
	filters []Filter,
) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fieldFilters := filtersForIndexField(field, filters)
	matchesKey := newIndexScanKeyMatcher(fieldFilters)
	match := func(key string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return matchesKey(key), nil
	}
	bounds := make([]graph.FieldIndexRange, 0, len(fieldFilters))
	for _, filter := range fieldFilters {
		switch filter.Op {
		case "gt", "gte", "lt", "lte", "prefix":
			number, numeric := asFloat(filter.Value)
			bounds = append(bounds, graph.FieldIndexRange{
				Op: filter.Op, Text: fmt.Sprint(filter.Value), Number: number, Numeric: numeric,
			})
		}
	}
	if len(bounds) > 0 {
		return g.ScanFieldIndexRangeIDs(kind, field, bounds, match)
	}
	return g.ScanFieldIndexIDs(kind, field, match)
}

func filtersForIndexField(field string, filters []Filter) []Filter {
	out := make([]Filter, 0)
	for _, filter := range filters {
		name, ok := indexedField(filter.Field)
		if ok && name == field && indexScanFilterSupported(filter) {
			out = append(out, filter)
		}
	}
	return out
}

func indexKeyComparableValue(value any) bool {
	switch value.(type) {
	case string, bool, float64, int, int64:
		return true
	default:
		return false
	}
}
