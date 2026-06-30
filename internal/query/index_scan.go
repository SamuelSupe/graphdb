package query

import (
	"context"
	"sort"
	"strconv"
	"strings"
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
	seen := map[string]struct{}{}
	for key, ids := range entries {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		value, comparable := indexKeyValue(key)
		if !comparable || !indexValueMatchesFilters(value, fieldFilters) {
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

func indexValueMatchesFilters(value any, filters []Filter) bool {
	for _, filter := range filters {
		if !filterMatches(value, true, filter) {
			return false
		}
	}
	return true
}

func indexKeyComparableValue(value any) bool {
	switch value.(type) {
	case string, bool, float64, int, int64:
		return true
	default:
		return false
	}
}

func indexKeyValue(key string) (any, bool) {
	switch {
	case key == "null":
		return nil, true
	case strings.HasPrefix(key, "s:"):
		return strings.TrimPrefix(key, "s:"), true
	case key == "b:true":
		return true, true
	case key == "b:false":
		return false, true
	case strings.HasPrefix(key, "n:"):
		value, err := strconv.ParseFloat(strings.TrimPrefix(key, "n:"), 64)
		return value, err == nil
	default:
		return nil, false
	}
}
