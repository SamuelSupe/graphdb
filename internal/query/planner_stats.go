package query

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (stats PlannerStats) field(kind string, field string) (PlannerIndexStat, bool) {
	for _, index := range stats.Indexes {
		if index.Kind == kind && index.Field == field && (index.Status == "" || index.Status == "ready") {
			return index, true
		}
	}
	return PlannerIndexStat{}, false
}

func estimateFromIndexStat(index PlannerIndexStat, values []any) int {
	if len(values) == 0 {
		return 0
	}
	seen := map[string]struct{}{}
	count := 0
	for _, value := range values {
		key := canonicalPlannerValue(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		count += estimateValueCount(index, key)
	}
	return count
}

func estimateValueCount(index PlannerIndexStat, key string) int {
	topTotal := 0
	for _, value := range index.TopValues {
		topTotal += value.Count
		if value.Value == key {
			return value.Count
		}
	}
	remainingDistinct := index.DistinctValues - len(index.TopValues)
	remainingEntries := index.EntryCount - topTotal
	if remainingDistinct <= 0 || remainingEntries <= 0 {
		return 0
	}
	return max(1, remainingEntries/remainingDistinct)
}

func canonicalPlannerValue(value any) string {
	if key, ok := graph.ScalarIndexKey(value); ok {
		return key
	}
	return fmt.Sprintf("v:%v", value)
}
