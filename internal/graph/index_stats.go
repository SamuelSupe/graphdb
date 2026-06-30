package graph

import "sort"

const defaultIndexTopValues = 16

type FieldIndexValueStat struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type FieldIndexSummary struct {
	EntryCount     int                   `json:"entry_count"`
	DistinctValues int                   `json:"distinct_values"`
	TopValues      []FieldIndexValueStat `json:"top_values,omitempty"`
}

func (g *Graph) FieldIndexSummary(kind string, field string, topN int) FieldIndexSummary {
	if topN <= 0 {
		topN = defaultIndexTopValues
	}
	byValue := g.fieldIndex[kind][field]
	summary := FieldIndexSummary{DistinctValues: len(byValue)}
	values := make([]FieldIndexValueStat, 0, len(byValue))
	for value, ids := range byValue {
		count := len(ids)
		summary.EntryCount += count
		values = append(values, FieldIndexValueStat{Value: value, Count: count})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Count == values[j].Count {
			return values[i].Value < values[j].Value
		}
		return values[i].Count > values[j].Count
	})
	if len(values) > topN {
		values = values[:topN]
	}
	summary.TopValues = values
	return summary
}
