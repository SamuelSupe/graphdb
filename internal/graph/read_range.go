package graph

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

type fieldValueOrderKey struct {
	kind, field string
}

type orderedFieldValue struct {
	key, text string
	number    float64
}

type fieldValueOrder struct {
	text, numeric, nonNumeric []orderedFieldValue
}

// FieldIndexRange bounds candidate keys. Numeric comparisons apply to numeric
// keys; other keys retain the query language's lexical comparison semantics.
type FieldIndexRange struct {
	Op      string
	Text    string
	Number  float64
	Numeric bool
}

// ScanFieldIndexRangeIDs seeks the ordered scalar keys before evaluating match.
// The final predicate must check all query filters; results remain in ID order.
func (g *Graph) ScanFieldIndexRangeIDs(kind, field string, bounds []FieldIndexRange, match func(string) (bool, error)) ([]string, error) {
	if match == nil {
		return nil, nil
	}
	order := g.sortedFieldValues(kind, field)
	ids := make([]string, 0)
	visit := func(values []orderedFieldValue) error {
		for _, value := range values {
			ok, err := match(value.key)
			if err != nil {
				return err
			}
			if ok {
				for id := range g.fieldIndex[kind][field][value.key] {
					ids = append(ids, id)
				}
			}
		}
		return nil
	}
	numericComparison := false
	for _, bound := range bounds {
		numericComparison = numericComparison || bound.Numeric && bound.Op != "prefix"
	}
	if numericComparison {
		numeric, other := order.numeric, order.nonNumeric
		for _, bound := range bounds {
			other = fieldValuesInRange(other, bound, false)
			if bound.Numeric && bound.Op != "prefix" {
				numeric = fieldValuesInRange(numeric, bound, true)
			}
		}
		if err := visit(numeric); err != nil {
			return nil, err
		}
		if err := visit(other); err != nil {
			return nil, err
		}
	} else {
		values := order.text
		for _, bound := range bounds {
			values = fieldValuesInRange(values, bound, false)
		}
		if err := visit(values); err != nil {
			return nil, err
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (g *Graph) sortedFieldValues(kind, field string) fieldValueOrder {
	g.fieldIndexOrderMu.Lock()
	defer g.fieldIndexOrderMu.Unlock()
	key := fieldValueOrderKey{kind: kind, field: field}
	if order, ok := g.fieldValueOrder[key]; ok {
		return order
	}
	var order fieldValueOrder
	for key := range g.fieldIndex[kind][field] {
		value := orderedFieldValue{key: key}
		switch {
		case key == "null":
			value.text = "<nil>"
		case strings.HasPrefix(key, "s:"), strings.HasPrefix(key, "b:"):
			value.text = key[2:]
		case strings.HasPrefix(key, "n:"):
			value.text = key[2:]
			number, err := strconv.ParseFloat(value.text, 64)
			if err != nil {
				continue
			}
			value.number = number
			order.text = append(order.text, value)
			if !math.IsNaN(number) {
				order.numeric = append(order.numeric, value)
			}
			continue
		default:
			continue
		}
		order.text = append(order.text, value)
		order.nonNumeric = append(order.nonNumeric, value)
	}
	sort.Slice(order.text, func(i, j int) bool { return order.text[i].text < order.text[j].text })
	sort.Slice(order.nonNumeric, func(i, j int) bool { return order.nonNumeric[i].text < order.nonNumeric[j].text })
	sort.Slice(order.numeric, func(i, j int) bool { return order.numeric[i].number < order.numeric[j].number })
	if g.fieldValueOrder == nil {
		g.fieldValueOrder = make(map[fieldValueOrderKey]fieldValueOrder)
	}
	g.fieldValueOrder[key] = order
	return order
}

func fieldValuesInRange(values []orderedFieldValue, bound FieldIndexRange, numeric bool) []orderedFieldValue {
	if numeric && math.IsNaN(bound.Number) {
		return nil
	}
	start, end := 0, len(values)
	strict := bound.Op == "gt" || bound.Op == "lte"
	position := sort.Search(len(values), func(i int) bool {
		if numeric {
			return values[i].number > bound.Number || !strict && values[i].number == bound.Number
		}
		return values[i].text > bound.Text || !strict && values[i].text == bound.Text
	})
	switch bound.Op {
	case "gt", "gte":
		start = position
	case "lt", "lte":
		end = position
	case "prefix":
		start = position
		end = start + sort.Search(len(values)-start, func(i int) bool {
			return !strings.HasPrefix(values[start+i].text, bound.Text)
		})
	}
	return values[start:end]
}
