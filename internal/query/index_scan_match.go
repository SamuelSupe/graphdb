package query

import (
	"fmt"
	"strconv"
	"strings"
)

type indexScanFilter struct {
	op      string
	number  float64
	numeric bool
	text    string
	exists  bool
}

type indexScanValue struct {
	number  float64
	numeric bool
	text    string
}

func newIndexScanKeyMatcher(filters []Filter) func(string) bool {
	compiled := make([]indexScanFilter, 0, len(filters))
	for _, filter := range filters {
		op := filter.Op
		if op == "" {
			op = "eq"
		}
		number, numeric := asFloat(filter.Value)
		exists, ok := filter.Value.(bool)
		if !ok {
			exists = true
		}
		compiled = append(compiled, indexScanFilter{
			op:      op,
			number:  number,
			numeric: numeric,
			text:    fmt.Sprint(filter.Value),
			exists:  exists,
		})
	}
	return func(key string) bool {
		value, ok := parseIndexScanValue(key)
		if !ok {
			return false
		}
		for _, filter := range compiled {
			if !value.matches(filter) {
				return false
			}
		}
		return true
	}
}

func parseIndexScanValue(key string) (indexScanValue, bool) {
	switch {
	case key == "null":
		return indexScanValue{text: "<nil>"}, true
	case strings.HasPrefix(key, "s:"):
		return indexScanValue{text: key[2:]}, true
	case key == "b:true":
		return indexScanValue{text: "true"}, true
	case key == "b:false":
		return indexScanValue{text: "false"}, true
	case strings.HasPrefix(key, "n:"):
		text := key[2:]
		number, err := strconv.ParseFloat(text, 64)
		return indexScanValue{number: number, numeric: err == nil, text: text}, err == nil
	default:
		return indexScanValue{}, false
	}
}

func (value indexScanValue) matches(filter indexScanFilter) bool {
	switch filter.op {
	case "exists":
		return filter.exists
	case "prefix":
		return strings.HasPrefix(value.text, filter.text)
	case "gt", "gte", "lt", "lte":
		if value.numeric && filter.numeric {
			return compareFloat(value.number, filter.number, filter.op)
		}
		return compareText(value.text, filter.text, filter.op)
	default:
		return false
	}
}

func compareText(left string, right string, op string) bool {
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
