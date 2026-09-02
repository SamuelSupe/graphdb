package query

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const maxAggregateBuckets = 10000

func aggregateResults(results []Result, specs []Aggregation) (map[string]any, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	acc := newAggregateAccumulator(specs)
	for _, result := range results {
		if err := acc.add(result); err != nil {
			return nil, err
		}
	}
	return acc.results(), nil
}

type aggregateAccumulator struct {
	specs  []Aggregation
	states []aggregateState
}

type aggregateState struct {
	count  int
	counts map[string]int
	sum    float64
	min    float64
	max    float64
}

func newAggregateAccumulator(specs []Aggregation) *aggregateAccumulator {
	if len(specs) == 0 {
		return nil
	}
	return &aggregateAccumulator{specs: specs, states: make([]aggregateState, len(specs))}
}

func (a *aggregateAccumulator) add(result Result) error {
	return a.addValues(func(field string) any {
		return resultValue(result, field)
	})
}

func (a *aggregateAccumulator) addEntity(entity graph.Entity) error {
	return a.addValues(func(field string) any {
		return entityValue(entity, field)
	})
}

func (a *aggregateAccumulator) addValues(valueFor func(string) any) error {
	if a == nil {
		return nil
	}
	for i, spec := range a.specs {
		state := &a.states[i]
		switch spec.Op {
		case "count":
			state.count++
		case "count_by":
			if state.counts == nil {
				state.counts = map[string]int{}
			}
			key := fmt.Sprint(valueFor(spec.Field))
			if _, exists := state.counts[key]; !exists && len(state.counts) >= maxAggregateBuckets {
				return fmt.Errorf("%w: aggregate supports at most %d buckets", ErrLimitExceeded, maxAggregateBuckets)
			}
			state.counts[key]++
		case "sum", "avg", "min", "max":
			value, ok := asFloat(valueFor(spec.Field))
			if !ok {
				continue
			}
			if state.count == 0 || value < state.min {
				state.min = value
			}
			if state.count == 0 || value > state.max {
				state.max = value
			}
			state.sum += value
			state.count++
		}
	}
	return nil
}

func (a *aggregateAccumulator) results() map[string]any {
	if a == nil {
		return nil
	}
	out := map[string]any{}
	for i, spec := range a.specs {
		out[aggregateName(spec)] = a.states[i].value(spec)
	}
	return out
}

func (s aggregateState) value(spec Aggregation) any {
	switch spec.Op {
	case "count":
		return s.count
	case "count_by":
		if s.counts == nil {
			return map[string]int{}
		}
		return s.counts
	case "sum":
		return s.sum
	case "avg":
		if s.count == 0 {
			return nil
		}
		return s.sum / float64(s.count)
	case "min":
		if s.count == 0 {
			return nil
		}
		return s.min
	case "max":
		if s.count == 0 {
			return nil
		}
		return s.max
	default:
		return nil
	}
}

func aggregateName(spec Aggregation) string {
	if spec.Name != "" {
		return spec.Name
	}
	name := spec.Op
	if spec.Field != "" {
		name += "_" + spec.Field
	}
	return name
}
