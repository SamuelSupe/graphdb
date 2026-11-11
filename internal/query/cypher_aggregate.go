package query

import (
	"fmt"
	"math"
)

type cyAccumulator struct {
	count  int64
	value  any
	values []any
	seen   map[string]bool
	mean   float64
}
type cyGroup struct {
	row          cyRow
	accumulators map[*cyExpr]*cyAccumulator
}

func (r *cyRuntime) aggregate(rows []cyRow, cl cyClause, emit func(cyRow, map[*cyExpr]any) error) error {
	grouping := cl.groupBy
	groups := []*cyGroup{}
	byKey := map[string]*cyGroup{}
	newGroup := func(row cyRow, key string) (*cyGroup, error) {
		if len(groups) >= 10000 {
			return nil, fmt.Errorf("%w: Cypher aggregation exceeds 10000 groups", ErrLimitExceeded)
		}
		if err := r.reserve(len(key) + 64 + len(cl.aggregates)*96); err != nil {
			return nil, err
		}
		g := &cyGroup{row: row, accumulators: map[*cyExpr]*cyAccumulator{}}
		for _, e := range cl.aggregates {
			g.accumulators[e] = &cyAccumulator{seen: map[string]bool{}}
		}
		byKey[key] = g
		groups = append(groups, g)
		return g, nil
	}
	if len(grouping) == 0 {
		if _, err := newGroup(r.cloneRow(nil), ""); err != nil {
			return err
		}
	}
	for _, row := range rows {
		key := ""
		if len(grouping) > 0 {
			values := make([]any, len(grouping))
			for i, e := range grouping {
				v, err := r.eval(e, row, nil)
				if err != nil {
					return err
				}
				values[i] = v
			}
			key = cyKey(values)
		}
		g := byKey[key]
		if g == nil {
			var err error
			g, err = newGroup(row, key)
			if err != nil {
				return err
			}
		} else if len(grouping) == 0 {
			g.row = row
		}
		for _, e := range cl.aggregates {
			acc := g.accumulators[e]
			v := any(true)
			if len(e.args) > 0 {
				var err error
				v, err = r.eval(e.args[0], row, nil)
				if err != nil {
					return err
				}
			}
			if v == nil {
				continue
			}
			if e.distinct {
				key := cyKey(v)
				if acc.seen[key] {
					continue
				}
				if err := r.reserve(len(key) + 32); err != nil {
					return err
				}
				acc.seen[key] = true
			}
			acc.count++
			switch e.name {
			case "count":
			case "collect":
				if err := r.reserve(16 + cySize(v)); err != nil {
					return err
				}
				acc.values = append(acc.values, v)
			case "min", "max":
				if acc.value == nil || (e.name == "min" && cyCompare(v, acc.value) < 0) || (e.name == "max" && cyCompare(v, acc.value) > 0) {
					acc.value = v
				}
			case "sum", "avg":
				f, ok := cyNumber(v)
				if !ok {
					return e.pos.fail("cypher_type_error", e.name+" requires numbers")
				}
				if e.name == "avg" {
					if acc.count == 1 {
						acc.mean = f
					} else {
						weight := 1 / float64(acc.count)
						if math.Signbit(acc.mean) != math.Signbit(f) {
							acc.mean = acc.mean*(1-weight) + f*weight
						} else {
							acc.mean += (f - acc.mean) * weight
						}
					}
				} else {
					if acc.value == nil {
						acc.value = v
					} else {
						var err error
						acc.value, err = cyArithmetic("+", acc.value, v)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	for _, g := range groups {
		values := map[*cyExpr]any{}
		for e, a := range g.accumulators {
			switch e.name {
			case "count":
				values[e] = a.count
			case "collect":
				if a.values == nil {
					values[e] = []any{}
				} else {
					values[e] = a.values
				}
			case "avg":
				if a.count > 0 {
					values[e] = a.mean
				}
			case "sum":
				values[e] = a.value
				if a.value == nil {
					values[e] = int64(0)
				}
			default:
				values[e] = a.value
			}
		}
		if err := emit(g.row, values); err != nil {
			return err
		}
	}
	return nil
}

// Slots increase as the compiler enters scopes. A pattern or comprehension
// can depend on input slots, while its own bindings are local to the expression.
func cyFreeVariable(e *cyExpr, outerSlots int) bool {
	if e == nil {
		return false
	}
	if e.op == "var" {
		return e.slot < outerSlots
	}
	if len(e.patterns) > 0 {
		outerSlots = min(outerSlots, e.patternSlotStart)
		for _, p := range e.patterns {
			if p.slot >= 0 && p.slot < outerSlots {
				return true
			}
			for _, node := range p.nodes {
				if node.slot < outerSlots || cyFreeVariable(node.props, outerSlots) {
					return true
				}
			}
			for _, rel := range p.rels {
				if rel.slot < outerSlots || cyFreeVariable(rel.props, outerSlots) {
					return true
				}
			}
		}
	}
	for i, a := range e.args {
		if i == 1 && (e.op == "comprehension" || e.op == "quantifier") {
			outerSlots = min(outerSlots, e.slot)
		}
		if cyFreeVariable(a, outerSlots) {
			return true
		}
	}
	return false
}
func cyGrouping(e *cyExpr, outerSlots int, groups *[]*cyExpr) {
	if e == nil || cyAggregate(e) {
		return
	}
	if !cyHasAggregate(e) {
		if cyFreeVariable(e, outerSlots) {
			*groups = append(*groups, e)
		}
		return
	}
	for i, a := range e.args {
		if i == 1 && (e.op == "comprehension" || e.op == "quantifier") {
			outerSlots = min(outerSlots, e.slot)
		}
		cyGrouping(a, outerSlots, groups)
	}
}
