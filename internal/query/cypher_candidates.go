package query

import (
	"slices"
)

func cyFieldHint(e *cyExpr, slot int) (string, *cyExpr) {
	if e == nil {
		return "", nil
	}
	if e.op == "binary" && e.name == "AND" {
		if field, value := cyFieldHint(e.args[0], slot); value != nil {
			return field, value
		}
		return cyFieldHint(e.args[1], slot)
	}
	if e.op == "binary" && e.name == "=" {
		for i := 0; i < 2; i++ {
			a, b := e.args[i], e.args[1-i]
			if a.op == "property" && a.args[0].op == "var" && a.args[0].slot == slot && (b.op == "literal" || b.op == "param") {
				return a.name, b
			}
		}
	}
	return "", nil
}
func cyNodeHint(p cyNodePattern, where *cyExpr) (string, *cyExpr) {
	field, e := cyFieldHint(where, p.slot)
	if e != nil {
		return field, e
	}
	if p.props != nil {
		for i, a := range p.props.args {
			if a.op == "literal" || a.op == "param" {
				return p.props.keys[i], a
			}
		}
	}
	return "", nil
}

// Indexed kinds are covered by exact scalar lookups. Other kinds still scan:
// a label may also come from Entity.labels, independently of Entity.kind.
func (r *cyRuntime) indexedNodes(p cyNodePattern, row cyRow, where *cyExpr, visit func(*cyNodeValue) error) (map[string]bool, error) {
	covered := map[string]bool{}
	field, hint := cyNodeHint(p, where)
	if hint == nil {
		return covered, nil
	}
	value, err := r.eval(hint, row, nil)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return covered, nil
	}
	if cyRank(value) != 5 && cyRank(value) != 6 && cyRank(value) != 7 {
		return covered, nil
	}
	callback := func(id string) (bool, error) {
		if err := r.budget.add(1); err != nil {
			return false, err
		}
		r.budget.scanned++
		n, err := r.node(id)
		if err != nil {
			return false, err
		}
		return true, visit(n)
	}
	if r.options.IndexLookup != nil {
		lookup, ok := r.options.IndexLookup.(FieldIndexVisitLookup)
		if !ok {
			return covered, nil
		}
		for _, stat := range r.options.PlannerStats.Indexes {
			if stat.Field != field || (stat.Status != "" && stat.Status != "ready") {
				continue
			}
			available, err := lookup.VisitFieldIndex(r.budget.ctx, stat.Kind, field, []any{value}, "", callback)
			if err != nil {
				return nil, err
			}
			if available {
				covered[stat.Kind] = true
			}
		}
		return covered, nil
	}
	for _, kind := range p.labels {
		if !r.g.HasFieldIndex(kind, field) {
			continue
		}
		_, err := r.g.VisitFieldIndexIDs(kind, field, []any{value}, func(id string) error { _, err := callback(id); return err })
		if err != nil {
			return nil, err
		}
		covered[kind] = true
	}
	return covered, nil
}

func (r *cyRuntime) choosePattern(p cyPattern, row cyRow, where *cyExpr) cyPattern {
	if len(p.rels) == 0 {
		return p
	}
	if r.options.EntityLookup != nil && !reverseEdgeIndexAvailable(r.options.PlannerStats) {
		return p
	}
	first, last := p.nodes[0], p.nodes[len(p.nodes)-1]
	if _, unbound := row[first.slot].(cyUnbound); !unbound {
		return p
	}
	_, backUnbound := row[last.slot].(cyUnbound)
	_, frontHint := cyNodeHint(first, where)
	_, backHint := cyNodeHint(last, where)
	if backUnbound && (cyIDHint(where, last.slot) == nil) && (backHint == nil || frontHint != nil) {
		return p
	}
	for _, n := range p.nodes {
		if cyFreeVariable(n.props, r.plan.slots) {
			return p
		}
	}
	for _, rel := range p.rels {
		if cyFreeVariable(rel.props, r.plan.slots) {
			return p
		}
	}
	p.nodes = slices.Clone(p.nodes)
	p.rels = slices.Clone(p.rels)
	slices.Reverse(p.nodes)
	slices.Reverse(p.rels)
	for i := range p.rels {
		switch p.rels[i].direction {
		case "out":
			p.rels[i].direction = "in"
		case "in":
			p.rels[i].direction = "out"
		}
	}
	p.reversed = true
	return p
}
