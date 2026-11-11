package query

import (
	"reflect"
	"slices"
)

func cyEquivalent(a, b *cyExpr) bool {
	if a == nil || b == nil {
		return a == b
	}
	// Pattern expressions have their own bindings. Comparing their scalar
	// arguments alone can equate different traversals inside an aggregate.
	if len(a.patterns) != 0 || len(b.patterns) != 0 {
		return a == b
	}
	if a.op != b.op || a.name != b.name || a.slot != b.slot || a.distinct != b.distinct || !slices.Equal(a.keys, b.keys) || len(a.args) != len(b.args) {
		return false
	}
	// Numerically equal literals may select different arithmetic, such as
	// integer division with 2 and floating-point division with 2.0.
	if a.op == "literal" && !reflect.DeepEqual(a.value, b.value) {
		return false
	}
	for i := range a.args {
		if !cyEquivalent(a.args[i], b.args[i]) {
			return false
		}
	}
	return true
}

// Grouped and DISTINCT projections expose their projected values to ORDER BY
// and WHERE. Slots introduced inside an expression are also visible within
// that expression. The first local slot bounds those lexical bindings.
func cyProjectedExpression(e *cyExpr, items []cyItem, firstLocal int) (*cyExpr, error) {
	if e == nil {
		return nil, nil
	}
	if e.op == "var" {
		slot, err := cyProjectedSlot(e.slot, e.pos, items, firstLocal)
		if err != nil || slot == e.slot {
			return e, err
		}
		return &cyExpr{op: "var", slot: slot, pos: e.pos}, nil
	}
	for _, it := range items {
		if cyEquivalent(e, it.expr) {
			return &cyExpr{op: "var", slot: it.slot, pos: e.pos}, nil
		}
	}
	if cyAggregate(e) {
		return nil, e.pos.fail("cypher_unsupported", "ORDER BY aggregate must use its projected expression or alias")
	}
	copy := *e
	if len(e.patterns) != 0 {
		firstLocal = min(firstLocal, e.patternSlotStart)
		copy.patterns = slices.Clone(e.patterns)
		for i := range copy.patterns {
			p, err := cyProjectedPattern(copy.patterns[i], e.pos, items, firstLocal)
			if err != nil {
				return nil, err
			}
			copy.patterns[i] = p
		}
	}
	copy.args = slices.Clone(e.args)
	for i, a := range e.args {
		if i == 1 && (e.op == "comprehension" || e.op == "quantifier") {
			firstLocal = min(firstLocal, e.slot)
		}
		rewritten, err := cyProjectedExpression(a, items, firstLocal)
		if err != nil {
			return nil, err
		}
		copy.args[i] = rewritten
	}
	return &copy, nil
}

func cyProjectedSlot(slot int, pos cyPos, items []cyItem, firstLocal int) (int, error) {
	if slot < 0 || slot >= firstLocal {
		return slot, nil
	}
	for _, it := range items {
		if slot == it.slot {
			return slot, nil
		}
		if it.expr.op == "var" && slot == it.expr.slot {
			return it.slot, nil
		}
	}
	return 0, pos.fail("cypher_semantic_error", "variable is not visible after projection")
}

func cyProjectedPattern(p cyPattern, pos cyPos, items []cyItem, firstLocal int) (cyPattern, error) {
	var err error
	p.slot, err = cyProjectedSlot(p.slot, pos, items, firstLocal)
	if err != nil {
		return p, err
	}
	p.nodes = slices.Clone(p.nodes)
	for i := range p.nodes {
		node := &p.nodes[i]
		node.slot, err = cyProjectedSlot(node.slot, pos, items, firstLocal)
		if err != nil {
			return p, err
		}
		node.props, err = cyProjectedExpression(node.props, items, firstLocal)
		if err != nil {
			return p, err
		}
	}
	p.rels = slices.Clone(p.rels)
	for i := range p.rels {
		rel := &p.rels[i]
		rel.slot, err = cyProjectedSlot(rel.slot, pos, items, firstLocal)
		if err != nil {
			return p, err
		}
		rel.props, err = cyProjectedExpression(rel.props, items, firstLocal)
		if err != nil {
			return p, err
		}
	}
	return p, nil
}
