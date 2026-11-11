package query

import (
	"sort"
	"strings"
)

func (c *cyCompiler) projection(n *cySyntax) (cyClause, error) {
	cl := cyClause{kind: strings.ToLower(n.rule)}
	body := n.one("ProjectionBody")
	cl.distinct = body.token("DISTINCT")
	input := c.scope
	next := map[string]cySymbol{}
	items := body.one("ProjectionItems")
	add := func(name string, e *cyExpr, pos cyPos) error {
		if _, exists := next[name]; exists {
			return pos.fail("cypher_semantic_error", "duplicate result column "+name)
		}
		slot := c.slot()
		kind := "any"
		if e.op == "var" {
			for _, s := range input {
				if s.slot == e.slot {
					kind = s.kind
				}
			}
		}
		next[name] = cySymbol{slot, kind}
		c.plan.definitions[slot] = e
		cl.items = append(cl.items, cyItem{name, e, slot})
		return nil
	}
	if items.token("*") {
		names := make([]string, 0, len(input))
		for name := range input {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool { return input[names[i]].slot < input[names[j]].slot })
		for _, name := range names {
			if err := add(name, &cyExpr{op: "var", slot: input[name].slot, pos: n.pos}, n.pos); err != nil {
				return cl, err
			}
		}
	}
	for _, it := range items.many("ProjectionItem") {
		expression := it.one("Expression")
		e, err := c.expr(expression)
		if err != nil {
			return cl, err
		}
		var name string
		switch alias := it.one("Variable"); {
		case alias != nil:
			name = alias.name()
		case e.op == "var":
			name = expression.name()
		case n.rule == "With":
			return cl, it.pos.fail("cypher_semantic_error", "WITH expressions require an alias")
		default:
			name = expression.text()
		}
		if err := add(name, e, it.pos); err != nil {
			return cl, err
		}
	}
	var collect func(*cyExpr, bool) error
	collect = func(e *cyExpr, in bool) error {
		if e == nil {
			return nil
		}
		if cyAggregate(e) {
			if in {
				return e.pos.fail("cypher_semantic_error", "nested aggregates are not allowed")
			}
			cl.aggregates = append(cl.aggregates, e)
			in = true
		}
		for _, a := range e.args {
			if err := collect(a, in); err != nil {
				return err
			}
		}
		return nil
	}
	for _, it := range cl.items {
		if err := collect(it.expr, false); err != nil {
			return cl, err
		}
	}
	if len(cl.aggregates) > 0 {
		for _, it := range cl.items {
			cyGrouping(it.expr, c.plan.slots, &cl.groupBy)
		}
	}
	orderScope := map[string]cySymbol{}
	for k, v := range input {
		orderScope[k] = v
	}
	for k, v := range next {
		orderScope[k] = v
	}
	c.scope = orderScope
	if order := body.one("Order"); order != nil {
		for _, s := range order.many("SortItem") {
			e, err := c.expr(s.one("Expression"))
			if err != nil {
				return cl, err
			}
			if len(cl.aggregates) > 0 || cl.distinct {
				e, err = cyProjectedExpression(e, cl.items, c.plan.slots)
				if err != nil {
					return cl, err
				}
			}
			if cyHasAggregate(e) {
				return cl, s.pos.fail("cypher_unsupported", "ORDER BY aggregates through their projected alias")
			}
			cl.sort = append(cl.sort, cySort{e, s.token("DESC") || s.token("DESCENDING")})
		}
	}
	c.scope = map[string]cySymbol{}
	if s := body.one("Skip"); s != nil {
		var err error
		cl.skip, err = c.expr(s.one("Expression"))
		if err != nil {
			return cl, err
		}
	}
	if l := body.one("Limit"); l != nil {
		var err error
		cl.limit, err = c.expr(l.one("Expression"))
		if err != nil {
			return cl, err
		}
	}
	c.scope = orderScope
	if w := n.one("Where"); w != nil {
		var err error
		cl.where, err = c.expr(w.one("Expression"))
		if err != nil {
			return cl, err
		}
		if cyHasAggregate(cl.where) {
			return cl, w.pos.fail("cypher_semantic_error", "aggregate in WITH WHERE")
		}
		if len(cl.aggregates) > 0 || cl.distinct {
			cl.where, err = cyProjectedExpression(cl.where, cl.items, c.plan.slots)
			if err != nil {
				return cl, err
			}
		}
	}
	c.scope = next
	return cl, nil
}
