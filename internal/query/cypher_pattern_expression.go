package query

import "errors"

func (c *cyCompiler) patternExpression(n *cySyntax) (*cyExpr, error) {
	if n.rule == "PatternPredicate" {
		relationships := n.one("RelationshipsPattern")
		vars := []*cySyntax{relationships.one("NodePattern").one("Variable")}
		for _, chain := range relationships.many("PatternElementChain") {
			vars = append(vars, chain.one("NodePattern").one("Variable"), chain.one("RelationshipPattern").one("RelationshipDetail").one("Variable"))
		}
		for _, v := range vars {
			if v != nil {
				if _, exists := c.scope[v.name()]; !exists {
					return nil, v.pos.fail("cypher_semantic_error", "pattern predicate cannot introduce variable "+v.name())
				}
			}
		}
	}
	if n.one("RegularQuery") != nil {
		return nil, n.pos.fail("cypher_unsupported", "EXISTS requires a pattern and optional WHERE")
	}
	scope := c.scope
	c.scope = map[string]cySymbol{}
	for k, v := range scope {
		c.scope[k] = v
	}
	defer func() { c.scope = scope }()
	e := &cyExpr{op: "pattern-exists", name: n.rule, pos: n.pos, patternSlotStart: c.plan.slots}
	seen := map[int]bool{}
	if patterns := n.one("Pattern"); patterns != nil {
		for _, part := range patterns.many("PatternPart") {
			p, err := c.pattern(part, seen)
			if err != nil {
				return nil, err
			}
			e.patterns = append(e.patterns, p)
		}
	} else {
		relationships := n.one("RelationshipsPattern")
		if relationships == nil {
			return nil, n.pos.fail("cypher_unsupported", "unsupported pattern expression")
		}
		element := cySyntax{rule: "PatternElement", pos: n.pos, children: relationships.children}
		part := &cySyntax{rule: "PatternPart", pos: n.pos, children: []cySyntax{{rule: "AnonymousPatternPart", pos: n.pos, children: []cySyntax{element}}}}
		if v := n.one("Variable"); v != nil {
			part.children = append([]cySyntax{*v}, part.children...)
		}
		p, err := c.pattern(part, seen)
		if err != nil {
			return nil, err
		}
		e.patterns = append(e.patterns, p)
	}
	where := (*cyExpr)(nil)
	var err error
	if w := n.one("Where"); w != nil {
		where, err = c.expr(w.one("Expression"))
		if err != nil {
			return nil, err
		}
		if cyHasAggregate(where) {
			return nil, w.pos.fail("cypher_semantic_error", "aggregate in pattern WHERE")
		}
	}
	e.args = []*cyExpr{where}
	if n.rule == "PatternComprehension" {
		e.op = "pattern-list"
		project, err := c.expr(n.one("Expression"))
		if err != nil {
			return nil, err
		}
		if cyHasAggregate(project) {
			return nil, n.pos.fail("cypher_semantic_error", "aggregate in pattern comprehension projection")
		}
		e.args = append(e.args, project)
	}
	return e, nil
}

func (r *cyRuntime) patternExpression(e *cyExpr, row cyRow, aggregates map[*cyExpr]any) (any, error) {
	if e.name != "ExistentialSubquery" {
		for _, p := range e.patterns {
			for _, n := range p.nodes {
				if row[n.slot] == nil {
					return nil, nil
				}
			}
		}
	}
	cl := cyClause{kind: "match", patterns: e.patterns, where: e.args[0]}
	found := false
	values := []any{}
	err := r.visitMatch(row, cl, func(bound cyRow) error {
		found = true
		if e.op == "pattern-exists" {
			return errCypherStop
		}
		value, err := r.eval(e.args[1], bound, aggregates)
		if err != nil {
			return err
		}
		if len(values) >= cypherMaxRows {
			return ErrLimitExceeded
		}
		if err := r.reserve(16 + cySize(value)); err != nil {
			return err
		}
		values = append(values, value)
		return nil
	})
	if err != nil && !errors.Is(err, errCypherStop) {
		return nil, err
	}
	if e.op == "pattern-exists" {
		return found, nil
	}
	return values, nil
}
