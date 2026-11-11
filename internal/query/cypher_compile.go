package query

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

func (c *cyCompiler) slot() int { s := c.plan.slots; c.plan.slots++; return s }
func (c *cyCompiler) bind(n *cySyntax, kind string) (int, error) {
	if n == nil {
		return c.slot(), nil
	}
	name := n.name()
	if old, ok := c.scope[name]; ok {
		if old.kind != "any" && old.kind != kind {
			return 0, n.pos.fail("cypher_semantic_error", "variable "+name+" has incompatible type")
		}
		return old.slot, nil
	}
	s := c.slot()
	c.scope[name] = cySymbol{s, kind}
	return s, nil
}
func (c *cyCompiler) branch(n *cySyntax) (cyBranch, error) {
	c.scope = map[string]cySymbol{}
	var b cyBranch
	var walk func(*cySyntax) error
	walk = func(n *cySyntax) error {
		if n == nil {
			return fmt.Errorf("%w: missing query", ErrInvalid)
		}
		switch n.rule {
		case "Match":
			cl, err := c.match(n)
			if err != nil {
				return err
			}
			b.clauses = append(b.clauses, cl)
			return nil
		case "Unwind":
			e, err := c.expr(n.one("Expression"))
			if err != nil {
				return err
			}
			if cyHasAggregate(e) {
				return n.pos.fail("cypher_semantic_error", "aggregate in UNWIND")
			}
			v := n.one("Variable")
			if _, exists := c.scope[v.name()]; exists {
				return v.pos.fail("cypher_semantic_error", "UNWIND variable already exists")
			}
			s, err := c.bind(v, "any")
			if err != nil {
				return err
			}
			b.clauses = append(b.clauses, cyClause{kind: "unwind", expr: e, slot: s})
			return nil
		case "With", "Return":
			cl, err := c.projection(n)
			if err != nil {
				return err
			}
			b.clauses = append(b.clauses, cl)
			if n.rule == "Return" {
				for _, it := range cl.items {
					b.columns = append(b.columns, it.name)
				}
			}
			return nil
		}
		for i := range n.children {
			if err := walk(&n.children[i]); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(n); err != nil {
		return b, err
	}
	if len(b.columns) == 0 {
		return b, n.pos.fail("cypher_semantic_error", "query must RETURN at least one column")
	}
	if len(b.clauses) > 64 {
		return b, n.pos.fail("cypher_unsupported", "too many clauses")
	}
	return b, nil
}

func (c *cyCompiler) match(n *cySyntax) (cyClause, error) {
	cl := cyClause{kind: "match", optional: n.token("OPTIONAL")}
	before := c.plan.slots
	seen := map[int]bool{}
	for _, part := range n.one("Pattern").many("PatternPart") {
		p, err := c.pattern(part, seen)
		if err != nil {
			return cl, err
		}
		cl.patterns = append(cl.patterns, p)
	}
	if len(cl.patterns) > 1 {
		for _, p := range cl.patterns {
			if p.shortest != "" {
				return cl, n.pos.fail("cypher_unsupported", "shortest paths require a separate MATCH clause")
			}
		}
	}
	for i := before; i < c.plan.slots; i++ {
		cl.newSlots = append(cl.newSlots, i)
	}
	if w := n.one("Where"); w != nil {
		var err error
		cl.where, err = c.expr(w.one("Expression"))
		if err != nil {
			return cl, err
		}
		if cyHasAggregate(cl.where) {
			return cl, w.pos.fail("cypher_semantic_error", "aggregate in MATCH WHERE")
		}
	}
	return cl, nil
}
func (c *cyCompiler) pattern(n *cySyntax, seen map[int]bool) (cyPattern, error) {
	p := cyPattern{slot: -1}
	before := c.plan.slots
	var err error
	if v := n.one("Variable"); v != nil {
		p.slot, err = c.bind(v, "path")
		if err != nil {
			return p, err
		}
	}
	anonymous := n.one("AnonymousPatternPart")
	e := anonymous.one("PatternElement")
	if shortest := anonymous.one("ShortestPathPattern"); shortest != nil {
		p.shortest = strings.ToLower(shortest.one("ShortestPathName").text())
		e = shortest.one("PatternElement")
	}
	for e != nil && e.one("NodePattern") == nil {
		e = e.one("PatternElement")
	}
	if e == nil {
		return p, n.pos.fail("cypher_unsupported", "unsupported pattern")
	}
	first, err := c.node(e.one("NodePattern"))
	if err != nil {
		return p, err
	}
	p.nodes = append(p.nodes, first)
	for _, chain := range e.many("PatternElementChain") {
		r, err := c.relationship(chain.one("RelationshipPattern"))
		if err != nil {
			return p, err
		}
		if seen[r.slot] {
			return p, chain.pos.fail("cypher_semantic_error", "relationship variable repeated in MATCH")
		}
		seen[r.slot] = true
		nd, err := c.node(chain.one("NodePattern"))
		if err != nil {
			return p, err
		}
		p.rels = append(p.rels, r)
		p.nodes = append(p.nodes, nd)
	}
	depth := 0
	for _, r := range p.rels {
		depth += r.max
	}
	if depth > cypherMaxDepth {
		return p, n.pos.fail("cypher_unsupported", "pattern maximum length exceeds 16")
	}
	if p.shortest != "" && (len(p.rels) != 1 || !p.rels[0].variable || p.nodes[0].slot >= before || p.nodes[len(p.nodes)-1].slot >= before) {
		return p, n.pos.fail("cypher_unsupported", "shortest paths require bound endpoints and one bounded variable-length relationship")
	}
	return p, nil
}
func (c *cyCompiler) node(n *cySyntax) (cyNodePattern, error) {
	out := cyNodePattern{}
	var err error
	out.slot, err = c.bind(n.one("Variable"), "node")
	if err != nil {
		return out, err
	}
	if labels := n.one("NodeLabels"); labels != nil {
		for _, l := range labels.many("NodeLabel") {
			label := l.one("LabelName").name()
			if !slices.Contains(out.labels, label) {
				out.labels = append(out.labels, label)
			}
		}
	}
	out.props, err = c.properties(n.one("Properties"))
	return out, err
}
func (c *cyCompiler) relationship(n *cySyntax) (cyRelPattern, error) {
	r := cyRelPattern{direction: "both", min: 1, max: 1}
	if n.one("LeftArrowHead") != nil && n.one("RightArrowHead") == nil {
		r.direction = "in"
	}
	if n.one("RightArrowHead") != nil && n.one("LeftArrowHead") == nil {
		r.direction = "out"
	}
	if r.direction != "out" {
		c.plan.reverse = true
	}
	d := n.one("RelationshipDetail")
	var v *cySyntax
	if d != nil {
		v = d.one("Variable")
	}
	var err error
	kind := "relationship"
	if d != nil && d.one("RangeLiteral") != nil {
		kind = "list"
	}
	r.slot, err = c.bind(v, kind)
	if err != nil {
		return r, err
	}
	if d == nil {
		return r, nil
	}
	if types := d.one("RelationshipTypes"); types != nil {
		for _, t := range types.many("RelTypeName") {
			r.types = append(r.types, t.name())
		}
	}
	if bounds := d.one("RangeLiteral"); bounds != nil {
		r.variable = true
		text := strings.TrimSpace(strings.TrimPrefix(bounds.text(), "*"))
		parts := strings.Split(text, "..")
		parse := func(s string) (int, error) {
			i, e := strconv.ParseInt(strings.TrimSpace(s), 0, 32)
			if e != nil {
				return 0, bounds.pos.fail("cypher_semantic_error", "invalid path bound")
			}
			return int(i), nil
		}
		if len(parts) == 1 {
			if parts[0] == "" {
				return r, bounds.pos.fail("cypher_unsupported", "variable paths require an upper bound")
			}
			r.min, err = parse(parts[0])
			r.max = r.min
		} else {
			r.min = 1
			if strings.TrimSpace(parts[0]) != "" {
				r.min, err = parse(parts[0])
			}
			if strings.TrimSpace(parts[1]) == "" {
				return r, bounds.pos.fail("cypher_unsupported", "variable paths require an upper bound")
			}
			if err == nil {
				r.max, err = parse(parts[1])
			}
		}
		if err != nil {
			return r, err
		}
		if r.min < 0 || r.max < r.min || r.max > cypherMaxDepth {
			return r, bounds.pos.fail("cypher_unsupported", "path bounds must satisfy 0 <= min <= max <= 16")
		}
	}
	r.props, err = c.properties(d.one("Properties"))
	return r, err
}
func (c *cyCompiler) properties(n *cySyntax) (*cyExpr, error) {
	if n == nil {
		return nil, nil
	}
	if n.one("Parameter") != nil {
		return nil, n.pos.fail("cypher_unsupported", "pattern properties require a literal map")
	}
	return c.expr(n.one("MapLiteral"))
}
