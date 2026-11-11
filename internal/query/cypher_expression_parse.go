package query

import (
	"strconv"
	"strings"
)

func (c *cyCompiler) expr(n *cySyntax) (*cyExpr, error) {
	if n == nil {
		return nil, nil
	}
	c.exprs++
	if c.exprs > 32768 {
		return nil, n.pos.fail("cypher_unsupported", "expression tree is too large")
	}
	switch n.rule {
	case "Variable":
		name := n.name()
		s, ok := c.scope[name]
		if !ok {
			return nil, n.pos.fail("cypher_semantic_error", "undefined variable "+name)
		}
		return &cyExpr{op: "var", slot: s.slot, pos: n.pos}, nil
	case "Parameter":
		name := strings.TrimPrefix(n.text(), "$")
		if strings.HasPrefix(name, "`") {
			name = strings.ReplaceAll(name[1:len(name)-1], "``", "`")
		}
		if _, ok := c.plan.request.Parameters[name]; !ok {
			return nil, n.pos.fail("cypher_parameter_missing", "missing parameter "+name)
		}
		return &cyExpr{op: "param", name: name, pos: n.pos, slot: -1}, nil
	case "IntegerLiteral":
		v, err := strconv.ParseInt(n.text(), 0, 64)
		if err != nil {
			return nil, n.pos.fail("cypher_semantic_error", "integer is outside signed 64-bit range")
		}
		return &cyExpr{op: "literal", value: v, pos: n.pos, slot: -1}, nil
	case "DoubleLiteral":
		v, err := strconv.ParseFloat(n.text(), 64)
		if err != nil {
			return nil, n.pos.fail("cypher_semantic_error", "invalid floating point literal")
		}
		return &cyExpr{op: "literal", value: v, pos: n.pos, slot: -1}, nil
	case "BooleanLiteral":
		return &cyExpr{op: "literal", value: strings.EqualFold(n.text(), "true"), pos: n.pos, slot: -1}, nil
	case "Literal":
		if n.token("NULL") {
			return &cyExpr{op: "literal", pos: n.pos, slot: -1}, nil
		}
		for i := range n.children {
			ch := &n.children[i]
			if ch.rule != "" {
				return c.expr(ch)
			}
			text := ch.text()
			if len(text) > 1 && (text[0] == '\'' || text[0] == '"') {
				s, err := cyString(text)
				if err != nil {
					return nil, ch.pos.fail("cypher_syntax_error", "invalid string escape")
				}
				return &cyExpr{op: "literal", value: s, pos: n.pos, slot: -1}, nil
			}
		}
	case "ListLiteral":
		e := &cyExpr{op: "list", pos: n.pos, slot: -1}
		for _, a := range n.many("Expression") {
			v, err := c.expr(a)
			if err != nil {
				return nil, err
			}
			e.args = append(e.args, v)
		}
		return e, nil
	case "MapLiteral":
		e := &cyExpr{op: "map", pos: n.pos, slot: -1}
		keys := n.many("PropertyKeyName")
		for i, a := range n.many("Expression") {
			v, err := c.expr(a)
			if err != nil {
				return nil, err
			}
			key := keys[i].name()
			for _, old := range e.keys {
				if old == key {
					return nil, n.pos.fail("cypher_semantic_error", "duplicate map key "+key)
				}
			}
			e.keys = append(e.keys, key)
			e.args = append(e.args, v)
		}
		return e, nil
	case "FunctionInvocation":
		e := &cyExpr{op: "call", name: strings.ToLower(n.one("FunctionName").text()), distinct: n.token("DISTINCT"), pos: n.pos, slot: -1}
		for _, a := range n.many("Expression") {
			v, err := c.expr(a)
			if err != nil {
				return nil, err
			}
			e.args = append(e.args, v)
		}
		if err := cyValidateFunction(e); err != nil {
			return nil, err
		}
		if e.name == "exists" && len(e.args) == 1 && e.args[0].op == "pattern-exists" {
			return e.args[0], nil
		}
		return e, nil
	case "Atom":
		if n.token("COUNT") {
			return &cyExpr{op: "call", name: "count", pos: n.pos, slot: -1}, nil
		}
	case "NonArithmeticOperatorExpression":
		base, err := c.expr(n.one("Atom"))
		if err != nil {
			return nil, err
		}
		for i := range n.children {
			ch := &n.children[i]
			switch ch.rule {
			case "PropertyLookup":
				base = &cyExpr{op: "property", name: ch.one("PropertyKeyName").name(), args: []*cyExpr{base}, pos: ch.pos}
			case "ListOperatorExpression":
				xs := ch.many("Expression")
				if ch.token("..") {
					lo, hi := &cyExpr{op: "omitted", pos: ch.pos}, &cyExpr{op: "omitted", pos: ch.pos}
					for _, x := range xs {
						v, er := c.expr(x)
						if er != nil {
							return nil, er
						}
						before := true
						for i := range ch.children {
							part := &ch.children[i]
							if part == x {
								break
							}
							if part.rule == "" && part.text() == ".." {
								before = false
							}
						}
						if before {
							lo = v
						} else {
							hi = v
						}
					}
					base = &cyExpr{op: "slice", args: []*cyExpr{base, lo, hi}, pos: ch.pos}
				} else {
					v, er := c.expr(xs[0])
					if er != nil {
						return nil, er
					}
					base = &cyExpr{op: "index", args: []*cyExpr{base, v}, pos: ch.pos}
				}
			case "NodeLabels":
				original := base
				var predicate *cyExpr
				for _, label := range ch.many("NodeLabel") {
					item := &cyExpr{op: "label", name: label.one("LabelName").name(), args: []*cyExpr{original}, pos: label.pos}
					if predicate == nil {
						predicate = item
					} else {
						predicate = &cyExpr{op: "binary", name: "AND", args: []*cyExpr{predicate, item}, pos: ch.pos}
					}
				}
				base = predicate
			}
		}
		return base, nil
	case "ComparisonExpression":
		left, err := c.expr(n.one("StringListNullPredicateExpression"))
		if err != nil {
			return nil, err
		}
		var combined *cyExpr
		for _, part := range n.many("PartialComparisonExpression") {
			right, er := c.expr(part.one("StringListNullPredicateExpression"))
			if er != nil {
				return nil, er
			}
			cmp := &cyExpr{op: "binary", name: part.children[0].text(), args: []*cyExpr{left, right}, pos: part.pos}
			if combined == nil {
				combined = cmp
			} else {
				combined = &cyExpr{op: "binary", name: "AND", args: []*cyExpr{combined, cmp}, pos: n.pos}
			}
			left = right
		}
		if combined != nil {
			return combined, nil
		}
		return left, nil
	case "StringListNullPredicateExpression":
		base, err := c.expr(n.one("AddOrSubtractExpression"))
		if err != nil {
			return nil, err
		}
		for i := range n.children {
			ch := &n.children[i]
			switch ch.rule {
			case "StringPredicateExpression", "ListPredicateExpression":
				op := "IN"
				if ch.token("CONTAINS") {
					op = "CONTAINS"
				} else if ch.token("STARTS") {
					op = "STARTS WITH"
				} else if ch.token("ENDS") {
					op = "ENDS WITH"
				}
				right, er := c.expr(ch.one("AddOrSubtractExpression"))
				if er != nil {
					return nil, er
				}
				base = &cyExpr{op: "binary", name: op, args: []*cyExpr{base, right}, pos: ch.pos}
			case "NullPredicateExpression":
				op := "IS NULL"
				if ch.token("NOT") {
					op = "IS NOT NULL"
				}
				base = &cyExpr{op: "unary", name: op, args: []*cyExpr{base}, pos: ch.pos}
			}
		}
		return base, nil
	case "OrExpression", "XorExpression", "AndExpression", "AddOrSubtractExpression", "MultiplyDivideModuloExpression", "PowerOfExpression":
		var base *cyExpr
		op := ""
		for i := range n.children {
			ch := &n.children[i]
			if ch.rule == "" {
				op = strings.ToUpper(ch.text())
				continue
			}
			v, err := c.expr(ch)
			if err != nil {
				return nil, err
			}
			if base == nil {
				base = v
			} else {
				base = &cyExpr{op: "binary", name: op, args: []*cyExpr{base, v}, pos: n.pos}
			}
		}
		return base, nil
	case "NotExpression", "UnaryAddOrSubtractExpression":
		if n.rule == "UnaryAddOrSubtractExpression" && n.token("-") {
			if operand := n.one("NonArithmeticOperatorExpression"); operand != nil {
				if v, err := strconv.ParseUint(operand.text(), 0, 64); err == nil && v == uint64(1)<<63 {
					return &cyExpr{op: "literal", value: int64(-9223372036854775808), pos: n.pos}, nil
				}
			}
		}
		var base *cyExpr
		ops := []string{}
		for i := range n.children {
			ch := &n.children[i]
			if ch.rule == "" {
				ops = append(ops, strings.ToUpper(ch.text()))
			} else {
				var err error
				base, err = c.expr(ch)
				if err != nil {
					return nil, err
				}
			}
		}
		for i := len(ops) - 1; i >= 0; i-- {
			base = &cyExpr{op: "unary", name: ops[i], args: []*cyExpr{base}, pos: n.pos}
		}
		return base, nil
	case "CaseExpression":
		e := &cyExpr{op: "case", pos: n.pos, slot: -1}
		direct := n.many("Expression")
		if len(n.children) > 1 && n.children[1].rule == "Expression" {
			e.name = "simple"
			v, err := c.expr(direct[0])
			if err != nil {
				return nil, err
			}
			e.args = append(e.args, v)
		}
		for _, alt := range n.many("CaseAlternative") {
			for _, a := range alt.many("Expression") {
				v, err := c.expr(a)
				if err != nil {
					return nil, err
				}
				e.args = append(e.args, v)
			}
		}
		last := &cyExpr{op: "literal", pos: n.pos}
		if n.token("ELSE") {
			var err error
			last, err = c.expr(direct[len(direct)-1])
			if err != nil {
				return nil, err
			}
		}
		e.args = append(e.args, last)
		return e, nil
	case "ListComprehension", "Quantifier":
		filter := n.one("FilterExpression")
		id := filter.one("IdInColl")
		list, err := c.expr(id.one("Expression"))
		if err != nil {
			return nil, err
		}
		scope := c.scope
		c.scope = map[string]cySymbol{}
		for k, v := range scope {
			c.scope[k] = v
		}
		s := c.slot()
		c.scope[id.one("Variable").name()] = cySymbol{s, "any"}
		defer func() { c.scope = scope }()
		pred := &cyExpr{op: "literal", value: true, pos: n.pos}
		if w := filter.one("Where"); w != nil {
			pred, err = c.expr(w.one("Expression"))
			if err != nil {
				return nil, err
			}
		}
		project := &cyExpr{op: "var", slot: s, pos: n.pos}
		if x := n.one("Expression"); x != nil {
			project, err = c.expr(x)
			if err != nil {
				return nil, err
			}
		}
		e := &cyExpr{op: "comprehension", slot: s, args: []*cyExpr{list, pred, project}, pos: n.pos}
		if n.rule == "Quantifier" {
			e.op = "quantifier"
			e.name = strings.ToUpper(n.children[0].text())
		}
		return e, nil
	case "PatternComprehension", "PatternPredicate", "ExistentialSubquery":
		return c.patternExpression(n)
	}
	if len(n.children) == 1 && n.children[0].rule != "" {
		return c.expr(&n.children[0])
	}
	if n.rule == "ParenthesizedExpression" {
		return c.expr(n.one("Expression"))
	}
	return nil, n.pos.fail("cypher_unsupported", "unsupported expression "+n.rule)
}

func cyValidateFunction(e *cyExpr) error {
	min, max := 1, 1
	switch e.name {
	case "count":
		min = 1
	case "sum", "avg", "min", "max", "collect", "labels", "type", "properties", "keys", "size", "length", "head", "last", "tolower", "toupper", "trim", "ltrim", "rtrim", "reverse", "nodes", "relationships", "startnode", "endnode", "graphdb.id", "tostring", "tointeger", "tofloat", "toboolean", "abs", "ceil", "ceiling", "floor", "round", "sign", "exists":
	case "coalesce":
		max = 256
	case "range", "substring":
		min = 2
		max = 3
	case "split", "left", "right":
		min = 2
		max = 2
	case "replace":
		min = 3
		max = 3
	default:
		return e.pos.fail("cypher_unsupported", "unsupported function "+e.name)
	}
	if len(e.args) < min || len(e.args) > max {
		return e.pos.fail("cypher_semantic_error", "invalid argument count for "+e.name)
	}
	if e.distinct && !cyAggregate(e) {
		return e.pos.fail("cypher_semantic_error", "DISTINCT requires an aggregate")
	}
	return nil
}

func cyString(s string) (string, error) {
	quote := s[0]
	body := s[1 : len(s)-1]
	var b strings.Builder
	for body != "" {
		r, _, rest, err := strconv.UnquoteChar(body, quote)
		if err != nil {
			return "", err
		}
		b.WriteRune(r)
		body = rest
	}
	return b.String(), nil
}
