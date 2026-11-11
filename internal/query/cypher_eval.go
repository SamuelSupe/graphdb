package query

import (
	"errors"
	"strings"
)

func (r *cyRuntime) eval(e *cyExpr, row cyRow, aggregates map[*cyExpr]any) (result any, err error) {
	if r.memoryErr != nil {
		return nil, r.memoryErr
	}
	if e == nil {
		return nil, nil
	}
	defer func() {
		var typed *CypherError
		if err != nil && errors.Is(err, ErrInvalid) && !errors.As(err, &typed) {
			err = e.pos.fail("cypher_type_error", err.Error())
		}
	}()
	if err := r.budget.add(1); err != nil {
		return nil, err
	}
	if cyAggregate(e) {
		if aggregates == nil {
			return nil, e.pos.fail("cypher_semantic_error", "aggregate outside projection")
		}
		return aggregates[e], nil
	}
	arg := func(i int) (any, error) { return r.eval(e.args[i], row, aggregates) }
	switch e.op {
	case "literal", "omitted":
		return e.value, nil
	case "param":
		return r.plan.request.Parameters[e.name], nil
	case "var":
		v := row[e.slot]
		if _, missing := v.(cyUnbound); missing {
			return nil, e.pos.fail("cypher_semantic_error", "unbound variable")
		}
		return v, nil
	case "property":
		v, err := arg(0)
		if err != nil {
			return nil, err
		}
		v, err = cyProperty(v, e.name)
		if err != nil {
			return nil, e.pos.fail("cypher_type_error", err.Error())
		}
		return v, nil
	case "label":
		v, err := arg(0)
		if err != nil || v == nil {
			return nil, err
		}
		n, ok := v.(*cyNodeValue)
		if !ok {
			return nil, e.pos.fail("cypher_type_error", "label predicate requires a node")
		}
		for _, label := range cyLabels(n) {
			if label == e.name {
				return true, nil
			}
		}
		return false, nil
	case "list", "map":
		xs := make([]any, len(e.args))
		for i := range xs {
			v, err := arg(i)
			if err != nil {
				return nil, err
			}
			xs[i] = v
		}
		if err := r.reserve(32 + len(xs)*32); err != nil {
			return nil, err
		}
		if e.op == "list" {
			return xs, nil
		}
		m := make(map[string]any, len(xs))
		for i, v := range xs {
			m[e.keys[i]] = v
		}
		return m, nil
	case "unary":
		v, err := arg(0)
		if err != nil {
			return nil, err
		}
		switch e.name {
		case "IS NULL":
			return v == nil, nil
		case "IS NOT NULL":
			return v != nil, nil
		case "NOT":
			b, er := cyBool(v)
			if er != nil {
				return nil, e.pos.fail("cypher_type_error", er.Error())
			}
			if b == nil {
				return nil, nil
			}
			return !b.(bool), nil
		case "+":
			if v == nil {
				return nil, nil
			}
			if _, ok := cyNumber(v); !ok {
				return nil, e.pos.fail("cypher_type_error", "unary + requires a number")
			}
			return v, nil
		case "-":
			return cyArithmetic("-", int64(0), v)
		}
	case "binary":
		a, err := arg(0)
		if err != nil {
			return nil, err
		}
		b, err := arg(1)
		if err != nil {
			return nil, err
		}
		switch e.name {
		case "AND", "OR", "XOR":
			return cyLogical(e.name, a, b)
		case "=":
			return cyEqual(a, b), nil
		case "<>":
			eq := cyEqual(a, b)
			if eq == nil {
				return nil, nil
			}
			return !eq.(bool), nil
		case "IN":
			if b == nil {
				return nil, nil
			}
			list, ok := b.([]any)
			if !ok {
				return nil, e.pos.fail("cypher_type_error", "IN requires a list")
			}
			unknown := false
			for _, v := range list {
				eq := cyEqual(a, v)
				if eq == true {
					return true, nil
				}
				unknown = unknown || eq == nil
			}
			if unknown {
				return nil, nil
			}
			return false, nil
		case "STARTS WITH", "ENDS WITH", "CONTAINS":
			if a == nil || b == nil {
				return nil, nil
			}
			as, aok := a.(string)
			bs, bok := b.(string)
			if !aok || !bok {
				return nil, nil
			}
			switch e.name {
			case "STARTS WITH":
				return strings.HasPrefix(as, bs), nil
			case "ENDS WITH":
				return strings.HasSuffix(as, bs), nil
			}
			return strings.Contains(as, bs), nil
		case "<", ">", "<=", ">=":
			if a == nil || b == nil {
				return nil, nil
			}
			if cyRank(a) != cyRank(b) {
				return nil, nil
			}
			cmp, known := cyPredicateCompare(a, b)
			if !known {
				return nil, nil
			}
			switch e.name {
			case "<":
				return cmp < 0, nil
			case ">":
				return cmp > 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">=":
				return cmp >= 0, nil
			}
		default:
			if err := r.reserve(cySize(a) + cySize(b)); err != nil {
				return nil, err
			}
			return cyArithmetic(e.name, a, b)
		}
	case "index":
		base, err := arg(0)
		if err != nil {
			return nil, err
		}
		index, err := arg(1)
		if err != nil {
			return nil, err
		}
		if base == nil || index == nil {
			return nil, nil
		}
		if key, ok := index.(string); ok {
			return cyProperty(base, key)
		}
		list, ok := base.([]any)
		i, yes := index.(int64)
		if !ok || !yes {
			return nil, e.pos.fail("cypher_type_error", "list indexing requires an integer")
		}
		if i < 0 {
			i += int64(len(list))
		}
		if i < 0 || i >= int64(len(list)) {
			return nil, nil
		}
		return list[i], nil
	case "slice":
		base, err := arg(0)
		if err != nil || base == nil {
			return nil, err
		}
		list, ok := base.([]any)
		if !ok {
			return nil, e.pos.fail("cypher_type_error", "slice requires a list")
		}
		lo, err := arg(1)
		if err != nil {
			return nil, err
		}
		hi, err := arg(2)
		if err != nil {
			return nil, err
		}
		if (lo == nil && e.args[1].op != "omitted") || (hi == nil && e.args[2].op != "omitted") {
			return nil, nil
		}
		bound := func(v any, def int) (int, error) {
			if v == nil {
				return def, nil
			}
			i, ok := v.(int64)
			if !ok {
				return 0, e.pos.fail("cypher_type_error", "slice bounds require integers")
			}
			if i < 0 {
				i += int64(len(list))
			}
			return int(max(int64(0), min(int64(len(list)), i))), nil
		}
		start, err := bound(lo, 0)
		if err != nil {
			return nil, err
		}
		end, err := bound(hi, len(list))
		if err != nil {
			return nil, err
		}
		if end < start {
			end = start
		}
		return list[start:end], nil
	case "case":
		start := 0
		var base any
		var err error
		if e.name == "simple" {
			base, err = arg(0)
			if err != nil {
				return nil, err
			}
			start = 1
		}
		for i := start; i < len(e.args)-1; i += 2 {
			cond, er := arg(i)
			if er != nil {
				return nil, er
			}
			if e.name == "simple" {
				cond = cyEqual(base, cond)
			} else {
				cond, er = cyBool(cond)
				if er != nil {
					return nil, e.pos.fail("cypher_type_error", er.Error())
				}
			}
			if cond == true {
				return arg(i + 1)
			}
		}
		return arg(len(e.args) - 1)
	case "comprehension", "quantifier":
		v, err := arg(0)
		if err != nil || v == nil {
			return nil, err
		}
		xs, ok := v.([]any)
		if !ok {
			return nil, e.pos.fail("cypher_type_error", "comprehension requires a list")
		}
		local := r.cloneRow(row)
		out := []any{}
		truth, unknown := 0, 0
		for _, x := range xs {
			local[e.slot] = x
			pred, er := r.eval(e.args[1], local, aggregates)
			if er != nil {
				return nil, er
			}
			pred, er = cyBool(pred)
			if er != nil {
				return nil, e.pos.fail("cypher_type_error", er.Error())
			}
			if pred == nil {
				unknown++
			}
			if pred == true {
				truth++
				if e.op == "comprehension" {
					value, er := r.eval(e.args[2], local, aggregates)
					if er != nil {
						return nil, er
					}
					if err := r.reserve(16); err != nil {
						return nil, err
					}
					out = append(out, value)
				}
			}
		}
		if e.op == "comprehension" {
			return out, nil
		}
		switch e.name {
		case "ALL":
			if truth+unknown < len(xs) {
				return false, nil
			}
			if unknown > 0 {
				return nil, nil
			}
			return true, nil
		case "ANY":
			if truth > 0 {
				return true, nil
			}
			if unknown > 0 {
				return nil, nil
			}
			return false, nil
		case "NONE":
			if truth > 0 {
				return false, nil
			}
			if unknown > 0 {
				return nil, nil
			}
			return true, nil
		case "SINGLE":
			if truth > 1 {
				return false, nil
			}
			if unknown > 0 {
				return nil, nil
			}
			return truth == 1, nil
		}
	case "call":
		return r.function(e, row, aggregates)
	case "pattern-exists", "pattern-list":
		return r.patternExpression(e, row, aggregates)
	}
	return nil, e.pos.fail("cypher_unsupported", "unsupported expression operation "+e.op)
}
