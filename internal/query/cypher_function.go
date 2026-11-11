package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func (r *cyRuntime) function(e *cyExpr, row cyRow, aggregates map[*cyExpr]any) (any, error) {
	args := make([]any, 0, len(e.args))
	for _, a := range e.args {
		v, err := r.eval(a, row, aggregates)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
		if e.name == "coalesce" && v != nil {
			return v, nil
		}
	}
	if e.name == "coalesce" {
		return nil, nil
	}
	if e.name == "exists" {
		return args[0] != nil, nil
	}
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
	}
	bad := func() (any, error) { return nil, e.pos.fail("cypher_type_error", "invalid argument type for "+e.name) }
	x := args[0]
	switch e.name {
	case "graphdb.id":
		switch v := x.(type) {
		case *cyNodeValue:
			return v.entity.ID, nil
		case *cyRelValue:
			return v.edge.ID, nil
		}
		return bad()
	case "labels":
		n, ok := x.(*cyNodeValue)
		if !ok {
			return bad()
		}
		return cyLabels(n), nil
	case "type":
		rel, ok := x.(*cyRelValue)
		if !ok {
			return bad()
		}
		return rel.edge.Type, nil
	case "properties", "keys":
		m, err := cyProperties(x)
		if err != nil {
			return bad()
		}
		if e.name == "keys" {
			if err := cyCheckListSize(len(m)); err != nil {
				return nil, err
			}
			return cySortedKeys(m), nil
		}
		if err := r.reserve(cySize(m)); err != nil {
			return nil, err
		}
		return m, nil
	case "nodes", "relationships", "length":
		p, ok := x.(*cyPathValue)
		if !ok {
			return bad()
		}
		if e.name == "length" {
			return int64(len(p.rels)), nil
		}
		if e.name == "nodes" {
			out := make([]any, len(p.nodes))
			for i, n := range p.nodes {
				out[i] = n
			}
			return out, nil
		}
		out := make([]any, len(p.rels))
		for i, rel := range p.rels {
			out[i] = rel
		}
		return out, nil
	case "startnode", "endnode":
		rel, ok := x.(*cyRelValue)
		if !ok {
			return bad()
		}
		id := rel.edge.From
		if e.name == "endnode" {
			id = rel.edge.To
		}
		return r.node(id)
	case "size":
		switch v := x.(type) {
		case string:
			return cyRuneLen(v), nil
		case []any:
			return int64(len(v)), nil
		}
		return bad()
	case "head", "last":
		list, ok := x.([]any)
		if !ok {
			return bad()
		}
		if len(list) == 0 {
			return nil, nil
		}
		if e.name == "head" {
			return list[0], nil
		}
		return list[len(list)-1], nil
	case "reverse":
		switch v := x.(type) {
		case []any:
			if err := r.reserve(24 + 16*len(v)); err != nil {
				return nil, err
			}
			out := append([]any(nil), v...)
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
			return out, nil
		case string:
			if err := r.reserve(4 * len(v)); err != nil {
				return nil, err
			}
			out := []rune(v)
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
			return string(out), nil
		}
		return bad()
	case "tostring":
		switch v := x.(type) {
		case string:
			return v, nil
		case bool:
			return strconv.FormatBool(v), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		}
		return bad()
	case "tointeger":
		switch v := x.(type) {
		case int64:
			return v, nil
		case float64:
			if v < math.MinInt64 || v >= 9223372036854775808.0 {
				return nil, nil
			}
			return int64(v), nil
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i, nil
			} else if errors.Is(err, strconv.ErrRange) && !strings.ContainsAny(v, ".eEpP") {
				return nil, nil
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || math.IsNaN(f) || f < math.MinInt64 || f >= 9223372036854775808.0 {
				return nil, nil
			}
			return int64(f), nil
		}
		return bad()
	case "tofloat":
		switch v := x.(type) {
		case int64:
			return float64(v), nil
		case float64:
			return v, nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, nil
			}
			return f, nil
		}
		return bad()
	case "toboolean":
		switch v := x.(type) {
		case bool:
			return v, nil
		case string:
			if strings.EqualFold(v, "true") {
				return true, nil
			}
			if strings.EqualFold(v, "false") {
				return false, nil
			}
			return nil, nil
		}
		return bad()
	case "abs", "ceil", "ceiling", "floor", "round", "sign":
		f, ok := cyNumber(x)
		if !ok {
			return bad()
		}
		if e.name == "abs" {
			if i, ok := x.(int64); ok {
				if i == math.MinInt64 {
					return nil, fmt.Errorf("%w: abs integer overflow", ErrLimitExceeded)
				}
				if i < 0 {
					i = -i
				}
				return i, nil
			}
			return math.Abs(f), nil
		}
		switch e.name {
		case "ceil", "ceiling":
			return math.Ceil(f), nil
		case "floor":
			return math.Floor(f), nil
		case "round":
			return math.Round(f), nil
		case "sign":
			if f < 0 {
				return int64(-1), nil
			}
			if f > 0 {
				return int64(1), nil
			}
			return int64(0), nil
		}
	case "range":
		start, ok := args[0].(int64)
		if !ok {
			return bad()
		}
		end, ok := args[1].(int64)
		if !ok {
			return bad()
		}
		step := int64(1)
		if len(args) == 3 {
			step, ok = args[2].(int64)
			if !ok {
				return bad()
			}
		}
		if step == 0 {
			return nil, e.pos.fail("cypher_semantic_error", "range step cannot be zero")
		}
		out := []any{}
		for i := start; (step > 0 && i <= end) || (step < 0 && i >= end); {
			if len(out) >= cypherMaxRows {
				return nil, fmt.Errorf("%w: range exceeds %d elements", ErrLimitExceeded, cypherMaxRows)
			}
			if err := r.budget.add(1); err != nil {
				return nil, err
			}
			if err := r.reserve(16); err != nil {
				return nil, err
			}
			out = append(out, i)
			next := i + step
			if (step > 0 && next < i) || (step < 0 && next > i) {
				break
			}
			i = next
		}
		return out, nil
	case "tolower", "toupper", "trim", "ltrim", "rtrim":
		s, ok := x.(string)
		if !ok {
			return bad()
		}
		if err := r.reserve(3 * len(s)); err != nil {
			return nil, err
		}
		switch e.name {
		case "tolower":
			return strings.ToLower(s), nil
		case "toupper":
			return strings.ToUpper(s), nil
		case "trim":
			return strings.TrimSpace(s), nil
		case "ltrim":
			return strings.TrimLeft(s, " \t\r\n\v\f"), nil
		case "rtrim":
			return strings.TrimRight(s, " \t\r\n\v\f"), nil
		}
	case "split":
		s, ok := x.(string)
		sep, yes := args[1].(string)
		if !ok || !yes {
			return bad()
		}
		var count int
		if sep == "" {
			count = int(cyRuneLen(s))
		} else {
			count = strings.Count(s, sep) + 1
		}
		if err := cyCheckListSize(count); err != nil {
			return nil, err
		}
		if err := r.reserve(32 + len(s)*32); err != nil {
			return nil, err
		}
		parts := strings.Split(s, sep)
		out := make([]any, len(parts))
		for i, v := range parts {
			out[i] = v
		}
		return out, nil
	case "replace":
		s, ok := x.(string)
		old, yes := args[1].(string)
		replacement, valid := args[2].(string)
		if !ok || !yes || !valid {
			return bad()
		}
		count := strings.Count(s, old)
		if count > 0 && len(replacement) > cypherMaxMemory/count {
			return nil, fmt.Errorf("%w: replacement too large", ErrLimitExceeded)
		}
		if err := r.reserve(len(s) + count*len(replacement)); err != nil {
			return nil, err
		}
		return strings.ReplaceAll(s, old, replacement), nil
	case "substring", "left", "right":
		s, ok := x.(string)
		offset, yes := args[1].(int64)
		if !ok || !yes {
			return bad()
		}
		if offset < 0 {
			return nil, e.pos.fail("cypher_semantic_error", "string offset/length must be nonnegative")
		}
		if err := r.reserve(4 * len(s)); err != nil {
			return nil, err
		}
		chars := []rune(s)
		if e.name == "left" {
			return string(chars[:min(int64(len(chars)), offset)]), nil
		}
		if e.name == "right" {
			return string(chars[max(int64(0), int64(len(chars))-offset):]), nil
		}
		start := min(int64(len(chars)), offset)
		end := int64(len(chars))
		if len(args) == 3 {
			length, valid := args[2].(int64)
			if !valid {
				return bad()
			}
			if length < 0 {
				return nil, e.pos.fail("cypher_semantic_error", "substring length must be nonnegative")
			}
			end = start + min(int64(len(chars))-start, length)
		}
		return string(chars[start:end]), nil
	}
	return nil, e.pos.fail("cypher_unsupported", "unsupported function "+e.name)
}
