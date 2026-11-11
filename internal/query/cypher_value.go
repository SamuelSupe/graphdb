package query

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type cyNodeValue struct{ entity graph.Entity }
type cyRelValue struct{ edge graph.Edge }
type cyPathValue struct {
	nodes []*cyNodeValue
	rels  []*cyRelValue
}
type cyUnbound struct{}
type cyRow []any

func cyCheckListSize(size int) error {
	if size > cypherMaxRows {
		return fmt.Errorf("%w: list exceeds %d elements", ErrLimitExceeded, cypherMaxRows)
	}
	return nil
}

func cyNormalize(v any, depth int) (any, error) {
	if depth > 32 {
		return nil, fmt.Errorf("%w: value nesting exceeds 32", ErrInvalid)
	}
	switch x := v.(type) {
	case nil, bool, string, int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return nil, fmt.Errorf("%w: non-finite number", ErrInvalid)
		}
		return x, nil
	case float32:
		return cyNormalize(float64(x), depth)
	case json.Number:
		if !strings.ContainsAny(string(x), ".eE") {
			i, e := x.Int64()
			if e != nil {
				return nil, fmt.Errorf("%w: integer outside signed 64-bit range", ErrInvalid)
			}
			return i, nil
		}
		f, e := x.Float64()
		if e != nil {
			return nil, fmt.Errorf("%w: invalid number", ErrInvalid)
		}
		return cyNormalize(f, depth)
	case []any:
		if err := cyCheckListSize(len(x)); err != nil {
			return nil, err
		}
		out := make([]any, len(x))
		for i, v := range x {
			var err error
			out[i], err = cyNormalize(v, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case []string:
		if err := cyCheckListSize(len(x)); err != nil {
			return nil, err
		}
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = v
		}
		return out, nil
	case graph.Fields:
		return cyNormalize(map[string]any(x), depth)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			var err error
			out[k], err = cyNormalize(v, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T", ErrInvalid, v)
	}
}
func cyLabels(n *cyNodeValue) []any {
	labels := graph.EntityLabels(n.entity)
	out := make([]any, 0, len(labels)+1)
	if n.entity.Kind != "" {
		out = append(out, n.entity.Kind)
	}
	for _, l := range labels {
		if l != n.entity.Kind {
			out = append(out, l)
		}
	}
	return out
}
func cyProperties(v any) (map[string]any, error) {
	var fields map[string]any
	switch x := v.(type) {
	case *cyNodeValue:
		fields = map[string]any(x.entity.Fields)
	case *cyRelValue:
		fields = map[string]any(x.edge.Fields)
	case map[string]any:
		return x, nil
	default:
		return nil, fmt.Errorf("%w: expected node, relationship or map", ErrInvalid)
	}
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		if k == graph.ReservedLabelsField {
			continue
		}
		n, err := cyNormalize(v, 0)
		if err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, nil
}
func cyProperty(v any, name string) (any, error) {
	if v == nil {
		return nil, nil
	}
	var raw any
	switch x := v.(type) {
	case *cyNodeValue:
		if name == graph.ReservedLabelsField {
			return nil, nil
		}
		raw = x.entity.Fields[name]
	case *cyRelValue:
		if name == graph.ReservedLabelsField {
			return nil, nil
		}
		raw = x.edge.Fields[name]
	case map[string]any:
		raw = x[name]
	default:
		return nil, fmt.Errorf("%w: property access requires a node, relationship or map", ErrInvalid)
	}
	return cyNormalize(raw, 0)
}
func cyNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
func cyNumericCompare(a, b any) (int, bool) {
	if ai, ok := a.(int64); ok {
		if bi, yes := b.(int64); yes {
			if ai < bi {
				return -1, true
			}
			if ai > bi {
				return 1, true
			}
			return 0, true
		}
	}
	af, aok := cyNumber(a)
	bf, bok := cyNumber(b)
	if !aok || !bok {
		return 0, false
	}
	var ar, br *big.Rat
	if i, ok := a.(int64); ok {
		ar = new(big.Rat).SetInt64(i)
	} else {
		ar = new(big.Rat).SetFloat64(af)
	}
	if i, ok := b.(int64); ok {
		br = new(big.Rat).SetInt64(i)
	} else {
		br = new(big.Rat).SetFloat64(bf)
	}
	return ar.Cmp(br), true
}
func cyEqual(a, b any) any {
	if a == nil || b == nil {
		return nil
	}
	if cmp, ok := cyNumericCompare(a, b); ok {
		return cmp == 0
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case *cyNodeValue:
		y, ok := b.(*cyNodeValue)
		return ok && x.entity.ID == y.entity.ID
	case *cyRelValue:
		y, ok := b.(*cyRelValue)
		return ok && x.edge.ID == y.edge.ID
	case *cyPathValue:
		y, ok := b.(*cyPathValue)
		if !ok || len(x.nodes) != len(y.nodes) || len(x.rels) != len(y.rels) {
			return false
		}
		for i := range x.nodes {
			if cyEqual(x.nodes[i], y.nodes[i]) != true {
				return false
			}
		}
		for i := range x.rels {
			if cyEqual(x.rels[i], y.rels[i]) != true {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		unknown := false
		for i, v := range x {
			eq := cyEqual(v, y[i])
			if eq == false {
				return false
			}
			unknown = unknown || eq == nil
		}
		if unknown {
			return nil
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		unknown := false
		for k, v := range x {
			other, exists := y[k]
			if !exists {
				return false
			}
			eq := cyEqual(v, other)
			if eq == false {
				return false
			}
			unknown = unknown || eq == nil
		}
		if unknown {
			return nil
		}
		return true
	}
	return false
}

func cyKey(v any) string {
	var encode func(any) any
	encode = func(v any) any {
		switch x := v.(type) {
		case nil:
			return []any{"null"}
		case int64:
			return []any{"number", strconv.FormatInt(x, 10)}
		case float64:
			if x >= math.MinInt64 && x < 9223372036854775808.0 && math.Trunc(x) == x {
				return []any{"number", strconv.FormatInt(int64(x), 10)}
			}
			return []any{"number", strconv.FormatFloat(x, 'g', -1, 64)}
		case string:
			return []any{"string", x}
		case bool:
			return []any{"bool", x}
		case *cyNodeValue:
			return []any{"node", x.entity.ID}
		case *cyRelValue:
			return []any{"relationship", x.edge.ID}
		case *cyPathValue:
			ns := make([]any, len(x.nodes))
			rs := make([]any, len(x.rels))
			for i, n := range x.nodes {
				ns[i] = n.entity.ID
			}
			for i, r := range x.rels {
				rs[i] = r.edge.ID
			}
			return []any{"path", ns, rs}
		case []any:
			out := make([]any, len(x))
			for i, v := range x {
				out[i] = encode(v)
			}
			return []any{"list", out}
		case map[string]any:
			out := map[string]any{}
			for k, v := range x {
				out[k] = encode(v)
			}
			return []any{"map", out}
		}
		return []any{"invalid"}
	}
	b, _ := json.Marshal(encode(v))
	return string(b)
}
func cyRank(v any) int {
	switch v.(type) {
	case map[string]any:
		return 0
	case *cyNodeValue:
		return 1
	case *cyRelValue:
		return 2
	case []any:
		return 3
	case *cyPathValue:
		return 4
	case string:
		return 5
	case bool:
		return 6
	case int64, float64:
		return 7
	case nil:
		return 8
	}
	return 9
}
func cyCompare(a, b any) int {
	ra, rb := cyRank(a), cyRank(b)
	if ra != rb {
		if ra < rb {
			return -1
		}
		return 1
	}
	if c, ok := cyNumericCompare(a, b); ok {
		return c
	}
	switch x := a.(type) {
	case string:
		return strings.Compare(x, b.(string))
	case bool:
		y := b.(bool)
		if x == y {
			return 0
		}
		if x {
			return 1
		}
		return -1
	case []any:
		y := b.([]any)
		for i := 0; i < len(x) && i < len(y); i++ {
			if c := cyCompare(x[i], y[i]); c != 0 {
				return c
			}
		}
		if len(x) < len(y) {
			return -1
		}
		if len(x) > len(y) {
			return 1
		}
		return 0
	case nil:
		return 0
	}
	return strings.Compare(cyKey(a), cyKey(b))
}

// Predicate comparisons propagate unknown list elements. ORDER BY instead
// uses cyCompare's total order, where null sorts last.
func cyPredicateCompare(a, b any) (int, bool) {
	if a == nil || b == nil || cyRank(a) != cyRank(b) {
		return 0, false
	}
	if x, ok := a.([]any); ok {
		y := b.([]any)
		for i := 0; i < len(x) && i < len(y); i++ {
			c, known := cyPredicateCompare(x[i], y[i])
			if !known {
				return 0, false
			}
			if c != 0 {
				return c, true
			}
		}
		if len(x) < len(y) {
			return -1, true
		}
		if len(x) > len(y) {
			return 1, true
		}
		return 0, true
	}
	return cyCompare(a, b), true
}
func cyBool(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if b, ok := v.(bool); ok {
		return b, nil
	}
	return nil, fmt.Errorf("%w: expected boolean", ErrInvalid)
}
func cyLogical(op string, a, b any) (any, error) {
	var err error
	a, err = cyBool(a)
	if err != nil {
		return nil, err
	}
	b, err = cyBool(b)
	if err != nil {
		return nil, err
	}
	switch op {
	case "AND":
		if a == false || b == false {
			return false, nil
		}
		if a == nil || b == nil {
			return nil, nil
		}
		return true, nil
	case "OR":
		if a == true || b == true {
			return true, nil
		}
		if a == nil || b == nil {
			return nil, nil
		}
		return false, nil
	case "XOR":
		if a == nil || b == nil {
			return nil, nil
		}
		return a != b, nil
	}
	return nil, fmt.Errorf("%w: invalid boolean operator", ErrInvalid)
}
func cyArithmetic(op string, a, b any) (any, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	if op == "+" {
		x, leftList := a.([]any)
		y, rightList := b.([]any)
		if leftList || rightList {
			if !leftList {
				x = []any{a}
			}
			if !rightList {
				y = []any{b}
			}
			length := len(x) + len(y)
			if err := cyCheckListSize(length); err != nil {
				return nil, err
			}
			out := make([]any, 0, length)
			out = append(out, x...)
			return append(out, y...), nil
		}
		if x, ok := a.(string); ok {
			switch y := b.(type) {
			case string:
				return x + y, nil
			case int64, float64:
				return x + fmt.Sprint(y), nil
			}
		}
		if y, ok := b.(string); ok {
			if _, yes := cyNumber(a); yes {
				return fmt.Sprint(a) + y, nil
			}
		}
	}
	af, aok := cyNumber(a)
	bf, bok := cyNumber(b)
	if !aok || !bok {
		return nil, fmt.Errorf("%w: arithmetic requires numbers", ErrInvalid)
	}
	if ai, ok := a.(int64); ok {
		if bi, yes := b.(int64); yes && op != "^" {
			ar, br := big.NewInt(ai), big.NewInt(bi)
			switch op {
			case "+":
				ar.Add(ar, br)
			case "-":
				ar.Sub(ar, br)
			case "*":
				ar.Mul(ar, br)
			case "/":
				if bi == 0 {
					return nil, fmt.Errorf("%w: division by zero", ErrInvalid)
				}
				ar.Quo(ar, br)
			case "%":
				if bi == 0 {
					return nil, fmt.Errorf("%w: division by zero", ErrInvalid)
				}
				ar.Rem(ar, br)
			}
			if !ar.IsInt64() {
				return nil, fmt.Errorf("%w: integer overflow", ErrLimitExceeded)
			}
			return ar.Int64(), nil
		}
	}
	var out float64
	switch op {
	case "+":
		out = af + bf
	case "-":
		out = af - bf
	case "*":
		out = af * bf
	case "/":
		if bf == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrInvalid)
		}
		out = af / bf
	case "%":
		if bf == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrInvalid)
		}
		out = math.Mod(af, bf)
	case "^":
		out = math.Pow(af, bf)
	}
	if math.IsInf(out, 0) || math.IsNaN(out) {
		return nil, fmt.Errorf("%w: non-finite arithmetic result", ErrLimitExceeded)
	}
	return out, nil
}

func cySize(v any) int {
	switch x := v.(type) {
	case string:
		return 16 + len(x)
	case []any:
		n := 24 + 16*len(x)
		for _, v := range x {
			n += cySize(v)
		}
		return n
	case map[string]any:
		n := 48
		for k, v := range x {
			n += len(k) + 32 + cySize(v)
		}
		return n
	case *cyNodeValue:
		return 16
	case *cyRelValue:
		return 16
	case *cyPathValue:
		return 48 + 16*(len(x.nodes)+len(x.rels))
	}
	return 16
}
