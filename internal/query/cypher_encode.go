package query

import (
	"sort"
	"unicode/utf8"
)

func cyWire(v any) (any, error) {
	switch x := v.(type) {
	case *cyNodeValue:
		p, e := cyProperties(x)
		if e != nil {
			return nil, e
		}
		return map[string]any{"$type": "node", "id": x.entity.ID, "labels": cyLabels(x), "properties": p}, nil
	case *cyRelValue:
		p, e := cyProperties(x)
		if e != nil {
			return nil, e
		}
		return map[string]any{"$type": "relationship", "id": x.edge.ID, "type": x.edge.Type, "from": x.edge.From, "to": x.edge.To, "properties": p}, nil
	case *cyPathValue:
		ns := make([]any, len(x.nodes))
		rs := make([]any, len(x.rels))
		for i, n := range x.nodes {
			v, e := cyWire(n)
			if e != nil {
				return nil, e
			}
			ns[i] = v
		}
		for i, r := range x.rels {
			v, e := cyWire(r)
			if e != nil {
				return nil, e
			}
			rs[i] = v
		}
		return map[string]any{"$type": "path", "nodes": ns, "relationships": rs}, nil
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			v, e := cyWire(v)
			if e != nil {
				return nil, e
			}
			out[i] = v
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			v, e := cyWire(v)
			if e != nil {
				return nil, e
			}
			out[k] = v
		}
		return out, nil
	}
	return v, nil
}

func cySortedKeys(m map[string]any) []any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = k
	}
	return out
}
func cyRuneLen(s string) int64 { return int64(utf8.RuneCountInString(s)) }
