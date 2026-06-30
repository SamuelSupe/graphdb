package graph

import "sort"

func (g *Graph) MatchEntityIDs(kind string) []string {
	ids := make([]string, 0, len(g.Entities))
	for id, entity := range g.Entities {
		if kind == "" || entity.Kind == kind {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (g *Graph) MatchFieldIndexIDs(kind string, field string, values []any) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		key, ok := scalarKey(value)
		if !ok {
			continue
		}
		for id := range g.fieldIndex[kind][field][key] {
			seen[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
