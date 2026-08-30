package graph

import "sort"

func (g *Graph) MatchEntityIDs(kind string) []string {
	ids := g.sortedEntityIDs(kind)
	return append([]string(nil), ids...)
}

// VisitEntitiesByID visits the selected kind in stable ID order. The callback
// must treat the entity as read-only and must not retain it after returning.
func (g *Graph) VisitEntitiesByID(kind string, afterID string, visit func(Entity) (bool, error)) error {
	if visit == nil {
		return nil
	}
	ids := g.sortedEntityIDs(kind)
	start := 0
	if afterID != "" {
		start = sort.SearchStrings(ids, afterID)
		for start < len(ids) && ids[start] <= afterID {
			start++
		}
	}
	for _, id := range ids[start:] {
		entity, ok := g.Entities[id]
		if !ok {
			continue
		}
		keepGoing, err := visit(entity)
		if err != nil || !keepGoing {
			return err
		}
	}
	return nil
}

func (g *Graph) sortedEntityIDs(kind string) []string {
	g.entityOrderMu.Lock()
	defer g.entityOrderMu.Unlock()
	if ids, ok := g.entityOrder[kind]; ok {
		return ids
	}
	ids := make([]string, 0, g.KindCount(kind))
	for id, entity := range g.Entities {
		if kind == "" || entity.Kind == kind {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if g.entityOrder == nil {
		g.entityOrder = map[string][]string{}
	}
	g.entityOrder[kind] = ids
	return ids
}

func (g *Graph) invalidateEntityOrder() {
	g.entityOrderMu.Lock()
	g.entityOrder = nil
	g.entityOrderMu.Unlock()
}

func (g *Graph) MatchFieldIndexIDs(kind string, field string, values []any) []string {
	if len(values) == 1 {
		key, ok := scalarKey(values[0])
		if !ok {
			return nil
		}
		valueIDs := g.fieldIndex[kind][field][key]
		ids := make([]string, 0, len(valueIDs))
		for id := range valueIDs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids
	}
	keys := distinctScalarKeys(values)
	count := 0
	for _, key := range keys {
		count += len(g.fieldIndex[kind][field][key])
	}
	ids := make([]string, 0, count)
	for _, key := range keys {
		for id := range g.fieldIndex[kind][field][key] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (g *Graph) ScanFieldIndexIDs(
	kind string,
	field string,
	match func(string) (bool, error),
) ([]string, error) {
	if match == nil {
		return nil, nil
	}
	ids := make([]string, 0)
	for value, valueIDs := range g.fieldIndex[kind][field] {
		matched, err := match(value)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		for id := range valueIDs {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}
