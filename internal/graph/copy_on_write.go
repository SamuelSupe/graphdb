package graph

// copyOnWriteState tracks index buckets shared with the source graph by a
// storage-only mutation copy. Public graph copies continue to be fully deep.
type copyOnWriteState struct {
	outNodes      map[string]struct{}
	inNodes       map[string]struct{}
	identityKinds map[string]struct{}
	fieldKinds    map[string]struct{}
	fieldNames    map[string]map[string]struct{}
	fieldValues   map[string]map[string]map[string]struct{}
}

func (g *Graph) cloneForStorageMutation() *Graph {
	return &Graph{
		Version:       g.Version,
		CITypes:       shallowCopyMap(g.CITypes),
		Entities:      shallowCopyMap(g.Entities),
		RelationTypes: shallowCopyMap(g.RelationTypes),
		Edges:         shallowCopyMap(g.Edges),
		out:           shallowCopyMap(g.out),
		in:            shallowCopyMap(g.in),
		fieldIndex:    shallowCopyMap(g.fieldIndex),
		identityIndex: shallowCopyMap(g.identityIndex),
		cow: &copyOnWriteState{
			outNodes:      map[string]struct{}{},
			inNodes:       map[string]struct{}{},
			identityKinds: map[string]struct{}{},
			fieldKinds:    map[string]struct{}{},
			fieldNames:    map[string]map[string]struct{}{},
			fieldValues:   map[string]map[string]map[string]struct{}{},
		},
	}
}

func shallowCopyMap[K comparable, V any](source map[K]V) map[K]V {
	copy := make(map[K]V, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func copyStringSet(source map[string]struct{}) map[string]struct{} {
	return shallowCopyMap(source)
}

func (g *Graph) writableOut(node string) map[string]struct{} {
	if g.cow == nil {
		if g.out[node] == nil {
			g.out[node] = map[string]struct{}{}
		}
		return g.out[node]
	}
	if _, ok := g.cow.outNodes[node]; !ok {
		g.out[node] = copyStringSet(g.out[node])
		g.cow.outNodes[node] = struct{}{}
	}
	if g.out[node] == nil {
		g.out[node] = map[string]struct{}{}
	}
	return g.out[node]
}

func (g *Graph) writableIn(node string) map[string]struct{} {
	if g.cow == nil {
		if g.in[node] == nil {
			g.in[node] = map[string]struct{}{}
		}
		return g.in[node]
	}
	if _, ok := g.cow.inNodes[node]; !ok {
		g.in[node] = copyStringSet(g.in[node])
		g.cow.inNodes[node] = struct{}{}
	}
	if g.in[node] == nil {
		g.in[node] = map[string]struct{}{}
	}
	return g.in[node]
}

func (g *Graph) writableIdentityKind(kind string) map[string]string {
	if g.cow == nil {
		if g.identityIndex[kind] == nil {
			g.identityIndex[kind] = map[string]string{}
		}
		return g.identityIndex[kind]
	}
	if _, ok := g.cow.identityKinds[kind]; !ok {
		g.identityIndex[kind] = shallowCopyMap(g.identityIndex[kind])
		g.cow.identityKinds[kind] = struct{}{}
	}
	return g.identityIndex[kind]
}

func (g *Graph) writableFieldKind(kind string) map[string]map[string]map[string]struct{} {
	if g.cow == nil {
		if g.fieldIndex[kind] == nil {
			g.fieldIndex[kind] = map[string]map[string]map[string]struct{}{}
		}
		return g.fieldIndex[kind]
	}
	if _, ok := g.cow.fieldKinds[kind]; !ok {
		g.fieldIndex[kind] = shallowCopyMap(g.fieldIndex[kind])
		g.cow.fieldKinds[kind] = struct{}{}
	}
	return g.fieldIndex[kind]
}

func (g *Graph) writableFieldName(kind, field string) map[string]map[string]struct{} {
	byKind := g.writableFieldKind(kind)
	if g.cow == nil {
		if byKind[field] == nil {
			byKind[field] = map[string]map[string]struct{}{}
		}
		return byKind[field]
	}
	if g.cow.fieldNames[kind] == nil {
		g.cow.fieldNames[kind] = map[string]struct{}{}
	}
	if _, ok := g.cow.fieldNames[kind][field]; !ok {
		byKind[field] = shallowCopyMap(byKind[field])
		g.cow.fieldNames[kind][field] = struct{}{}
	}
	return byKind[field]
}

func (g *Graph) writableFieldValue(kind, field, value string) map[string]struct{} {
	byField := g.writableFieldName(kind, field)
	if g.cow == nil {
		if byField[value] == nil {
			byField[value] = map[string]struct{}{}
		}
		return byField[value]
	}
	if g.cow.fieldValues[kind] == nil {
		g.cow.fieldValues[kind] = map[string]map[string]struct{}{}
	}
	if g.cow.fieldValues[kind][field] == nil {
		g.cow.fieldValues[kind][field] = map[string]struct{}{}
	}
	if _, ok := g.cow.fieldValues[kind][field][value]; !ok {
		byField[value] = copyStringSet(byField[value])
		g.cow.fieldValues[kind][field][value] = struct{}{}
	}
	if byField[value] == nil {
		byField[value] = map[string]struct{}{}
	}
	return byField[value]
}
