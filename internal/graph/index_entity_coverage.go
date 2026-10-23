package graph

// ScalarIndexCoversField uses counts maintained beside field postings; planning
// must not scan the graph before admission, cancellation and query budgets.
func (g *Graph) ScalarIndexCoversField(kind, field string) bool {
	return g.Entities.Len() > 0 && g.nonScalarFields != nil && g.nonScalarFields.At(kind+"\x00"+field) == 0
}
func (g *Graph) adjustNonScalarField(kind, field string, delta int) {
	for _, scope := range []string{kind, ""} {
		key := scope + "\x00" + field
		n := g.nonScalarFields.At(key) + delta
		if n <= 0 {
			g.nonScalarFields.Delete(key)
		} else {
			g.nonScalarFields.Set(key, n)
		}
		if kind == "" {
			break
		}
	}
}
func (g *Graph) writableKindEntityIDs(kind string) *ShardedMap[struct{}] {
	ids := g.kindEntityIDs.At(kind)
	if ids == nil {
		ids = NewShardedMap[struct{}]()
		g.kindEntityIDs.Set(kind, ids)
	} else if g.cow != nil {
		if g.cow.kindEntityCopies == nil {
			g.cow.kindEntityCopies = make(map[string]struct{})
		}
		if _, copied := g.cow.kindEntityCopies[kind]; !copied {
			ids = ids.Clone()
			g.kindEntityIDs.Set(kind, ids)
		}
	}
	if g.cow != nil {
		if g.cow.kindEntityCopies == nil {
			g.cow.kindEntityCopies = make(map[string]struct{})
		}
		g.cow.kindEntityCopies[kind] = struct{}{}
	}
	return ids
}
func cloneKindEntityIDs(source *ShardedMap[*ShardedMap[struct{}]]) *ShardedMap[*ShardedMap[struct{}]] {
	result := NewShardedMap[*ShardedMap[struct{}]]()
	for kind, ids := range source.All() {
		result.Set(kind, ids.Clone())
	}
	return result
}
