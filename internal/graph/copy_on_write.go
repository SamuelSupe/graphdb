package graph

import "maps"

// copyOnWriteState tracks index buckets shared with the source graph by a
// storage-only mutation copy. Public graph copies continue to be fully deep.
type copyOnWriteState struct {
	outNodes         map[string]struct{}
	inNodes          map[string]struct{}
	edgeAliases      map[string]struct{}
	edgeTypes        map[string]struct{}
	entityAliases    map[string]struct{}
	identityKinds    map[string]struct{}
	fieldKinds       map[string]struct{}
	kindEntityCopies map[string]struct{}
	fieldNames       map[string]map[string]struct{}
	fieldValues      map[string]map[string]map[string]struct{}
}

type storageMutationImpact struct {
	ciTypes       bool
	entities      bool
	relationTypes bool
	edges         bool
}

func storageMutationImpactFor(mutations Mutations) storageMutationImpact {
	entityChanges := len(mutations.UpsertEntities) > 0 ||
		len(mutations.DeleteEntities) > 0 ||
		len(mutations.DeleteEntityRequests) > 0 ||
		len(mutations.MarkSourceStale) > 0 ||
		len(mutations.MergeEntities) > 0 ||
		len(mutations.SplitEntities) > 0
	edgeChanges := len(mutations.UpsertEdges) > 0 ||
		len(mutations.DeleteEdges) > 0 ||
		len(mutations.DeleteEdgeRequests) > 0 ||
		len(mutations.DeleteRelationTypes) > 0 ||
		len(mutations.DeleteEntities) > 0 ||
		len(mutations.DeleteEntityRequests) > 0 ||
		len(mutations.MarkSourceStale) > 0 ||
		len(mutations.MergeEntities) > 0 ||
		len(mutations.SplitEntities) > 0
	return storageMutationImpact{
		ciTypes: len(mutations.UpsertCITypes) > 0 ||
			len(mutations.DeleteCITypes) > 0,
		entities: entityChanges,
		relationTypes: len(mutations.UpsertRelationTypes) > 0 ||
			len(mutations.DeleteRelationTypes) > 0,
		edges: edgeChanges,
	}
}

func (g *Graph) cloneForStorageMutation(mutations Mutations) *Graph {
	return g.cloneForStorageImpact(storageMutationImpactFor(mutations))
}

func (g *Graph) cloneForStorageImpact(impact storageMutationImpact) *Graph {
	fingerprint, fingerprintReady := g.contentFingerprintState()
	logicalHashCache := g.shareLogicalHashCache()
	clone := &Graph{
		Version:                 g.Version,
		CITypes:                 storageMutationMap(g.CITypes, impact.ciTypes),
		Entities:                g.Entities.Clone(),
		RelationTypes:           storageMutationMap(g.RelationTypes, impact.relationTypes),
		Edges:                   g.Edges.Clone(),
		out:                     g.out.Clone(),
		in:                      g.in.Clone(),
		edgeAliasIndex:          g.edgeAliasIndex.Clone(),
		edgeTypeIndex:           g.edgeTypeIndex.Clone(),
		entityAliasIndex:        g.entityAliasIndex.Clone(),
		kindCounts:              storageMutationMap(g.kindCounts, impact.entities),
		kindEntityIDs:           g.kindEntityIDs.Clone(),
		nonScalarFields:         g.nonScalarFields.Clone(),
		fieldIndex:              storageMutationMap(g.fieldIndex, impact.entities),
		identityIndex:           storageMutationMap(g.identityIndex, impact.entities || impact.ciTypes),
		legacyIdentity:          g.legacyIdentity,
		contentFingerprint:      fingerprint,
		contentFingerprintReady: fingerprintReady,
		logicalHashCache:        logicalHashCache,
		entityPartitions:        g.inheritedEntityPartitions(),
		cow: &copyOnWriteState{
			outNodes:      map[string]struct{}{},
			inNodes:       map[string]struct{}{},
			edgeAliases:   map[string]struct{}{},
			edgeTypes:     map[string]struct{}{},
			entityAliases: map[string]struct{}{},
			identityKinds: map[string]struct{}{},
			fieldKinds:    map[string]struct{}{},
			fieldNames:    map[string]map[string]struct{}{},
			fieldValues:   map[string]map[string]map[string]struct{}{},
		},
	}
	clone.inheritReadOrder(g)
	return clone
}

func storageMutationMap[K comparable, V any](source map[K]V, writable bool) map[K]V {
	if !writable {
		return source
	}
	return shallowCopyMap(source)
}

func shallowCopyMap[K comparable, V any](source map[K]V) map[K]V {
	if source == nil {
		return map[K]V{}
	}
	return maps.Clone(source)
}

func copyStringSet(source map[string]struct{}) map[string]struct{} {
	return shallowCopyMap(source)
}

func (g *Graph) writableOut(node string) *ShardedMap[struct{}] {
	if g.cow == nil {
		return writableIndexSet(g.out, node, nil)
	}
	return writableIndexSet(g.out, node, g.cow.outNodes)
}

func (g *Graph) writableIn(node string) *ShardedMap[struct{}] {
	if g.cow == nil {
		return writableIndexSet(g.in, node, nil)
	}
	return writableIndexSet(g.in, node, g.cow.inNodes)
}

func (g *Graph) writableEntityAlias(alias string) *ShardedMap[struct{}] {
	if g.cow == nil {
		return writableIndexSet(g.entityAliasIndex, alias, nil)
	}
	return writableIndexSet(g.entityAliasIndex, alias, g.cow.entityAliases)
}

func (g *Graph) writableEdgeAlias(alias string) *ShardedMap[struct{}] {
	if g.cow == nil {
		return writableIndexSet(g.edgeAliasIndex, alias, nil)
	}
	return writableIndexSet(g.edgeAliasIndex, alias, g.cow.edgeAliases)
}

func (g *Graph) writableEdgeType(edgeType string) *ShardedMap[struct{}] {
	if g.cow == nil {
		return writableIndexSet(g.edgeTypeIndex, edgeType, nil)
	}
	return writableIndexSet(g.edgeTypeIndex, edgeType, g.cow.edgeTypes)
}

func writableIndexSet(index *ShardedMap[*ShardedMap[struct{}]], key string, copied map[string]struct{}) *ShardedMap[struct{}] {
	ids := index.At(key)
	_, owned := copied[key]
	if ids == nil || (copied != nil && !owned) {
		ids = ids.Clone()
		index.Set(key, ids)
		if copied != nil {
			copied[key] = struct{}{}
		}
	}
	return ids
}

func (g *Graph) writableIdentityKind(kind string) *ShardedMap[string] {
	identities := g.identityIndex[kind]
	if g.cow == nil {
		if identities == nil {
			identities = NewShardedMap[string]()
			g.identityIndex[kind] = identities
		}
	} else if _, ok := g.cow.identityKinds[kind]; !ok {
		identities = identities.Clone()
		g.identityIndex[kind] = identities
		g.cow.identityKinds[kind] = struct{}{}
	}
	return identities
}

func (g *Graph) writableFieldKind(kind string) map[string]*fieldValueIndex {
	if g.cow == nil {
		if g.fieldIndex[kind] == nil {
			g.fieldIndex[kind] = map[string]*fieldValueIndex{}
		}
		return g.fieldIndex[kind]
	}
	if _, ok := g.cow.fieldKinds[kind]; !ok {
		g.fieldIndex[kind] = shallowCopyMap(g.fieldIndex[kind])
		g.cow.fieldKinds[kind] = struct{}{}
	}
	if g.fieldIndex[kind] == nil {
		g.fieldIndex[kind] = map[string]*fieldValueIndex{}
	}
	return g.fieldIndex[kind]
}

func (g *Graph) writableFieldName(kind, field string) *fieldValueIndex {
	byKind := g.writableFieldKind(kind)
	if g.cow == nil {
		if byKind[field] == nil {
			byKind[field] = NewShardedMap[*ShardedMap[struct{}]]()
		}
		return byKind[field]
	}
	if g.cow.fieldNames[kind] == nil {
		g.cow.fieldNames[kind] = map[string]struct{}{}
	}
	if _, ok := g.cow.fieldNames[kind][field]; !ok {
		byKind[field] = byKind[field].Clone()
		g.cow.fieldNames[kind][field] = struct{}{}
	}
	if byKind[field] == nil {
		byKind[field] = NewShardedMap[*ShardedMap[struct{}]]()
	}
	return byKind[field]
}

func (g *Graph) writableFieldValue(kind, field, value string) *ShardedMap[struct{}] {
	byField := g.writableFieldName(kind, field)
	if g.cow == nil {
		if byField.At(value) == nil {
			byField.Set(value, NewShardedMap[struct{}]())
		}
		return byField.At(value)
	}
	if g.cow.fieldValues[kind] == nil {
		g.cow.fieldValues[kind] = map[string]map[string]struct{}{}
	}
	if g.cow.fieldValues[kind][field] == nil {
		g.cow.fieldValues[kind][field] = map[string]struct{}{}
	}
	if _, ok := g.cow.fieldValues[kind][field][value]; !ok {
		byField.Set(value, byField.At(value).Clone())
		g.cow.fieldValues[kind][field][value] = struct{}{}
	}
	if byField.At(value) == nil {
		byField.Set(value, NewShardedMap[struct{}]())
	}
	return byField.At(value)
}
