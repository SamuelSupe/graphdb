package graph

import (
	"slices"
	"sort"
)

func (g *Graph) rebuildIndexes() {
	g.invalidateEntityOrder()
	g.invalidateFieldIndexOrder()
	g.cow = nil
	g.out = map[string]map[string]struct{}{}
	g.in = map[string]map[string]struct{}{}
	g.edgeAliasIndex = map[string]map[string]struct{}{}
	g.edgeTypeIndex = map[string]map[string]struct{}{}
	g.entityAliasIndex = map[string]map[string]struct{}{}
	g.kindCounts = map[string]int{}
	g.fieldIndex = map[string]map[string]map[string]map[string]struct{}{}
	g.identityIndex = map[string]map[string]string{}

	for id, entity := range g.Entities {
		g.kindCounts[entity.Kind]++
		g.addEntityAliasesToIndex(id, entity)
		if g.identityIndex[entity.Kind] == nil {
			g.identityIndex[entity.Kind] = map[string]string{}
		}
		for _, signature := range g.identitySignatures(entity) {
			g.identityIndex[entity.Kind][signature.Value] = id
		}
		for field, value := range entity.Fields {
			key, ok := scalarKey(value)
			if !ok {
				continue
			}
			byKind := g.fieldIndex[entity.Kind]
			if byKind == nil {
				byKind = map[string]map[string]map[string]struct{}{}
				g.fieldIndex[entity.Kind] = byKind
			}
			byField := byKind[field]
			if byField == nil {
				byField = map[string]map[string]struct{}{}
				byKind[field] = byField
			}
			ids := byField[key]
			if ids == nil {
				ids = map[string]struct{}{}
				byField[key] = ids
			}
			ids[id] = struct{}{}
		}
	}

	for id, edge := range g.Edges {
		g.addEdgeToIndexes(id, edge)
	}
}

func (g *Graph) removeEntityFromIndexes(id string, entity Entity) {
	g.invalidateEntityKindOrder(entity.Kind)
	if count := g.kindCounts[entity.Kind]; count <= 1 {
		delete(g.kindCounts, entity.Kind)
	} else {
		g.kindCounts[entity.Kind] = count - 1
	}
	g.removeEntityAliasesFromIndex(id, entity)
	for _, signature := range g.identitySignatures(entity) {
		if identities := g.identityIndex[entity.Kind]; identities != nil && identities[signature.Value] == id {
			delete(g.writableIdentityKind(entity.Kind), signature.Value)
		}
	}
	for field, value := range entity.Fields {
		if key, ok := scalarKey(value); ok {
			g.removeEntityFieldIndex(id, entity.Kind, field, key)
		}
	}
}

func (g *Graph) removeEntityAliasesFromIndex(id string, entity Entity) {
	for _, alias := range entityAliasValues(entity) {
		entityIDs := g.entityAliasIndex[alias]
		if entityIDs == nil {
			continue
		}
		entityIDs = g.writableEntityAlias(alias)
		delete(entityIDs, id)
		if len(entityIDs) == 0 {
			delete(g.entityAliasIndex, alias)
		}
	}
}

func (g *Graph) addEntityToIndexes(id string, entity Entity) {
	g.invalidateEntityKindOrder(entity.Kind)
	g.kindCounts[entity.Kind]++
	g.addEntityAliasesToIndex(id, entity)
	for _, signature := range g.identitySignatures(entity) {
		g.writableIdentityKind(entity.Kind)[signature.Value] = id
	}
	for field, value := range entity.Fields {
		key, ok := scalarKey(value)
		if !ok {
			continue
		}
		g.addEntityFieldIndex(id, entity.Kind, field, key)
	}
}

func (g *Graph) updateEntityIndexes(id string, before, after Entity) {
	if before.Kind != after.Kind {
		g.removeEntityFromIndexes(id, before)
		g.addEntityToIndexes(id, after)
		return
	}
	if !slices.Equal(before.MergedFrom, after.MergedFrom) {
		g.removeEntityAliasesFromIndex(id, before)
		g.addEntityAliasesToIndex(id, after)
	}
	oldIdentities, newIdentities := g.identitySignatures(before), g.identitySignatures(after)
	if !slices.Equal(oldIdentities, newIdentities) {
		identities := g.writableIdentityKind(after.Kind)
		for _, signature := range oldIdentities {
			if identities[signature.Value] == id {
				delete(identities, signature.Value)
			}
		}
		for _, signature := range newIdentities {
			identities[signature.Value] = id
		}
	}
	for field, value := range before.Fields {
		key, indexed := scalarKey(value)
		nextValue, exists := after.Fields[field]
		nextKey, nextIndexed := scalarKey(nextValue)
		if indexed && (!exists || !nextIndexed || key != nextKey) {
			g.removeEntityFieldIndex(id, before.Kind, field, key)
		}
	}
	for field, value := range after.Fields {
		key, indexed := scalarKey(value)
		previousValue, exists := before.Fields[field]
		previousKey, previousIndexed := scalarKey(previousValue)
		if indexed && (!exists || !previousIndexed || key != previousKey) {
			g.addEntityFieldIndex(id, after.Kind, field, key)
		}
	}
}

func (g *Graph) removeEntityFieldIndex(id, kind, field, key string) {
	if g.fieldIndex[kind][field][key] == nil {
		return
	}
	g.invalidateFieldValueOrder(kind, field, key)
	ids := g.writableFieldValue(kind, field, key)
	delete(ids, id)
	if len(ids) == 0 {
		delete(g.writableFieldName(kind, field), key)
		g.invalidateFieldKeyOrder(kind, field)
	}
}

func (g *Graph) addEntityFieldIndex(id, kind, field, key string) {
	g.invalidateFieldValueOrder(kind, field, key)
	if len(g.fieldIndex[kind][field][key]) == 0 {
		g.invalidateFieldKeyOrder(kind, field)
	}
	g.writableFieldValue(kind, field, key)[id] = struct{}{}
}

func (g *Graph) addEntityAliasesToIndex(id string, entity Entity) {
	for _, alias := range entityAliasValues(entity) {
		g.writableEntityAlias(alias)[id] = struct{}{}
	}
}

func entityAliasValues(entity Entity) []string {
	aliases := make([]string, 0, len(entity.MergedFrom))
	for _, alias := range entity.MergedFrom {
		if alias != "" {
			aliases = append(aliases, alias)
		}
	}
	return aliases
}

func (g *Graph) removeEdgeFromIndexes(id string, edge Edge) {
	if edges := g.out[edge.From]; edges != nil {
		edges = g.writableOut(edge.From)
		delete(edges, id)
		if len(edges) == 0 {
			delete(g.out, edge.From)
		}
	}
	if edges := g.in[edge.To]; edges != nil {
		edges = g.writableIn(edge.To)
		delete(edges, id)
		if len(edges) == 0 {
			delete(g.in, edge.To)
		}
	}
	for _, alias := range edgeAliasValues(edge) {
		edgeIDs := g.edgeAliasIndex[alias]
		if edgeIDs == nil {
			continue
		}
		edgeIDs = g.writableEdgeAlias(alias)
		delete(edgeIDs, id)
		if len(edgeIDs) == 0 {
			delete(g.edgeAliasIndex, alias)
		}
	}
	if edgeIDs := g.edgeTypeIndex[edge.Type]; edgeIDs != nil {
		edgeIDs = g.writableEdgeType(edge.Type)
		delete(edgeIDs, id)
		if len(edgeIDs) == 0 {
			delete(g.edgeTypeIndex, edge.Type)
		}
	}
}

func (g *Graph) addEdgeToIndexes(id string, edge Edge) {
	g.writableOut(edge.From)[id] = struct{}{}
	g.writableIn(edge.To)[id] = struct{}{}
	g.addEdgeAliasesToIndex(id, edge)
	g.writableEdgeType(edge.Type)[id] = struct{}{}
}

func (g *Graph) addEdgeAliasesToIndex(id string, edge Edge) {
	for _, alias := range edgeAliasValues(edge) {
		g.writableEdgeAlias(alias)[id] = struct{}{}
	}
}

func edgeAliasValues(edge Edge) []string {
	aliases := make([]string, 0, 1+len(edge.Sources)*2)
	if edge.ExternalID != "" {
		aliases = append(aliases, edge.ExternalID)
	}
	for _, source := range edge.Sources {
		if source.EdgeID != "" {
			aliases = append(aliases, source.EdgeID)
		}
		if source.ExternalID != "" {
			aliases = append(aliases, source.ExternalID)
		}
	}
	return aliases
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
