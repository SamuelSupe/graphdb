package storage

import "sort"

type edgeShardCatalog struct {
	specs     map[string]EdgeShard
	relations []string
}

func (l *PersistedIndexLookup) edgeCatalogForShard(shard string) edgeShardCatalog {
	l.edgeCatalogMu.Lock()
	defer l.edgeCatalogMu.Unlock()
	if catalog, ok := l.edgeCatalogByShard[shard]; ok {
		return catalog
	}

	catalog := buildEdgeShardCatalog(l.Catalog.EdgeShards, shard)
	if l.edgeCatalogByShard == nil {
		l.edgeCatalogByShard = make(map[string]edgeShardCatalog)
	}
	l.edgeCatalogByShard[shard] = catalog
	return catalog
}

func (l *PersistedIndexLookup) reverseCatalogForShard(shard string) edgeShardCatalog {
	l.reverseCatalogMu.Lock()
	defer l.reverseCatalogMu.Unlock()
	if catalog, ok := l.reverseCatalogByShard[shard]; ok {
		return catalog
	}
	var specs []EdgeShard
	if l.ReverseCatalog != nil {
		specs = l.ReverseCatalog.EdgeShards
	}
	catalog := buildEdgeShardCatalog(specs, shard)
	if l.reverseCatalogByShard == nil {
		l.reverseCatalogByShard = make(map[string]edgeShardCatalog)
	}
	l.reverseCatalogByShard[shard] = catalog
	return catalog
}

func buildEdgeShardCatalog(specs []EdgeShard, shard string) edgeShardCatalog {
	catalog := edgeShardCatalog{specs: make(map[string]EdgeShard)}
	for _, spec := range specs {
		if spec.Shard != shard {
			continue
		}
		if _, exists := catalog.specs[spec.RelationType]; !exists {
			catalog.specs[spec.RelationType] = spec
		}
	}
	catalog.relations = make([]string, 0, len(catalog.specs))
	for relation := range catalog.specs {
		catalog.relations = append(catalog.relations, relation)
	}
	sort.Strings(catalog.relations)
	return catalog
}

func (l *PersistedIndexLookup) catalogEdgeShardSpec(relationType string, shard string) (EdgeShard, bool) {
	spec, ok := l.edgeCatalogForShard(shard).specs[relationType]
	return spec, ok
}

func (l *PersistedIndexLookup) relationTypesForShard(shard string, allowed map[string]struct{}) []string {
	return filterAllowedRelations(l.edgeCatalogForShard(shard).relations, allowed)
}

func (l *PersistedIndexLookup) reverseEdgeShardSpec(relationType string, shard string) (EdgeShard, bool) {
	spec, ok := l.reverseCatalogForShard(shard).specs[relationType]
	return spec, ok
}

func (l *PersistedIndexLookup) reverseRelationTypesForShard(shard string, allowed map[string]struct{}) []string {
	return filterAllowedRelations(l.reverseCatalogForShard(shard).relations, allowed)
}

func filterAllowedRelations(relations []string, allowed map[string]struct{}) []string {
	if len(allowed) == 0 {
		return relations
	}
	filtered := make([]string, 0, min(len(relations), len(allowed)))
	for _, relation := range relations {
		if relationAllowedForLookup(relation, allowed) {
			filtered = append(filtered, relation)
		}
	}
	return filtered
}
