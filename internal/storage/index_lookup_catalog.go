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

	catalog := edgeShardCatalog{specs: make(map[string]EdgeShard)}
	for _, spec := range l.Catalog.EdgeShards {
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
	if l.edgeCatalogByShard == nil {
		l.edgeCatalogByShard = make(map[string]edgeShardCatalog)
	}
	l.edgeCatalogByShard[shard] = catalog
	return catalog
}

func (l *PersistedIndexLookup) catalogEdgeShardSpec(relationType string, shard string) (EdgeShard, bool) {
	spec, ok := l.edgeCatalogForShard(shard).specs[relationType]
	return spec, ok
}

func (l *PersistedIndexLookup) relationTypesForShard(shard string, allowed map[string]struct{}) []string {
	relations := l.edgeCatalogForShard(shard).relations
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
