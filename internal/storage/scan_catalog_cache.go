package storage

import (
	"fmt"
	"sort"
	"strconv"
)

const maxCompiledScanCatalogEntries = 512

type compiledScanCatalog struct {
	contentHash  string
	entityShards []string
	entitySpecs  map[string]EntityPageSpec
	edgeTargets  []edgeShardTarget
	edgeSpecs    map[string]EdgeShard
}

func (s *TenantStore) compiledScanCatalog(tenantID string, catalog IndexCatalog, expectedHash string) (*compiledScanCatalog, error) {
	if expectedHash != "" {
		if compiled, ok := s.getCompiledScanCatalog(compiledScanCatalogKey(tenantID, catalog.Version, expectedHash)); ok {
			return compiled, nil
		}
	}
	contentHash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return nil, err
	}
	if expectedHash != "" && contentHash != expectedHash {
		return nil, fmt.Errorf("cursor index catalog content hash mismatch")
	}
	key := compiledScanCatalogKey(tenantID, catalog.Version, contentHash)
	if compiled, ok := s.getCompiledScanCatalog(key); ok {
		return compiled, nil
	}
	compiled := &compiledScanCatalog{
		contentHash: contentHash,
		entitySpecs: entityPageSpecMap(catalog),
		edgeSpecs:   edgeShardSpecMap(catalog),
	}
	compiled.entityShards = make([]string, 0, len(catalog.EntityPages))
	for _, page := range catalog.EntityPages {
		compiled.entityShards = append(compiled.entityShards, page.Shard)
	}
	sort.Strings(compiled.entityShards)
	compiled.edgeTargets = make([]edgeShardTarget, 0, len(catalog.EdgeShards))
	for _, shard := range catalog.EdgeShards {
		compiled.edgeTargets = append(compiled.edgeTargets, edgeShardTarget{RelationType: shard.RelationType, Shard: shard.Shard})
	}
	sort.Slice(compiled.edgeTargets, func(i, j int) bool {
		return edgeShardTargetKey(compiled.edgeTargets[i].RelationType, compiled.edgeTargets[i].Shard) <
			edgeShardTargetKey(compiled.edgeTargets[j].RelationType, compiled.edgeTargets[j].Shard)
	})
	s.lockMu.Lock()
	if cached := s.compiledScanCatalogCache[key]; cached != nil {
		s.lockMu.Unlock()
		return cached, nil
	}
	evictOneCacheEntry(s.compiledScanCatalogCache, key, maxCompiledScanCatalogEntries)
	s.compiledScanCatalogCache[key] = compiled
	s.lockMu.Unlock()
	return compiled, nil
}

func (s *TenantStore) getCompiledScanCatalog(key string) (*compiledScanCatalog, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	compiled, ok := s.compiledScanCatalogCache[key]
	return compiled, ok
}

func compiledScanCatalogKey(tenantID string, version int64, contentHash string) string {
	return tenantID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + contentHash
}

func (catalog *compiledScanCatalog) entityTargets(requested string) []string {
	if requested != "" {
		return []string{requested}
	}
	return catalog.entityShards
}

func (catalog *compiledScanCatalog) edgeScanTargets(options EdgeScanOptions) []edgeShardTarget {
	if options.Type == "" && options.FromShard == "" && options.From == "" {
		return catalog.edgeTargets
	}
	items := make([]edgeShardTarget, 0, len(catalog.edgeTargets))
	for _, target := range catalog.edgeTargets {
		if options.Type != "" && target.RelationType != options.Type {
			continue
		}
		if options.FromShard != "" && target.Shard != options.FromShard {
			continue
		}
		if options.From != "" && !indexShardIDMatches(options.From, target.Shard) {
			continue
		}
		items = append(items, target)
	}
	return items
}
