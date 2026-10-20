package storage

import (
	"fmt"
	"sort"
	"strconv"
)

const maxCompiledScanCatalogEntries = 512
const maxCompiledScanCatalogBytes int64 = 64 << 20

type compiledScanCatalog struct {
	bytes        int64
	contentHash  string
	entityShards []string
	entitySpecs  map[string]EntityPageSpec
	edgeTargets  []edgeShardTarget
	edgeSpecs    map[string]EdgeShard
}

func (s *TenantStore) compiledScanCatalog(tenantID string, catalog IndexCatalog, expectedHash string) (*compiledScanCatalog, error) {
	contentHash := catalog.contentHash
	if contentHash == "" {
		var err error
		contentHash, err = indexCatalogContentHash(catalog)
		if err != nil {
			return nil, err
		}
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
		if hasIndexRanges(page.Objects) {
			if page.Codec != parquetEntityRangeCodec {
				return nil, fmt.Errorf("unsupported entity range layout")
			}
			if err := validateRangeObjects(page.Objects, page.EntityCount, page.ContentHash); err != nil {
				return nil, err
			}
		}
		compiled.bytes += 256 + int64(len(page.Shard)) + rangeReferenceBytes(page.Objects)
		compiled.entityShards = append(compiled.entityShards, page.Shard)
	}
	sort.Strings(compiled.entityShards)
	compiled.edgeTargets = make([]edgeShardTarget, 0, len(catalog.EdgeShards))
	for _, shard := range catalog.EdgeShards {
		if hasIndexRanges(shard.Objects) {
			if shard.Codec != parquetEdgeRangeCodec {
				return nil, fmt.Errorf("unsupported edge range layout")
			}
			if err := validateRangeObjects(shard.Objects, shard.EdgeCount, shard.ContentHash); err != nil {
				return nil, err
			}
		}
		compiled.bytes += 256 + int64(len(shard.Shard)+len(shard.RelationType)) + rangeReferenceBytes(shard.Objects)
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
	if compiled.bytes > maxCompiledScanCatalogBytes {
		s.lockMu.Unlock()
		return compiled, nil
	}
	var used int64
	for _, entry := range s.compiledScanCatalogCache {
		used += entry.bytes
	}
	for cacheKey, entry := range s.compiledScanCatalogCache {
		if len(s.compiledScanCatalogCache) < maxCompiledScanCatalogEntries && used+compiled.bytes <= maxCompiledScanCatalogBytes {
			break
		}
		delete(s.compiledScanCatalogCache, cacheKey)
		used -= entry.bytes
	}
	s.compiledScanCatalogCache[key] = compiled
	s.lockMu.Unlock()
	return compiled, nil
}

func rangeReferenceBytes(objects []IndexObject) int64 {
	var bytes int64
	for _, o := range objects {
		bytes += 256 + int64(len(o.Role)+len(o.Key)+len(o.Format)+len(o.Codec)+len(o.ContentHash)+len(o.SchemaHash))
	}
	return bytes
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
