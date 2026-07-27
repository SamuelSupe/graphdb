package storage

import (
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const (
	maxWriterMetadataCacheEntries = 4_096
	maxIndexCatalogCacheEntries   = 512
)

type cachedSourcePolicy struct {
	policy     graph.SourcePolicy
	configured bool
	meta       ObjectMeta
}

type cachedTenantConfig struct {
	config     TenantConfig
	configured bool
	meta       ObjectMeta
}

type cachedTenantMetadata struct {
	metadata   TenantMetadata
	configured bool
	meta       ObjectMeta
	checkedAt  time.Time
}

type cachedIndexCatalog struct {
	catalog     IndexCatalog
	meta        ObjectMeta
	contentHash string
	checkedAt   time.Time
}

func (s *TenantStore) getCachedSourcePolicy(tenantID string) (graph.SourcePolicy, bool, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.sourcePolicyCache[tenantID]
	if !ok {
		return graph.SourcePolicy{}, false, ObjectMeta{}, false
	}
	return copySourcePolicy(cached.policy), cached.configured, cached.meta, true
}

func (s *TenantStore) setCachedSourcePolicy(tenantID string, policy graph.SourcePolicy, configured bool, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	evictOneCacheEntry(s.sourcePolicyCache, tenantID, maxWriterMetadataCacheEntries)
	s.sourcePolicyCache[tenantID] = cachedSourcePolicy{policy: copySourcePolicy(policy), configured: configured, meta: meta}
}

func (s *TenantStore) deleteCachedSourcePolicy(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.sourcePolicyCache, tenantID)
}

func (s *TenantStore) getCachedTenantConfig(tenantID string) (TenantConfig, bool, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.tenantConfigCache[tenantID]
	if !ok {
		return TenantConfig{}, false, ObjectMeta{}, false
	}
	return cached.config, cached.configured, cached.meta, true
}

func (s *TenantStore) setCachedTenantConfig(tenantID string, config TenantConfig, configured bool, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	evictOneCacheEntry(s.tenantConfigCache, tenantID, maxWriterMetadataCacheEntries)
	s.tenantConfigCache[tenantID] = cachedTenantConfig{config: config, configured: configured, meta: meta}
}

func (s *TenantStore) deleteCachedTenantConfig(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.tenantConfigCache, tenantID)
}

func (s *TenantStore) getCachedTenantMetadata(tenantID string) (TenantMetadata, bool, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.tenantMetadataCache[tenantID]
	if !ok {
		return TenantMetadata{}, false, ObjectMeta{}, false
	}
	return cached.metadata, cached.configured, cached.meta, true
}

func (s *TenantStore) setCachedTenantMetadata(tenantID string, metadata TenantMetadata, configured bool, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	evictOneCacheEntry(s.tenantMetadataCache, tenantID, maxWriterMetadataCacheEntries)
	s.tenantMetadataCache[tenantID] = cachedTenantMetadata{metadata: metadata, configured: configured, meta: meta, checkedAt: time.Now().UTC()}
}

func (s *TenantStore) getCachedTenantMetadataFresh(tenantID string, now time.Time) (TenantMetadata, bool, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.tenantMetadataCache[tenantID]
	if !ok || now.Sub(cached.checkedAt) >= s.lifecycleCacheTTL() {
		return TenantMetadata{}, false, ObjectMeta{}, false
	}
	return cached.metadata, cached.configured, cached.meta, true
}

func (s *TenantStore) deleteCachedTenantMetadata(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.tenantMetadataCache, tenantID)
}

func copySourcePolicy(policy graph.SourcePolicy) graph.SourcePolicy {
	policy.Sources = append([]graph.SourcePolicyItem(nil), policy.Sources...)
	policy.FieldAliases = append([]graph.FieldAliasRule(nil), policy.FieldAliases...)
	for i := range policy.FieldAliases {
		policy.FieldAliases[i].Aliases = copyStringMap(policy.FieldAliases[i].Aliases)
	}
	policy.FieldPriorities = append([]graph.FieldPriorityRule(nil), policy.FieldPriorities...)
	for i := range policy.FieldPriorities {
		policy.FieldPriorities[i].Fields = copyIntMap(policy.FieldPriorities[i].Fields)
	}
	return policy
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyIntMap(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (s *TenantStore) getCachedIndexCatalog(tenantID string) (IndexCatalog, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.indexCatalogCache[tenantID]
	if !ok || !cached.meta.Exists {
		return IndexCatalog{}, ObjectMeta{}, false
	}
	return copyIndexCatalog(cached.catalog), cached.meta, true
}

func (s *TenantStore) setCachedIndexCatalog(tenantID string, catalog IndexCatalog, meta ObjectMeta) {
	contentHash := catalog.contentHash
	if contentHash == "" {
		contentHash, _ = indexCatalogContentHash(catalog)
	}
	s.setCachedIndexCatalogWithHash(tenantID, catalog, meta, contentHash)
}

func (s *TenantStore) setCachedIndexCatalogWithHash(tenantID string, catalog IndexCatalog, meta ObjectMeta, contentHash string) {
	catalog.contentHash = contentHash
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.setCachedIndexCatalogEntryLocked(tenantID, cachedIndexCatalog{
		catalog:     copyIndexCatalog(catalog),
		meta:        meta,
		contentHash: contentHash,
		checkedAt:   time.Now(),
	})
}

func (s *TenantStore) setCachedIndexCatalogEntryLocked(
	tenantID string,
	entry cachedIndexCatalog,
) {
	evictOneCacheEntry(s.indexCatalogCache, tenantID, maxIndexCatalogCacheEntries)
	s.indexCatalogCache[tenantID] = entry
}

func (s *TenantStore) getCachedIndexCatalogSnapshot(tenantID string, version int64, contentHash string) (IndexCatalog, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.indexCatalogCache[tenantID]
	if !ok || cached.catalog.Version != version || cached.contentHash == "" || cached.contentHash != contentHash {
		return IndexCatalog{}, false
	}
	return copyIndexCatalog(cached.catalog), true
}

func evictOneCacheEntry[T any](cache map[string]T, incoming string, max int) {
	if _, exists := cache[incoming]; exists || len(cache) < max {
		return
	}
	for key := range cache {
		delete(cache, key)
		return
	}
}

func (s *TenantStore) deleteCachedIndexCatalog(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.indexCatalogCache, tenantID)
	delete(s.reverseIndexCatalogCache, tenantID)
}

func copyIndexCatalog(catalog IndexCatalog) IndexCatalog {
	catalog.Indexes = append([]IndexSpec(nil), catalog.Indexes...)
	for i := range catalog.Indexes {
		catalog.Indexes[i].Objects = append([]IndexObject(nil), catalog.Indexes[i].Objects...)
		catalog.Indexes[i].TopValues = append([]IndexValueStat(nil), catalog.Indexes[i].TopValues...)
	}
	catalog.EdgeShards = append([]EdgeShard(nil), catalog.EdgeShards...)
	for i := range catalog.EdgeShards {
		catalog.EdgeShards[i].Objects = append([]IndexObject(nil), catalog.EdgeShards[i].Objects...)
	}
	catalog.EntityPages = append([]EntityPageSpec(nil), catalog.EntityPages...)
	for i := range catalog.EntityPages {
		catalog.EntityPages[i].Objects = append([]IndexObject(nil), catalog.EntityPages[i].Objects...)
	}
	return catalog
}
