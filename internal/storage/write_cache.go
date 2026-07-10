package storage

import (
	"context"
	"errors"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"go.opentelemetry.io/otel/attribute"
)

const (
	writeCacheLogicalWeight = int64(8)
	minimumWriteCacheBytes  = int64(4 * 1024)
)

func (s *TenantStore) loadForWriteLocked(ctx context.Context, tenantID string) (loaded loadedGraph, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.load_for_write", tenantTraceAttr(tenantID))
	defer func() {
		if err == nil {
			span.SetAttributes(manifestTraceAttrs("graphdb.manifest", loaded.Manifest)...)
		}
		endStorageSpan(span, err)
	}()
	if cached, ok := s.getWriteCache(tenantID); ok {
		span.SetAttributes(
			attribute.Bool("graphdb.write_cache.found", true),
			attribute.Bool("graphdb.write_cache.hit", true),
			attribute.Int64("graphdb.write_cache.version", cached.Manifest.Version),
			attribute.Int64("graphdb.write_cache.current_manifest_version", cached.Manifest.Version),
		)
		return cached, nil
	}
	span.SetAttributes(
		attribute.Bool("graphdb.write_cache.found", false),
		attribute.Bool("graphdb.write_cache.hit", false),
	)
	loaded, err = s.loadWithMeta(ctx, tenantID)
	if err == nil {
		span.SetAttributes(attribute.Int64("graphdb.write_cache.current_manifest_version", loaded.Manifest.Version))
	}
	return loaded, err
}

func (s *TenantStore) loadForExpectedVersionLocked(ctx context.Context, tenantID string, expectedVersion int64, manifest Manifest, meta ObjectMeta) (loadedGraph, error) {
	for attempt := 0; ; attempt++ {
		if cached, ok := s.getWriteCache(tenantID); ok && cachedManifestMatches(cached, manifest, meta) {
			return cached, nil
		}
		loaded, err := s.loadManifestGraph(ctx, tenantID, manifest, meta)
		if err == nil || !errors.Is(err, ErrNotFound) || attempt >= manifestObjectMissingReloads {
			return loaded, err
		}
		nextManifest, nextMeta, manifestErr := s.getManifest(ctx, tenantID)
		if manifestErr != nil {
			return loadedGraph{}, manifestErr
		}
		if nextManifest.Version != expectedVersion {
			return loadedGraph{}, fmt.Errorf("expected version %d, current version %d", expectedVersion, nextManifest.Version)
		}
		if sameManifestReadSet(manifest, nextManifest) {
			return loadedGraph{}, err
		}
		manifest, meta = nextManifest, nextMeta
	}
}

func cachedManifestMatches(cached loadedGraph, manifest Manifest, meta ObjectMeta) bool {
	if cached.Meta.Exists != meta.Exists || cached.Meta.ETag != meta.ETag {
		return false
	}
	return sameManifestReadSet(cached.Manifest, manifest)
}

func (s *TenantStore) getWriteCache(tenantID string) (loadedGraph, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.writeCache[tenantID]
	if ok {
		s.touchWriteCacheLocked(tenantID)
	}
	return cached, ok
}

func (s *TenantStore) setWriteCache(tenantID string, loaded loadedGraph) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	loaded.CacheBytes = normalizedWriteCacheBytes(loaded)
	if s.MaxWriteCacheTenants <= 0 || s.MaxWriteCacheBytes <= 0 || loaded.CacheBytes > s.MaxWriteCacheBytes {
		s.removeWriteCacheLocked(tenantID)
		return
	}
	if previous, ok := s.writeCache[tenantID]; ok {
		s.writeCacheBytes -= previous.CacheBytes
	}
	s.writeCache[tenantID] = loaded
	s.writeCacheBytes += loaded.CacheBytes
	s.touchWriteCacheLocked(tenantID)
	for len(s.writeCache) > s.MaxWriteCacheTenants || s.writeCacheBytes > s.MaxWriteCacheBytes {
		oldest := s.writeCacheOrder[0]
		s.removeWriteCacheLocked(oldest)
	}
}

func (s *TenantStore) deleteWriteCache(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.removeWriteCacheLocked(tenantID)
}

func (s *TenantStore) removeWriteCacheLocked(tenantID string) {
	if cached, ok := s.writeCache[tenantID]; ok {
		s.writeCacheBytes -= cached.CacheBytes
		if s.writeCacheBytes < 0 {
			s.writeCacheBytes = 0
		}
		delete(s.writeCache, tenantID)
	}
	s.removeWriteCacheOrderLocked(tenantID)
}

func (s *TenantStore) touchWriteCacheLocked(tenantID string) {
	s.removeWriteCacheOrderLocked(tenantID)
	s.writeCacheOrder = append(s.writeCacheOrder, tenantID)
}

func (s *TenantStore) removeWriteCacheOrderLocked(tenantID string) {
	for i, cachedTenantID := range s.writeCacheOrder {
		if cachedTenantID != tenantID {
			continue
		}
		copy(s.writeCacheOrder[i:], s.writeCacheOrder[i+1:])
		s.writeCacheOrder = s.writeCacheOrder[:len(s.writeCacheOrder)-1]
		return
	}
}

func cloneGraph(g *graph.Graph) (*graph.Graph, error) {
	return g.Clone(), nil
}

func writeCacheBytesFromLogicalSize(logicalBytes int64) int64 {
	if logicalBytes <= 0 {
		return minimumWriteCacheBytes
	}
	if logicalBytes > (1<<63-1)/writeCacheLogicalWeight {
		return 1<<63 - 1
	}
	weighted := logicalBytes * writeCacheLogicalWeight
	if weighted < minimumWriteCacheBytes {
		return minimumWriteCacheBytes
	}
	return weighted
}

func normalizedWriteCacheBytes(loaded loadedGraph) int64 {
	if loaded.CacheBytes > 0 {
		return loaded.CacheBytes
	}
	if loaded.Graph == nil {
		return minimumWriteCacheBytes
	}
	// Callers that did not just compute a logical hash use a conservative,
	// allocation-free fallback. Normal commits carry the measured logical size.
	weight := minimumWriteCacheBytes + int64(len(loaded.Graph.Entities))*1024 + int64(len(loaded.Graph.Edges))*768
	weight += int64(len(loaded.Graph.CITypes)+len(loaded.Graph.RelationTypes)) * 2048
	if weight < minimumWriteCacheBytes {
		return 1<<63 - 1
	}
	return weight
}
