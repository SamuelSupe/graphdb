package storage

import (
	"context"
	"errors"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"go.opentelemetry.io/otel/attribute"
)

const minimumWriteCacheBytes = int64(4 * 1024)
const writeCacheLogicalBytesMultiplier = int64(8)

func (s *TenantStore) loadForWriteLocked(ctx context.Context, tenantID string) (loaded loadedGraph, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.load_for_write", tenantTraceAttr(tenantID))
	defer func() {
		if err == nil {
			span.SetAttributes(manifestTraceAttrs("graphdb.manifest", loaded.Manifest)...)
		}
		endStorageSpan(span, err)
	}()
	if s.coordinated() {
		cached, cacheFound := s.getWriteCache(tenantID)
		var manifest Manifest
		var meta ObjectMeta
		var manifestErr error
		if cacheFound {
			head, exists, headErr := s.Coordinator.Head(ctx, tenantID)
			if headErr != nil {
				return loadedGraph{}, headErr
			}
			if exists && writeCacheMatchesCoordinatorHead(cached, head) {
				span.SetAttributes(
					attribute.Bool("graphdb.write_cache.found", true),
					attribute.Bool("graphdb.write_cache.hit", true),
					attribute.Int64("graphdb.write_cache.version", cached.Manifest.Version),
					attribute.Int64("graphdb.write_cache.current_manifest_version", head.GraphVersion),
				)
				return cached, nil
			}
			if exists {
				manifest, meta, manifestErr = s.getCoordinatedManifestAtHead(
					ctx, tenantID, head,
				)
			} else {
				manifest, meta, manifestErr = s.getManifest(ctx, tenantID)
			}
		} else {
			manifest, meta, manifestErr = s.getManifest(ctx, tenantID)
		}
		if manifestErr != nil {
			return loadedGraph{}, manifestErr
		}
		if cacheFound {
			caughtUp, applied, catchupErr := s.catchUpWriteCache(
				ctx, tenantID, cached, manifest, meta,
			)
			if catchupErr != nil {
				return loadedGraph{}, catchupErr
			}
			if applied {
				span.SetAttributes(
					attribute.Bool("graphdb.write_cache.found", true),
					attribute.Bool("graphdb.write_cache.hit", true),
					attribute.Bool("graphdb.write_cache.incremental_catchup", true),
					attribute.Int64("graphdb.write_cache.version", cached.Manifest.Version),
					attribute.Int64("graphdb.write_cache.current_manifest_version", manifest.Version),
				)
				s.setWriteCache(tenantID, caughtUp)
				return caughtUp, nil
			}
		}
		span.SetAttributes(
			attribute.Bool("graphdb.write_cache.found", cacheFound),
			attribute.Bool("graphdb.write_cache.hit", false),
			attribute.Int64("graphdb.write_cache.current_manifest_version", manifest.Version),
		)
		return s.loadManifestGraph(ctx, tenantID, manifest, meta)
	}
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
		previousRevision := coordinatedMetaRevision(previous.Meta)
		loadedRevision := coordinatedMetaRevision(loaded.Meta)
		if s.coordinated() && previousRevision > 0 && loadedRevision > 0 {
			if previousRevision > loadedRevision ||
				(previousRevision == loadedRevision && previous.Manifest.Version > loaded.Manifest.Version) {
				return
			}
		} else if previous.Manifest.Version > loaded.Manifest.Version {
			return
		}
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

func coordinatedMetaRevision(meta ObjectMeta) int64 {
	revision, err := parseCoordinatedRevision(meta)
	if err != nil {
		return 0
	}
	return revision
}

func (s *TenantStore) deleteWriteCache(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.removeWriteCacheLocked(tenantID)
}

func (s *TenantStore) handleManifestPublishFailureCache(
	tenantID string,
	loaded loadedGraph,
	publishErr error,
) {
	if s.coordinated() && errors.Is(publishErr, ErrConflict) {
		// The candidate never became authoritative, but its loaded base still
		// is. Keep that base so the retry can apply only the winning commits
		// instead of reconstructing the whole graph from object storage.
		s.setWriteCache(tenantID, loaded)
		return
	}
	s.deleteWriteCache(tenantID)
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

func normalizedWriteCacheBytes(loaded loadedGraph) int64 {
	if loaded.CacheBytes > 0 {
		return loaded.CacheBytes
	}
	if loaded.Graph == nil {
		return minimumWriteCacheBytes
	}
	_, logicalBytes, err := loaded.Graph.ContentMD5WithLogicalSize()
	if err != nil {
		return int64(^uint64(0) >> 1)
	}
	return writeCacheBytesForGraphWithCommitTail(
		loaded.Graph, logicalBytes, loaded.CommitTail,
	)
}

func writeCacheBytesFromLogicalSize(logicalBytes int64) int64 {
	maxInt64 := int64(^uint64(0) >> 1)
	if logicalBytes > maxInt64/writeCacheLogicalBytesMultiplier {
		return maxInt64
	}
	weight := logicalBytes * writeCacheLogicalBytesMultiplier
	if weight < minimumWriteCacheBytes {
		return minimumWriteCacheBytes
	}
	return weight
}

func writeCacheBytesForGraph(g *graph.Graph, logicalBytes int64) int64 {
	variableWeight := writeCacheBytesFromLogicalSize(logicalBytes)
	if g == nil {
		return variableWeight
	}
	maxInt64 := int64(^uint64(0) >> 1)
	structuralWeight := minimumWriteCacheBytes
	parts := []struct {
		count int
		bytes int64
	}{
		{len(g.Entities), 1024},
		{len(g.Edges), 768},
		{len(g.CITypes) + len(g.RelationTypes), 2048},
	}
	for _, part := range parts {
		if int64(part.count) > (maxInt64-structuralWeight)/part.bytes {
			return maxInt64
		}
		structuralWeight += int64(part.count) * part.bytes
	}
	if structuralWeight > variableWeight {
		return structuralWeight
	}
	return variableWeight
}

func writeCacheBytesForGraphWithCommitTail(
	g *graph.Graph,
	logicalBytes int64,
	tail commitTailCache,
) int64 {
	return addWriteCacheBytes(
		writeCacheBytesForGraph(g, logicalBytes), tail.bytes,
	)
}

func addWriteCacheBytes(left, right int64) int64 {
	maxInt64 := int64(^uint64(0) >> 1)
	if left < 0 || right < 0 || left > maxInt64-right {
		return maxInt64
	}
	return left + right
}

func writeCacheBytesWithoutCommitTail(loaded loadedGraph) int64 {
	if loaded.CacheBytes <= loaded.CommitTail.bytes {
		return 0
	}
	return loaded.CacheBytes - loaded.CommitTail.bytes
}
