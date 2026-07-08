package storage

import (
	"context"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
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
			attribute.Int64("graphdb.write_cache.version", cached.Manifest.Version),
		)
		manifest, _, err := s.getManifest(ctx, tenantID)
		if err != nil {
			return loadedGraph{}, err
		}
		if cached.Manifest.Version >= manifest.Version && sameManifestReadSet(cached.Manifest, manifest) {
			span.SetAttributes(attribute.Bool("graphdb.write_cache.hit", true))
			return cached, nil
		}
		span.SetAttributes(
			attribute.Bool("graphdb.write_cache.hit", false),
			attribute.Int64("graphdb.write_cache.current_manifest_version", manifest.Version),
		)
		s.deleteWriteCache(tenantID)
	} else {
		span.SetAttributes(attribute.Bool("graphdb.write_cache.found", false))
	}
	return s.loadWithMeta(ctx, tenantID)
}

func (s *TenantStore) getWriteCache(tenantID string) (loadedGraph, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.writeCache[tenantID]
	return cached, ok
}

func (s *TenantStore) setWriteCache(tenantID string, loaded loadedGraph) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.writeCache[tenantID] = loaded
}

func (s *TenantStore) deleteWriteCache(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.writeCache, tenantID)
}

func cloneGraph(g *graph.Graph) (*graph.Graph, error) {
	return g.Clone(), nil
}
