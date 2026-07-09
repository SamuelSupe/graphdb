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
