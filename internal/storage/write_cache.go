package storage

import (
	"context"

	"graphdb/internal/graph"
)

func (s *TenantStore) loadForWriteLocked(ctx context.Context, tenantID string) (loadedGraph, error) {
	if cached, ok := s.getWriteCache(tenantID); ok {
		manifest, _, err := s.getManifest(ctx, tenantID)
		if err != nil {
			return loadedGraph{}, err
		}
		if cached.Manifest.Version >= manifest.Version && sameManifestReadSet(cached.Manifest, manifest) {
			return cached, nil
		}
		s.deleteWriteCache(tenantID)
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
