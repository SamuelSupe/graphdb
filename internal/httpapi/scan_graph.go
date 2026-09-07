package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) withCachedScanGraph(ctx context.Context, tenantID string, minVersion int64, visit func(*graph.Graph, storage.Manifest, storage.IndexCatalog) error) (bool, error) {
	if s.Mode != "all" || s.Cache == nil {
		return false, nil
	}
	used := false
	_, err := s.Cache.WithCachedReadOnlyGraph(ctx, tenantID, minVersion, func(g *graph.Graph, manifest storage.Manifest) error {
		catalog, err := s.Store.GetIndexCatalogAtVersion(ctx, tenantID, manifest.Version)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		// A returned cursor must remain readable through the persisted catalog
		// after this in-memory version has been replaced or evicted.
		if catalog.Version != manifest.Version {
			return nil
		}
		used = true
		return visit(g, manifest, catalog)
	})
	return used, err
}

func (s *Server) streamGraphSnapshot(w http.ResponseWriter, r *http.Request, tenantID string, g *graph.Graph, version int64) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	if err := encodeStreamItem(r.Context(), encoder, map[string]any{
		"stream": "snapshot", "tenant_id": tenantID, "version": version,
	}, flush); err != nil {
		return
	}
	for _, name := range slices.Sorted(maps.Keys(g.CITypes)) {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"ci_type": g.CITypes[name]}, flush); err != nil {
			return
		}
	}
	for _, name := range slices.Sorted(maps.Keys(g.RelationTypes)) {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"relation_type": g.RelationTypes[name]}, flush); err != nil {
			return
		}
	}
	if err := g.VisitEntitiesByID("", "", func(entity graph.Entity) (bool, error) {
		err := encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity.JSONValue()}, flush)
		return err == nil, err
	}); err != nil {
		return
	}
	for _, id := range slices.Sorted(maps.Keys(g.Edges)) {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge": g.Edges[id]}, flush); err != nil {
			return
		}
	}
	_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": version}, flush)
}
