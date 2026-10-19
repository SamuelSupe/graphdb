package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"slices"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Keep the read view for the entire response and encode one record at a time;
// neither a copied snapshot nor a tenant-sized encoder buffer is required.
func writeGraphSnapshotJSON(w http.ResponseWriter, ctx context.Context, tenant string, version int64, g *graph.Graph) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	var err error
	write := func(text string) {
		if err == nil {
			_, err = io.WriteString(w, text)
		}
	}
	encode := func(value any) {
		if err == nil {
			err = ctx.Err()
			if err == nil {
				err = encoder.Encode(value)
			}
		}
	}
	write(`{"tenant_id":`)
	encode(tenant)
	write(`,"version":`)
	encode(version)
	write(`,"snapshot":{"version":`)
	encode(g.Version)
	if len(g.CITypes) > 0 {
		write(`,"ci_types":`)
		encode(g.ListCITypes())
	}
	write(`,"relation_types":`)
	encode(g.ListRelationTypes())
	write(`,"entities":[`)
	first := true
	if err == nil {
		err = g.VisitEntitiesByID("", "", func(entity graph.Entity) (bool, error) {
			if !first {
				write(",")
			}
			first = false
			encode(entity.JSONValue())
			return err == nil, err
		})
	}
	write(`],"edges":[`)
	if err != nil {
		return err
	}
	for i, id := range slices.Sorted(g.Edges.Keys()) {
		if err != nil {
			break
		}
		if i > 0 {
			write(",")
		}
		encode(g.Edges.At(id))
	}
	write("]}}\n")
	return err
}

func (s *Server) withCachedScanGraph(ctx context.Context, tenantID string, minVersion int64, visit func(*graph.Graph, storage.Manifest, storage.ScanCursorBinding) error) (bool, error) {
	if s.Mode != "all" || s.Cache == nil {
		return false, nil
	}
	used := false
	_, err := s.Cache.WithCachedReadOnlyGraph(ctx, tenantID, minVersion, func(g *graph.Graph, manifest storage.Manifest) error {
		binding, err := s.Store.GetScanCursorBinding(ctx, tenantID, manifest.Version)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		// A returned cursor must remain readable through the persisted catalog
		// after this in-memory version has been replaced or evicted.
		used = true
		return visit(g, manifest, binding)
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
	for _, id := range slices.Sorted(g.Edges.Keys()) {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge": g.Edges.At(id)}, flush); err != nil {
			return
		}
	}
	_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": version}, flush)
}
