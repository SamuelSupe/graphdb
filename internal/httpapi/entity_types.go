package httpapi

import (
	"net/http"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) entityTypes(w http.ResponseWriter, r *http.Request) {
	s.listEntityTypes(w, r, "entity_types")
}

func (s *Server) listEntityTypes(w http.ResponseWriter, r *http.Request, responseField string) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	var items []graph.EntityType
	var version int64
	err = s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		items = g.ListCITypes()
		version = manifest.Version
		return nil
	})
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, responseField: items})
}
