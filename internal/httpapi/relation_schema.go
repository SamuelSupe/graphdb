package httpapi

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) relationSchemas(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	catalog, err := s.Store.GetRelationSchemas(r.Context(), tenantID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, catalog)
}

func (s *Server) relationSchema(w http.ResponseWriter, r *http.Request) {
	relationType, err := relationSchemaTypeFromPath(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.putRelationSchema(w, r, relationType)
	case http.MethodDelete:
		s.deleteRelationSchema(w, r, relationType)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) putRelationSchema(w http.ResponseWriter, r *http.Request, relationType string) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "relation schema updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var schema storage.RelationSchema
	if !decodeJSONBody(w, r, &schema, maxConfigRequestBytes) {
		return
	}
	if schema.RelationType != "" && strings.TrimSpace(schema.RelationType) != relationType {
		writeError(w, http.StatusBadRequest, "relation_type must match the request path")
		return
	}
	schema.RelationType = relationType
	catalog, err := s.Store.PutRelationSchema(r.Context(), tenantID, schema)
	if err != nil {
		s.auditError("relation_schema_update_failed", tenantID, err, map[string]any{"relation_type": relationType})
		writeStorageError(w, err)
		return
	}
	s.auditInfo("relation_schema_updated", tenantID, map[string]any{"relation_type": relationType, "revision": catalog.Revision})
	writeJSON(w, http.StatusOK, catalog)
}

func (s *Server) deleteRelationSchema(w http.ResponseWriter, r *http.Request, relationType string) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "relation schema updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	catalog, err := s.Store.DeleteRelationSchema(r.Context(), tenantID, relationType)
	if err != nil {
		s.auditError("relation_schema_delete_failed", tenantID, err, map[string]any{"relation_type": relationType})
		writeStorageError(w, err)
		return
	}
	s.auditInfo("relation_schema_deleted", tenantID, map[string]any{"relation_type": relationType, "revision": catalog.Revision})
	writeJSON(w, http.StatusOK, catalog)
}

func relationSchemaTypeFromPath(r *http.Request) (string, error) {
	const prefix = "/v1/relation-schemas/"
	raw := strings.TrimPrefix(r.URL.EscapedPath(), prefix)
	if raw == "" || strings.Contains(raw, "/") {
		return "", fmt.Errorf("relation type is required")
	}
	value, err := url.PathUnescape(raw)
	if err != nil {
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "/") {
		return "", fmt.Errorf("relation type is required")
	}
	return value, nil
}
