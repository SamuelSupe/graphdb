package httpapi

import (
	"net/http"

	"graphdb/internal/storage"
)

type TenantConfigResponse struct {
	Configured bool                 `json:"configured"`
	Config     storage.TenantConfig `json:"config"`
}

func (s *Server) tenantConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getTenantConfig(w, r)
	case http.MethodPut:
		s.putTenantConfig(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) getTenantConfig(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	config, configured, err := s.Store.GetTenantConfig(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, TenantConfigResponse{Configured: configured, Config: config})
}

func (s *Server) putTenantConfig(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant config updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var config storage.TenantConfig
	if !decodeJSONBody(w, r, &config, maxConfigRequestBytes) {
		return
	}
	config, err := s.Store.PutTenantConfig(r.Context(), tenantID, config)
	if err != nil {
		s.auditError("tenant_config_update_failed", tenantID, err, map[string]any{})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("tenant_config_updated", tenantID, map[string]any{})
	writeJSON(w, http.StatusOK, TenantConfigResponse{Configured: true, Config: config})
}
