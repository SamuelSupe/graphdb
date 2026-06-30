package httpapi

import (
	"net/http"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type SourcePolicyResponse struct {
	Configured bool               `json:"configured"`
	Policy     graph.SourcePolicy `json:"policy"`
}

func (s *Server) sourcePolicy(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getSourcePolicy(w, r)
	case http.MethodPut:
		s.putSourcePolicy(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) getSourcePolicy(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	policy, configured, err := s.Store.GetSourcePolicy(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, SourcePolicyResponse{Configured: configured, Policy: policy})
}

func (s *Server) putSourcePolicy(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "source policy updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var policy graph.SourcePolicy
	if !decodeJSONBody(w, r, &policy, maxConfigRequestBytes) {
		return
	}
	policy, err := s.Store.PutSourcePolicy(r.Context(), tenantID, policy)
	if err != nil {
		s.auditError("source_policy_update_failed", tenantID, err, map[string]any{})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("source_policy_updated", tenantID, map[string]any{"sources": len(policy.Sources), "default_priority": policy.DefaultPriority})
	writeJSON(w, http.StatusOK, SourcePolicyResponse{Configured: true, Policy: policy})
}
