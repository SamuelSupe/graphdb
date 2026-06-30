package httpapi

import (
	"net/http"
	"time"
)

func (s *Server) tenantUsage(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	now := time.Now().UTC()
	if report, ok := s.usageCache.fresh(tenantID, now); ok {
		writeJSON(w, http.StatusOK, report)
		return
	}
	report, err := s.Store.TenantUsage(r.Context(), tenantID)
	if err != nil {
		if report, ok := s.usageCache.stale(tenantID, now, err.Error()); ok {
			writeJSON(w, http.StatusOK, report)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.usageCache.put(tenantID, report, now)
	writeJSON(w, http.StatusOK, report)
}
