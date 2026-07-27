package httpapi

import (
	"context"
	"net/http"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) tenantUsage(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	now := time.Now().UTC()
	report, err := s.cachedTenantUsage(r.Context(), tenantID, now)
	if err != nil {
		if report, ok := s.usageCache.stale(tenantID, now, err.Error()); ok {
			writeJSON(w, http.StatusOK, report)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) cachedTenantUsage(
	ctx context.Context,
	tenantID string,
	now time.Time,
) (storage.TenantUsageReport, error) {
	return s.usageCache.getOrLoad(ctx, tenantID, now, s.Store.TenantUsage)
}
