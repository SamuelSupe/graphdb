package httpapi

import (
	"net/http"
	"time"
)

func (s *Server) enterRead(w http.ResponseWriter, r *http.Request, tenantID string) (func(), bool) {
	if s.ReadAdmission == nil {
		return func() {}, true
	}
	start := time.Now()
	release, err := s.ReadAdmission.Acquire(r.Context(), tenantID)
	if err != nil {
		s.obs().Metrics.RecordReadAdmissionQueue(tenantID, "rejected", time.Since(start))
		writeErrorDetail(w, http.StatusTooManyRequests, ErrorCodeTooManyRequests, "read admission queue timeout", true, nil)
		return nil, false
	}
	s.obs().Metrics.RecordReadAdmissionQueue(tenantID, "ok", time.Since(start))
	return release, true
}
