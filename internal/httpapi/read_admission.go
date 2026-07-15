package httpapi

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func (s *Server) enterRead(w http.ResponseWriter, r *http.Request, tenantID string) (func(), bool) {
	ctx, span := startAPIPhase(r.Context(), "read_admission.acquire", attribute.String("graphdb.tenant", tenantID))
	r = r.WithContext(ctx)
	if s.ReadAdmission == nil {
		if span != nil {
			span.SetAttributes(attribute.String("graphdb.read_admission.result", "disabled"))
		}
		endHTTPSpan(span, nil)
		return func() {}, true
	}
	start := time.Now()
	release, err := s.ReadAdmission.Acquire(r.Context(), tenantID)
	waited := time.Since(start)
	if err != nil {
		if span != nil {
			span.SetAttributes(
				attribute.String("graphdb.read_admission.result", "rejected"),
				attribute.Int64("graphdb.read_admission.wait_ms", waited.Milliseconds()),
			)
		}
		endHTTPSpan(span, err)
		s.obs().Metrics.RecordReadAdmissionQueue(tenantID, "rejected", waited)
		writeErrorDetail(w, http.StatusTooManyRequests, ErrorCodeTooManyRequests, "read admission queue timeout", true, nil)
		return nil, false
	}
	if span != nil {
		span.SetAttributes(
			attribute.String("graphdb.read_admission.result", "accepted"),
			attribute.Int64("graphdb.read_admission.wait_ms", waited.Milliseconds()),
		)
	}
	endHTTPSpan(span, nil)
	s.obs().Metrics.RecordReadAdmissionQueue(tenantID, "ok", waited)
	return release, true
}
