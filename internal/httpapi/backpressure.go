package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) enterWrite(w http.ResponseWriter, r *http.Request, tenantID string) (func(), bool) {
	enterCtx, enterSpan := startAPIPhase(r.Context(), "enter_write", attribute.String("graphdb.tenant", tenantID))
	r = r.WithContext(enterCtx)
	var enterErr error
	defer func() { endHTTPSpan(enterSpan, enterErr) }()

	tracer := otel.Tracer("graphdb/http")
	prefix := apiTracePrefix(r.Context(), "graphdb.commit")
	acquireCtx, acquireSpan := tracer.Start(r.Context(), prefix+".write_admission.acquire", trace.WithAttributes(attribute.String("graphdb.tenant", tenantID)))
	release, waited, err := s.WriteAdmission.Acquire(acquireCtx, tenantID)
	acquireSpan.SetAttributes(attribute.Int64("graphdb.write_admission.wait_ms", waited.Milliseconds()))
	if err != nil {
		enterErr = err
		acquireSpan.SetAttributes(attribute.String("graphdb.write_admission.result", "rejected"))
		endHTTPSpan(acquireSpan, err)
		s.obs().Metrics.RecordWriteAdmissionQueue(tenantID, "rejected", waited)
		s.writeBackpressure(w, tenantID, &storage.BackpressureError{
			Reasons: []storage.BackpressureReason{{
				Code:    "write_admission_queue_timeout",
				Message: "write admission queue timeout",
			}},
			RetryAfter: retryAfterFromStore(s.Store),
		})
		return nil, false
	}
	acquireSpan.SetAttributes(attribute.String("graphdb.write_admission.result", "accepted"))
	endHTTPSpan(acquireSpan, nil)
	s.obs().Metrics.RecordWriteAdmissionQueue(tenantID, "accepted", waited)

	checkCtx, checkSpan := tracer.Start(r.Context(), prefix+".check_backpressure", trace.WithAttributes(
		attribute.String("graphdb.tenant", tenantID),
		attribute.Int64("graphdb.write_admission.wait_ms", waited.Milliseconds()),
	))
	if err := s.Store.CheckWriteBackpressure(checkCtx, tenantID); err != nil {
		enterErr = err
		endHTTPSpan(checkSpan, err)
		release()
		if s.writeBackpressureIfNeeded(w, tenantID, err) {
			return nil, false
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	endHTTPSpan(checkSpan, nil)
	return release, true
}

func (s *Server) enterMaintenance(w http.ResponseWriter, tenantID string) (func(), bool) {
	release, err := s.Store.TryAcquireMaintenance(tenantID)
	if err == nil {
		return release, true
	}
	if errors.Is(err, storage.ErrMaintenanceBusy) {
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds(retryAfterFromStore(s.Store)), 10))
	}
	writeStorageError(w, err)
	return nil, false
}

func (s *Server) writeBackpressureIfNeeded(w http.ResponseWriter, tenantID string, err error) bool {
	var pressure *storage.BackpressureError
	if !errors.As(err, &pressure) {
		return false
	}
	s.writeBackpressure(w, tenantID, pressure)
	return true
}

func (s *Server) writeBackpressure(w http.ResponseWriter, tenantID string, pressure *storage.BackpressureError) {
	retryAfter := pressure.RetryAfter
	if retryAfter <= 0 {
		retryAfter = retryAfterFromStore(s.Store)
	}
	code, retryable := backpressureErrorContract(pressure.Reasons)
	for _, reason := range pressure.Reasons {
		s.obs().Metrics.RecordWriteBackpressure(tenantID, reason.Code)
	}
	s.auditInfo("write_backpressure", tenantID, map[string]any{
		"retry_after_ms": retryAfter.Milliseconds(),
		"reasons":        pressure.Reasons,
	})
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds(retryAfter), 10))
	writeJSON(w, http.StatusTooManyRequests, map[string]any{
		"error":          "write backpressure",
		"code":           code,
		"message":        "write backpressure",
		"retryable":      retryable,
		"retry_after_ms": retryAfter.Milliseconds(),
		"reasons":        pressure.Reasons,
		"detail": map[string]any{
			"retry_after_ms": retryAfter.Milliseconds(),
			"reasons":        pressure.Reasons,
		},
	})
}

func backpressureErrorContract(reasons []storage.BackpressureReason) (ErrorCode, bool) {
	for _, reason := range reasons {
		switch reason.Code {
		case "tenant_entity_quota_exceeded", "tenant_edge_quota_exceeded":
			return ErrorCodeQuotaExceeded, false
		case "object_store_latency_high", "object_store_errors_high", "object_store_unavailable":
			return ErrorCodeObjectStoreUnavailable, true
		case "manifest_cas_conflicts_high":
			return ErrorCodeManifestCASConflict, true
		case "index_rebuild_running":
			return ErrorCodeIndexRebuildRunning, true
		case "gc_running":
			return ErrorCodeMaintenanceTaskRunning, true
		case "commit_tail_too_long":
			return ErrorCodeCommitTailTooLong, false
		case "tenant_object_count_high", "tenant_bytes_high":
			return ErrorCodeWriteBackpressure, true
		case "write_admission_queue_timeout":
			return ErrorCodeWriteAdmissionTimeout, true
		}
	}
	return ErrorCodeWriteBackpressure, true
}

func retryAfterFromStore(store *storage.TenantStore) time.Duration {
	if store != nil && store.Backpressure != nil {
		if retry := store.Backpressure.Config().RetryAfter; retry > 0 {
			return retry
		}
	}
	return 2 * time.Second
}

func retryAfterSeconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 1
	}
	return int64((duration + time.Second - 1) / time.Second)
}
