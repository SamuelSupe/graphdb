package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) enterWrite(w http.ResponseWriter, r *http.Request, tenantID string) (func(), bool) {
	release, waited, err := s.WriteAdmission.Acquire(r.Context(), tenantID)
	if err != nil {
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
	s.obs().Metrics.RecordWriteAdmissionQueue(tenantID, "accepted", waited)
	if err := s.Store.CheckWriteBackpressure(r.Context(), tenantID); err != nil {
		release()
		if s.writeBackpressureIfNeeded(w, tenantID, err) {
			return nil, false
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return release, true
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
