package httpapi

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "ingest is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request storage.IngestRequest
	if !decodeJSONBody(w, r, &request, maxWriteRequestBytes) {
		return
	}
	if s.IngestService != nil {
		s.acceptWALIngest(w, r, tenantID, request)
		return
	}
	release, ok := s.enterWrite(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	writeCtx, cancel := s.writeExecutionContext(r.Context())
	defer cancel()
	result, err := s.Store.Ingest(writeCtx, tenantID, request)
	if err != nil {
		if s.writeBackpressureIfNeeded(w, tenantID, err) {
			return
		}
		if writeCtx.Err() != nil {
			if ingestMayHaveChangedData(result) {
				s.invalidate(tenantID)
			}
			s.auditError("ingest_timeout", tenantID, err, map[string]any{
				"source": request.Source, "collector_id": request.CollectorID, "batch_id": result.BatchID,
			})
			writeRequestError(w, writeCtx.Err())
			return
		}
		if ingestMayHaveChangedData(result) {
			s.invalidate(tenantID)
			s.auditError("ingest_metadata_failed", tenantID, err, map[string]any{
				"source": request.Source, "collector_id": request.CollectorID, "batch_id": result.BatchID,
			})
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.auditError("ingest_failed", tenantID, err, map[string]any{
			"source": request.Source, "collector_id": request.CollectorID, "batch_id": request.BatchID,
		})
		writeStorageError(w, err)
		return
	}
	if result.Applied > 0 && !result.Skipped {
		s.publishReadCacheAfterWrite(tenantID)
	}
	if result.Skipped {
		s.obs().Metrics.RecordIngestSkipped(tenantID, request.Source, result.SkipReason)
	}
	if result.Suppressed > 0 {
		s.obs().Metrics.RecordIngestSuppressed(tenantID, request.Source, result.Suppressed)
		s.recordIngestConflictResources(tenantID, result.Conflicts)
	}
	s.auditInfo("ingest_completed", tenantID, map[string]any{
		"source": request.Source, "collector_id": request.CollectorID, "batch_id": result.BatchID,
		"applied": result.Applied, "failed": result.Failed, "suppressed": result.Suppressed, "skipped": result.Skipped, "skip_reason": result.SkipReason,
	})
	writeIngestResult(w, result)
}

func (s *Server) acceptWALIngest(w http.ResponseWriter, r *http.Request, tenantID string, request storage.IngestRequest) {
	accepted, err := s.IngestService.Accept(r.Context(), tenantID, request)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrIngestQueueFull), errors.Is(err, storage.ErrIngestWALFull):
			s.writeBackpressure(w, tenantID, &storage.BackpressureError{
				Reasons: []storage.BackpressureReason{{
					Code:    "ingest_wal_queue_full",
					Message: "ingest WAL queue is full",
				}},
				RetryAfter: retryAfterFromStore(s.Store),
			})
		case errors.Is(err, storage.ErrIngestIdentityConflict):
			writeStorageError(w, err)
		default:
			writeStorageError(w, err)
		}
		return
	}
	statusPath := ingestBatchStatusPath(accepted.WriterID, accepted.Source, accepted.CollectorID, accepted.BatchID)
	w.Header().Set("Location", statusPath)
	if preferCommitted(r.Header.Get("Prefer")) {
		w.Header().Set("Preference-Applied", "wait=committed")
		result, waitErr := s.IngestService.Wait(r.Context(), accepted)
		if waitErr != nil {
			writeRequestError(w, waitErr)
			return
		}
		s.writeCompletedIngest(w, tenantID, request, result)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"writer_id":          accepted.WriterID,
		"batch_id":           accepted.BatchID,
		"source":             accepted.Source,
		"collector_id":       accepted.CollectorID,
		"state":              accepted.State,
		"durability":         accepted.Durability,
		"accepted_at":        accepted.AcceptedAt,
		"estimated_flush_at": accepted.EstimatedFlush,
		"status_url":         statusPath,
	})
}

func (s *Server) writeCompletedIngest(w http.ResponseWriter, tenantID string, request storage.IngestRequest, result storage.IngestResult) {
	if result.Applied > 0 && !result.Skipped {
		s.publishReadCacheAfterWrite(tenantID)
	}
	if result.Skipped {
		s.obs().Metrics.RecordIngestSkipped(tenantID, request.Source, result.SkipReason)
	}
	if result.Suppressed > 0 {
		s.obs().Metrics.RecordIngestSuppressed(tenantID, request.Source, result.Suppressed)
		s.recordIngestConflictResources(tenantID, result.Conflicts)
	}
	writeIngestResult(w, result)
}

func writeIngestResult(w http.ResponseWriter, result storage.IngestResult) {
	if result.Failed == 0 {
		writeJSON(w, http.StatusOK, result)
		return
	}
	status := http.StatusMultiStatus
	code := ErrorCode("")
	switch result.ErrorCode {
	case storage.IngestErrorVersionConflict:
		status = http.StatusConflict
		code = ErrorCodeVersionConflict
	case storage.IngestErrorPreconditionFailed:
		status = http.StatusPreconditionFailed
		code = ErrorCodePreconditionFailed
	case storage.IngestErrorAtomicValidation:
		status = http.StatusUnprocessableEntity
		code = ErrorCodeAtomicValidationFailed
	case storage.IngestErrorAtomicSuppressed:
		status = http.StatusConflict
		code = ErrorCodeAtomicSuppressed
	case storage.IngestErrorIdempotencyConflict:
		status = http.StatusConflict
		code = ErrorCodeIdempotencyConflict
	}
	if code == "" {
		writeJSON(w, status, result)
		return
	}
	message := "ingest request failed"
	if len(result.Conflicts) > 0 && result.Conflicts[len(result.Conflicts)-1].Message != "" {
		message = result.Conflicts[len(result.Conflicts)-1].Message
	} else if len(result.Failures) > 0 && result.Failures[0].Error != "" {
		message = result.Failures[0].Error
	}
	writeErrorDetail(w, status, code, message, false, result)
}

func (s *Server) ingestBatchStatus(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	prefix := "/v1/ingest/batches/"
	partCount := 3
	ownerID := ""
	if strings.HasPrefix(r.URL.EscapedPath(), "/v1/ingest/writers/") {
		prefix = "/v1/ingest/writers/"
		partCount = 4
	}
	parts, err := escapedPathParts(r, prefix, partCount)
	if err != nil || len(parts) != partCount {
		writeError(w, http.StatusBadRequest, "batch status path must include source, collector_id, and batch_id")
		return
	}
	if partCount == 4 {
		ownerID = parts[0]
		parts = parts[1:]
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" || (partCount == 4 && ownerID == "") {
		writeError(w, http.StatusBadRequest, "batch status path contains an empty identifier")
		return
	}
	if s.IngestService != nil {
		if ownerID != "" && ownerID != s.IngestService.WriterID() {
			writeError(w, http.StatusConflict, "batch status request was routed to a different writer")
			return
		}
		status, statusErr := s.IngestService.Status(r.Context(), tenantID, parts[0], parts[1], parts[2])
		if statusErr != nil {
			writeStorageError(w, statusErr)
			return
		}
		writeJSON(w, http.StatusOK, status)
		return
	}
	record, recordErr := s.Store.GetIngestBatch(r.Context(), tenantID, parts[0], parts[1], parts[2])
	if recordErr != nil {
		writeStorageError(w, recordErr)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func ingestBatchStatusPath(writerID string, source string, collectorID string, batchID string) string {
	prefix := "/v1/ingest/batches/"
	if writerID != "" {
		prefix = "/v1/ingest/writers/" + url.PathEscape(writerID) + "/"
	}
	return prefix + url.PathEscape(source) + "/" + url.PathEscape(collectorID) + "/" + url.PathEscape(batchID)
}

func preferCommitted(value string) bool {
	for _, preference := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(preference), "wait=committed") {
			return true
		}
	}
	return false
}

func (s *Server) recordIngestConflictResources(tenantID string, conflicts []storage.IngestConflict) {
	counts := map[string]int{}
	for _, conflict := range conflicts {
		resource := conflict.ResourceType
		if resource == "" {
			resource = "entity"
		}
		counts[resource]++
	}
	for resource, count := range counts {
		s.obs().Metrics.RecordSuppressed(tenantID, resource, count)
	}
}

func ingestMayHaveChangedData(result storage.IngestResult) bool {
	return result.BatchID != "" || result.Version > 0 || result.Applied > 0 || result.Failed > 0
}

func (s *Server) collectorStatus(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	parts, err := escapedPathParts(r, "/v1/ingest/collectors/", 2)
	if err != nil || len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "collector status path must be /v1/ingest/collectors/{source}/{collector_id}")
		return
	}
	status, err := s.Store.GetCollectorStatus(r.Context(), tenantID, parts[0], parts[1])
	if err != nil {
		writeStorageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}
