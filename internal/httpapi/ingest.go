package httpapi

import (
	"net/http"

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
	release, ok := s.enterWrite(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	var request storage.IngestRequest
	if !decodeJSONBody(w, r, &request, maxWriteRequestBytes) {
		return
	}
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
		s.invalidate(tenantID)
	}
	if result.Suppressed > 0 {
		s.obs().Metrics.RecordIngestSuppressed(tenantID, request.Source, result.Suppressed)
		s.recordIngestConflictResources(tenantID, result.Conflicts)
	}
	s.auditInfo("ingest_completed", tenantID, map[string]any{
		"source": request.Source, "collector_id": request.CollectorID, "batch_id": result.BatchID,
		"applied": result.Applied, "failed": result.Failed, "suppressed": result.Suppressed, "skipped": result.Skipped,
	})
	status := http.StatusOK
	if result.Failed > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, result)
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
