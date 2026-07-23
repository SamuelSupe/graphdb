package httpapi

import (
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) startImport(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "imports are disabled in reader mode")
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
	format, err := importFormat(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	batchSize, err := importBatchSize(r.URL.Query().Get("batch_size"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxWriteRequestBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)
		return
	}
	task, err := s.Store.StartImport(r.Context(), tenantID, data, storage.ImportOptions{
		Format: format, Source: r.URL.Query().Get("source"), CollectorID: r.URL.Query().Get("collector_id"),
		BatchSize: batchSize, OnError: r.URL.Query().Get("on_error"),
	})
	if err != nil {
		if s.writeBackpressureIfNeeded(w, tenantID, err) {
			return
		}
		s.auditError("import_start_failed", tenantID, err, map[string]any{"format": format})
		writeStorageError(w, err)
		return
	}
	s.auditInfo("import_started", tenantID, map[string]any{"task_id": task.ID, "format": format})
	w.Header().Set("Location", "/v1/tasks/"+task.ID)
	writeJSON(w, http.StatusAccepted, task)
}

func importFormat(r *http.Request) (string, error) {
	if format := strings.TrimSpace(r.URL.Query().Get("format")); format != "" {
		return format, nil
	}
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch strings.ToLower(mediaType) {
	case "application/x-ndjson", "application/ndjson", "application/jsonl":
		return "jsonl", nil
	case "text/csv", "application/csv":
		return "csv", nil
	default:
		return "", traceError("import format is required when Content-Type is not JSONL or CSV")
	}
}

func importBatchSize(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	batchSize, err := strconv.Atoi(value)
	if err != nil || batchSize < 1 {
		return 0, traceError("batch_size must be a positive integer")
	}
	return batchSize, nil
}
