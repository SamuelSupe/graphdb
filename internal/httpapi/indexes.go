package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) indexCatalog(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	catalog, err := s.Store.GetIndexCatalog(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, catalog)
}

func (s *Server) indexDefinitions(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	definitions, err := s.Store.ListIndexDefinitions(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"indexes": definitions})
}

func (s *Server) createIndex(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "index create is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var definition storage.IndexDefinition
	if !decodeJSONBody(w, r, &definition, maxConfigRequestBytes) {
		return
	}
	result, err := s.Store.CreateIndex(r.Context(), tenantID, definition)
	if err != nil {
		s.auditError("index_create_failed", tenantID, err, map[string]any{"name": definition.Name, "kind": definition.Kind, "field": definition.Field})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("index_created", tenantID, map[string]any{"name": result.Definition.Name, "kind": result.Definition.Kind, "field": result.Definition.Field, "task_id": result.Task.ID})
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) dropIndex(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "index drop is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	name, err := escapedPathTail(r, "/v1/indexes/definitions/")
	if err != nil || name == "" {
		writeError(w, http.StatusBadRequest, "index path must be /v1/indexes/definitions/{name}")
		return
	}
	result, err := s.Store.DropIndex(r.Context(), tenantID, name)
	if err != nil {
		s.auditError("index_drop_failed", tenantID, err, map[string]any{"name": name})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("index_dropped", tenantID, map[string]any{"name": result.Definition.Name, "task_id": result.Task.ID})
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) rebuildIndexes(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "index rebuild is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	if r.URL.Query().Get("async") == "true" {
		task, err := s.Store.StartIndexRebuild(r.Context(), tenantID)
		if err != nil {
			s.auditError("index_rebuild_start_failed", tenantID, err, map[string]any{"async": true})
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.auditInfo("index_rebuild_started", tenantID, map[string]any{"task_id": task.ID, "async": true})
		writeJSON(w, http.StatusAccepted, task)
		return
	}
	if r.URL.Query().Get("format") != "" {
		writeError(w, http.StatusBadRequest, "index rebuild format is fixed to parquet")
		return
	}
	catalog, err := s.Store.RebuildIndexes(r.Context(), tenantID)
	if err != nil {
		s.auditError("index_rebuild_failed", tenantID, err, map[string]any{"async": false})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.cleanupIndexOrphansAfterRebuild(r, tenantID, catalog.Version, false)
	s.auditInfo("index_rebuild_completed", tenantID, map[string]any{"version": catalog.Version, "async": false})
	writeJSON(w, http.StatusOK, catalog)
}

func (s *Server) cleanupIndexOrphansAfterRebuild(r *http.Request, tenantID string, version int64, async bool) {
	report, err := s.Store.RunGC(r.Context(), tenantID, storage.GCOptions{KeepSnapshots: 2, CleanupIndexOrphans: true, SkipEntityRecordCleanup: true})
	if err != nil {
		s.auditError("index_rebuild_cleanup_failed", tenantID, err, map[string]any{"version": version, "async": async})
		return
	}
	fields := map[string]any{
		"version":              version,
		"async":                async,
		"index_cleanup":        report.IndexCleanupAttempt,
		"index_cleanup_skip":   report.IndexCleanupSkippedReason,
		"index_cleanup_error":  report.IndexCleanupError,
		"reader_watermark":     report.ReaderWatermarkVersion,
		"reader_watermark_num": report.ReaderWatermarkReaders,
	}
	if report.IndexCleanupError != "" {
		s.auditError("index_rebuild_cleanup_error", tenantID, errors.New(report.IndexCleanupError), fields)
		return
	}
	s.auditInfo("index_rebuild_cleanup_completed", tenantID, fields)
}

func (s *Server) indexHealth(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	deep := strings.EqualFold(r.URL.Query().Get("deep"), "true")
	health, err := s.Store.IndexHealthWithOptions(r.Context(), tenantID, storage.IndexHealthOptions{Deep: deep})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.obs().Metrics.RecordIndexHealth(tenantID, health.Status, len(health.Issues))
	s.auditInfo("index_health_checked", tenantID, map[string]any{
		"status": health.Status, "manifest_version": health.ManifestVersion, "catalog_version": health.CatalogVersion, "issues": len(health.Issues), "deep": deep,
	})
	writeJSON(w, http.StatusOK, health)
}

func (s *Server) indexTask(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	taskID := strings.TrimPrefix(r.URL.Path, "/v1/indexes/tasks/")
	if taskID == "" || strings.Contains(taskID, "/") {
		writeError(w, http.StatusBadRequest, "task path must be /v1/indexes/tasks/{id}")
		return
	}
	task, err := s.Store.GetIndexTask(r.Context(), tenantID, taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}
