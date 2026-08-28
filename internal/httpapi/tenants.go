package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel/attribute"
)

type tenantRequest struct {
	TenantID     string                `json:"tenant_id,omitempty"`
	Name         string                `json:"name,omitempty"`
	Description  string                `json:"description,omitempty"`
	Labels       map[string]string     `json:"labels,omitempty"`
	Metadata     map[string]any        `json:"metadata,omitempty"`
	Config       *storage.TenantConfig `json:"config,omitempty"`
	SourcePolicy *graph.SourcePolicy   `json:"source_policy,omitempty"`
}

type tenantCloneRequest struct {
	TargetTenantID string            `json:"target_tenant_id"`
	Name           string            `json:"name,omitempty"`
	Description    string            `json:"description,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Metadata       map[string]any    `json:"metadata,omitempty"`
}

type tenantRestoreRequest struct {
	BackupKey string `json:"backup_key"`
	Overwrite bool   `json:"overwrite,omitempty"`
	DryRun    bool   `json:"dry_run,omitempty"`
}

type tenantRestoreDrillRequest struct {
	BackupKey      string   `json:"backup_key,omitempty"`
	TargetTenantID string   `json:"target_tenant_id,omitempty"`
	TargetPrefix   string   `json:"target_prefix,omitempty"`
	Cleanup        *bool    `json:"cleanup,omitempty"`
	DryRun         bool     `json:"dry_run,omitempty"`
	QueryTemplates []string `json:"query_templates,omitempty"`
	QueryTimeoutMS int64    `json:"query_timeout_ms,omitempty"`
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	var (
		tenants []storage.TenantInfo
		err     error
	)
	if r.URL.Query().Get("include_legacy") == "true" {
		tenants, err = s.Store.ListTenantInfosIncludingLegacy(r.Context())
	} else {
		tenants, err = s.Store.ListManagedTenantInfos(r.Context())
	}
	if err != nil {
		writeErrorErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant create is disabled in reader mode")
		return
	}
	var request tenantRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	if request.TenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}
	info, err := s.Store.CreateTenant(r.Context(), request.TenantID, storage.TenantCreateOptions{
		Name: request.Name, Description: request.Description, Labels: request.Labels, Metadata: request.Metadata, Config: request.Config, SourcePolicy: request.SourcePolicy,
	})
	if err != nil {
		s.auditError("tenant_create_failed", request.TenantID, err, map[string]any{})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_created", request.TenantID, map[string]any{"status": info.Status})
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) tenantInfo(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 1, "tenant path must be /v1/tenants/{tenant-id}")
	if !ok {
		return
	}
	info, err := s.Store.GetTenantInfo(r.Context(), tenantID)
	if err != nil {
		writeTenantLifecycleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) updateTenantMetadata(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant metadata updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 1, "tenant path must be /v1/tenants/{tenant-id}")
	if !ok {
		return
	}
	var request tenantRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	info, err := s.Store.UpdateTenantMetadata(r.Context(), tenantID, storage.TenantCreateOptions{
		Name: request.Name, Description: request.Description, Labels: request.Labels, Metadata: request.Metadata,
	})
	if err != nil {
		s.auditError("tenant_metadata_update_failed", tenantID, err, map[string]any{})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_metadata_updated", tenantID, map[string]any{"status": info.Status})
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) setTenantLifecycleStatus(w http.ResponseWriter, r *http.Request, status string) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant status updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 2, "tenant status path must be /v1/tenants/{tenant-id}/{action}")
	if !ok {
		return
	}
	info, err := s.Store.SetTenantStatus(r.Context(), tenantID, status)
	if err != nil {
		s.auditError("tenant_status_update_failed", tenantID, err, map[string]any{"status": status})
		writeTenantLifecycleError(w, err)
		return
	}
	s.invalidate(tenantID)
	s.auditInfo("tenant_status_updated", tenantID, map[string]any{"status": info.Status})
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) softDeleteTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant delete is disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 1, "tenant path must be /v1/tenants/{tenant-id}")
	if !ok {
		return
	}
	info, err := s.Store.SetTenantStatus(r.Context(), tenantID, storage.TenantStatusDeleted)
	if err != nil {
		s.auditError("tenant_status_update_failed", tenantID, err, map[string]any{"status": storage.TenantStatusDeleted})
		writeTenantLifecycleError(w, err)
		return
	}
	s.invalidate(tenantID)
	s.auditInfo("tenant_status_updated", tenantID, map[string]any{"status": info.Status})
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) purgeTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant purge is disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 2, "tenant purge path must be /v1/tenants/{tenant-id}/purge")
	if !ok {
		return
	}
	force := r.URL.Query().Get("force") == "true"
	report, err := s.Store.PurgeTenant(r.Context(), tenantID, force)
	if err != nil {
		s.auditError("tenant_purge_failed", tenantID, err, map[string]any{"force": force})
		writeTenantLifecycleError(w, err)
		return
	}
	s.invalidate(tenantID)
	s.auditInfo("tenant_purged", tenantID, map[string]any{"deleted": report.Deleted, "force": force})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) cloneTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant clone is disabled in reader mode")
		return
	}
	sourceTenantID, ok := tenantIDFromLifecyclePath(w, r, 2, "tenant clone path must be /v1/tenants/{tenant-id}/clone")
	if !ok {
		return
	}
	var request tenantCloneRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	info, err := s.Store.CloneTenant(r.Context(), sourceTenantID, storage.TenantCloneOptions{
		TargetTenantID: request.TargetTenantID,
		Name:           request.Name,
		Description:    request.Description,
		Labels:         request.Labels,
		Metadata:       request.Metadata,
	})
	if err != nil {
		s.auditError("tenant_clone_failed", sourceTenantID, err, map[string]any{"target_tenant": request.TargetTenantID})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_cloned", sourceTenantID, map[string]any{"target_tenant": info.TenantID, "version": info.ManifestVersion})
	writeJSON(w, http.StatusAccepted, info)
}

func (s *Server) backupTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant backup is disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 2, "tenant backup path must be /v1/tenants/{tenant-id}/backup")
	if !ok {
		return
	}
	task, err := s.Store.StartTask(r.Context(), tenantID, storage.TaskTypeTenantBackup, nil)
	if err != nil {
		s.auditError("tenant_backup_start_failed", tenantID, err, map[string]any{})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_backup_started", tenantID, map[string]any{"task_id": task.ID})
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) restoreTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant restore is disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 2, "tenant restore path must be /v1/tenants/{tenant-id}/restore")
	if !ok {
		return
	}
	var request tenantRestoreRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	task, err := s.Store.StartTask(r.Context(), tenantID, storage.TaskTypeTenantRestore, map[string]any{
		"backup_key": request.BackupKey,
		"overwrite":  request.Overwrite,
		"dry_run":    request.DryRun,
	})
	if err != nil {
		s.auditError("tenant_restore_start_failed", tenantID, err, map[string]any{"backup_key": request.BackupKey})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_restore_started", tenantID, map[string]any{"task_id": task.ID, "backup_key": request.BackupKey})
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) restoreDrillTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant restore drill is disabled in reader mode")
		return
	}
	tenantID, ok := tenantIDFromLifecyclePath(w, r, 2, "tenant restore drill path must be /v1/tenants/{tenant-id}/restore-drill")
	if !ok {
		return
	}
	var request tenantRestoreDrillRequest
	if r.ContentLength != 0 {
		if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
			return
		}
	}
	params := map[string]any{
		"backup_key":       request.BackupKey,
		"target_tenant_id": request.TargetTenantID,
		"target_prefix":    request.TargetPrefix,
		"dry_run":          request.DryRun,
		"query_templates":  request.QueryTemplates,
		"query_timeout_ms": request.QueryTimeoutMS,
	}
	if request.Cleanup != nil {
		params["cleanup"] = *request.Cleanup
	}
	task, err := s.Store.StartTask(r.Context(), tenantID, storage.TaskTypeTenantRestoreDrill, params)
	if err != nil {
		s.auditError("tenant_restore_drill_start_failed", tenantID, err, map[string]any{"backup_key": request.BackupKey, "target_tenant": request.TargetTenantID})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_restore_drill_started", tenantID, map[string]any{"task_id": task.ID, "backup_key": request.BackupKey, "target_tenant": request.TargetTenantID})
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) tenantLifecycle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v1/tenants" && r.Method == http.MethodGet:
		s.listTenants(w, r)
	case r.URL.Path == "/v1/tenants" && r.Method == http.MethodPost:
		s.createTenant(w, r)
	case strings.HasSuffix(r.URL.Path, "/disable") && r.Method == http.MethodPost:
		s.setTenantLifecycleStatus(w, r, storage.TenantStatusDisabled)
	case strings.HasSuffix(r.URL.Path, "/enable") && r.Method == http.MethodPost:
		s.setTenantLifecycleStatus(w, r, storage.TenantStatusActive)
	case strings.HasSuffix(r.URL.Path, "/purge") && r.Method == http.MethodPost:
		s.purgeTenant(w, r)
	case strings.HasSuffix(r.URL.Path, "/clone") && r.Method == http.MethodPost:
		s.cloneTenant(w, r)
	case strings.HasSuffix(r.URL.Path, "/backup") && r.Method == http.MethodPost:
		s.backupTenant(w, r)
	case strings.HasSuffix(r.URL.Path, "/restore") && r.Method == http.MethodPost:
		s.restoreTenant(w, r)
	case strings.HasSuffix(r.URL.Path, "/restore-drill") && r.Method == http.MethodPost:
		s.restoreDrillTenant(w, r)
	case r.Method == http.MethodGet:
		s.tenantInfo(w, r)
	case r.Method == http.MethodPut:
		s.updateTenantMetadata(w, r)
	case r.Method == http.MethodDelete:
		s.softDeleteTenant(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) tenantLifecycleGate(mutation bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := startAPIPhase(r.Context(), "tenant_lifecycle_gate")
		if s.Store == nil {
			if span != nil {
				span.SetAttributes(attribute.String("graphdb.tenant_gate.result", "bypassed"))
			}
			endHTTPSpan(span, nil)
			next.ServeHTTP(w, r)
			return
		}
		tenantID := r.Header.Get("X-Tenant-ID")
		if tenantID == "" {
			if span != nil {
				span.SetAttributes(attribute.String("graphdb.tenant_gate.result", "tenant_missing"))
			}
			endHTTPSpan(span, nil)
			next.ServeHTTP(w, r)
			return
		}
		setAPITraceTenant(ctx, tenantID)
		status, err := s.Store.TenantStatus(ctx, tenantID)
		if err != nil {
			if errors.Is(err, storage.ErrCoordinatorUnavailable) && !mutation {
				if span != nil {
					span.SetAttributes(
						attribute.String("graphdb.tenant", tenantID),
						attribute.String("graphdb.tenant_gate.result", "coordinator_unavailable_read_fallback"),
					)
				}
				endHTTPSpan(span, nil)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			endHTTPSpan(span, err)
			writeTenantLifecycleError(w, err)
			return
		}
		if span != nil {
			span.SetAttributes(
				attribute.String("graphdb.tenant", tenantID),
				attribute.String("graphdb.tenant.status", status),
			)
		}
		if status == storage.TenantStatusDeleted {
			endHTTPSpan(span, storage.ErrTenantDeleted)
			writeErrorErr(w, http.StatusGone, storage.ErrTenantDeleted)
			return
		}
		if status == storage.TenantStatusDisabled && mutation {
			endHTTPSpan(span, storage.ErrTenantDisabled)
			writeErrorErr(w, http.StatusForbidden, storage.ErrTenantDisabled)
			return
		}
		if span != nil {
			span.SetAttributes(attribute.String("graphdb.tenant_gate.result", "allowed"))
		}
		endHTTPSpan(span, nil)
		next.ServeHTTP(w, r)
	})
}

func tenantIDFromLifecyclePath(w http.ResponseWriter, r *http.Request, count int, message string) (string, bool) {
	_, span := startAPIPhase(r.Context(), "resolve_tenant")
	parts, err := escapedPathParts(r, "/v1/tenants/", count)
	if err != nil || len(parts) != count || parts[0] == "" {
		if err == nil {
			err = traceError(message)
		}
		endHTTPSpan(span, err)
		writeError(w, http.StatusBadRequest, message)
		return "", false
	}
	if err := storage.ValidateTenantID(parts[0]); err != nil {
		endHTTPSpan(span, err)
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	setAPITraceTenant(r.Context(), parts[0])
	if span != nil {
		span.SetAttributes(
			attribute.String("graphdb.tenant", parts[0]),
			attribute.String("graphdb.tenant.resolve_result", "ok"),
		)
	}
	endHTTPSpan(span, nil)
	return parts[0], true
}

func writeTenantLifecycleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrCoordinatorUnavailable),
		errors.Is(err, storage.ErrWriteConflict),
		errors.Is(err, storage.ErrVersionConflict),
		errors.Is(err, storage.ErrIdempotencyInProgress):
		writeStorageError(w, err)
	case errors.Is(err, storage.ErrNotFound):
		writeErrorErr(w, http.StatusNotFound, err)
	case errors.Is(err, storage.ErrTenantDisabled):
		writeErrorErr(w, http.StatusForbidden, err)
	case errors.Is(err, storage.ErrTenantDeleted):
		writeErrorErr(w, http.StatusGone, err)
	default:
		writeErrorErr(w, http.StatusBadRequest, err)
	}
}
