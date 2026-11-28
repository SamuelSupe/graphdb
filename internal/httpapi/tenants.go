package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
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
	BackupKey      string `json:"backup_key"`
	SourceTenantID string `json:"source_tenant_id,omitempty"`
	Overwrite      bool   `json:"overwrite,omitempty"`
	DryRun         bool   `json:"dry_run,omitempty"`
}

type tenantRestoreDrillRequest struct {
	BackupKey      string   `json:"backup_key,omitempty"`
	SourceTenantID string   `json:"source_tenant_id,omitempty"`
	TargetTenantID string   `json:"target_tenant_id,omitempty"`
	TargetPrefix   string   `json:"target_prefix,omitempty"`
	Cleanup        *bool    `json:"cleanup,omitempty"`
	DryRun         bool     `json:"dry_run,omitempty"`
	QueryTemplates []string `json:"query_templates,omitempty"`
	QueryTimeoutMS int64    `json:"query_timeout_ms,omitempty"`
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	options, err := storage.ParseTenantListOptions(r.URL.Query())
	if err != nil {
		writeErrorErr(w, http.StatusBadRequest, err)
		return
	}
	scope := r.Header.Get("X-Tenant-ID")
	if scope == "" && s.RequireTenantScope && storage.EnforceTenantScope(r.Context()) && !isAdminAuthRequest(r.Context()) {
		writeError(w, http.StatusForbidden, "tenant scope header X-Tenant-ID is required")
		return
	}
	if scope != "" && storage.EnforceTenantScope(r.Context()) {
		if err := storage.ValidateTenantID(scope); err != nil {
			writeErrorErr(w, http.StatusBadRequest, err)
			return
		}
		var tenants []storage.TenantInfo
		info, err := s.Store.GetTenantInfo(r.Context(), scope)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			writeTenantLifecycleError(w, err)
			return
		}
		if err == nil {
			tenants = []storage.TenantInfo{info}
		}
		if options.Limit > 0 {
			ids := []string{}
			if len(tenants) == 1 {
				ids = []string{scope}
			}
			pageIDs, total, next := storage.PageTenantIDs(ids, options)
			items := make([]storage.TenantInfo, 0, len(pageIDs))
			if len(pageIDs) == 1 && len(tenants) == 1 {
				items = append(items, tenants[0])
			}
			writeJSON(w, http.StatusOK, storage.TenantInfoPage{Tenants: items, Total: total, NextCursor: next})
			return
		}
		if tenants == nil {
			tenants = []storage.TenantInfo{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
		return
	}
	if options.Limit > 0 {
		page, err := s.Store.ListTenantInfoPage(r.Context(), options)
		if err != nil {
			writeErrorErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	var (
		tenants []storage.TenantInfo
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
	if !s.tenantScopeAllows(w, r, request.TenantID) {
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 1, "tenant path must be /v1/tenants/{tenant-id}")
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 1, "tenant path must be /v1/tenants/{tenant-id}")
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "tenant status path must be /v1/tenants/{tenant-id}/{action}")
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 1, "tenant path must be /v1/tenants/{tenant-id}")
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "tenant purge path must be /v1/tenants/{tenant-id}/purge")
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
	s.obs().UnregisterTenant(tenantID)
	s.auditInfo("tenant_purged", tenantID, map[string]any{"deleted": report.Deleted, "force": force})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) cloneTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant clone is disabled in reader mode")
		return
	}
	sourceTenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "tenant clone path must be /v1/tenants/{tenant-id}/clone")
	if !ok {
		return
	}
	var request tenantCloneRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	if !s.tenantScopeAllows(w, r, request.TargetTenantID) {
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "tenant backup path must be /v1/tenants/{tenant-id}/backup")
	if !ok {
		return
	}
	var request struct {
		Destination string `json:"destination,omitempty"`
	}
	if !decodeOptionalJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	task, err := s.Store.StartTask(r.Context(), tenantID, storage.TaskTypeTenantBackup, map[string]any{"destination": request.Destination})
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
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "tenant restore path must be /v1/tenants/{tenant-id}/restore")
	if !ok {
		return
	}
	var request tenantRestoreRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	params := map[string]any{
		"backup_key":             request.BackupKey,
		"overwrite":              request.Overwrite,
		"dry_run":                request.DryRun,
		"required_source_tenant": request.SourceTenantID,
	}
	if scope := r.Header.Get("X-Tenant-ID"); scope != "" && storage.EnforceTenantScope(r.Context()) {
		if request.SourceTenantID != "" && request.SourceTenantID != scope {
			writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "backup source is outside the authenticated tenant scope", false, nil)
			return
		}
		params["required_source_tenant"] = scope
	}
	task, err := s.Store.StartTask(r.Context(), tenantID, storage.TaskTypeTenantRestore, params)
	if err != nil {
		s.auditError("tenant_restore_start_failed", tenantID, err, map[string]any{"backup_key": request.BackupKey})
		writeTenantLifecycleError(w, err)
		return
	}
	s.auditInfo("tenant_restore_started", tenantID, map[string]any{"task_id": task.ID, "backup_key": request.BackupKey})
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) listTenantObjectBackups(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "backup list path must be /v1/tenants/{tenant-id}/backups")
	if !ok {
		return
	}
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		var err error
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
	}
	page, err := s.Store.ListObjectBackups(r.Context(), tenantID, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeTenantLifecycleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) restoreDrillTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tenant restore drill is disabled in reader mode")
		return
	}
	tenantID, ok := s.tenantIDFromLifecyclePath(w, r, 2, "tenant restore drill path must be /v1/tenants/{tenant-id}/restore-drill")
	if !ok {
		return
	}
	var request tenantRestoreDrillRequest
	if !decodeOptionalJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	params := map[string]any{
		"backup_key":             request.BackupKey,
		"target_tenant_id":       request.TargetTenantID,
		"target_prefix":          request.TargetPrefix,
		"dry_run":                request.DryRun,
		"query_templates":        request.QueryTemplates,
		"query_timeout_ms":       request.QueryTimeoutMS,
		"required_source_tenant": request.SourceTenantID,
	}
	if request.Cleanup != nil {
		params["cleanup"] = *request.Cleanup
	}
	if r.Header.Get("X-Tenant-ID") != "" && storage.EnforceTenantScope(r.Context()) {
		if request.SourceTenantID != "" && request.SourceTenantID != tenantID {
			writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "backup source is outside the authenticated tenant scope", false, nil)
			return
		}
		if request.TargetTenantID != "" || request.TargetPrefix != "" {
			writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "tenant-scoped restore drills require an automatically isolated target", false, nil)
			return
		}
		params["required_source_tenant"] = tenantID
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
	if r.URL.Path == "/v1/tenants" {
		switch r.Method {
		case http.MethodGet:
			s.listTenants(w, r)
		case http.MethodPost:
			s.createTenant(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/v1/tenants/")
	parts := strings.Split(escaped, "/")
	if len(parts) == 2 && parts[0] != "" {
		switch {
		case parts[1] == "disable" && r.Method == http.MethodPost:
			s.setTenantLifecycleStatus(w, r, storage.TenantStatusDisabled)
			return
		case parts[1] == "enable" && r.Method == http.MethodPost:
			s.setTenantLifecycleStatus(w, r, storage.TenantStatusActive)
			return
		case parts[1] == "purge" && r.Method == http.MethodPost:
			s.purgeTenant(w, r)
			return
		case parts[1] == "clone" && r.Method == http.MethodPost:
			s.cloneTenant(w, r)
			return
		case parts[1] == "backup" && r.Method == http.MethodPost:
			s.backupTenant(w, r)
			return
		case parts[1] == "backups" && r.Method == http.MethodGet:
			s.listTenantObjectBackups(w, r)
			return
		case parts[1] == "restore" && r.Method == http.MethodPost:
			s.restoreTenant(w, r)
			return
		case parts[1] == "restore-drill" && r.Method == http.MethodPost:
			s.restoreDrillTenant(w, r)
			return
		}
	}
	switch r.Method {
	case http.MethodGet:
		s.tenantInfo(w, r)
	case http.MethodPut:
		s.updateTenantMetadata(w, r)
	case http.MethodDelete:
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

func (s *Server) tenantIDFromLifecyclePath(w http.ResponseWriter, r *http.Request, count int, message string) (string, bool) {
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
	if !s.tenantScopeAllows(w, r, parts[0]) {
		endHTTPSpan(span, traceError("tenant scope forbidden"))
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
	case errors.Is(err, storage.ErrWriteConflict),
		errors.Is(err, storage.ErrConflict),
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
		writeStorageError(w, err)
	}
}

// An authenticated tenant header scopes every tenant named by a lifecycle
// operation. Trusted platform operators use the private API without this header.
func (s *Server) tenantScopeAllows(w http.ResponseWriter, r *http.Request, target string) bool {
	if !storage.EnforceTenantScope(r.Context()) {
		return true
	}
	scope := r.Header.Get("X-Tenant-ID")
	if scope == "" && s.RequireTenantScope && !isAdminAuthRequest(r.Context()) {
		writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "X-Tenant-ID scope header is required", false, nil)
		return false
	}
	if scope != "" && scope != target {
		writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "tenant scope does not authorize the requested tenant", false, nil)
		return false
	}
	return true
}
