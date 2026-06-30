package storage

import (
	"context"
	"errors"
	"time"
)

type IntegrityAuditOptions struct {
	Deep bool `json:"deep,omitempty"`
}

type IntegrityAuditReport struct {
	TenantID            string                 `json:"tenant_id"`
	Status              string                 `json:"status"`
	CheckedAt           time.Time              `json:"checked_at"`
	ManifestVersion     int64                  `json:"manifest_version,omitempty"`
	SnapshotVersion     int64                  `json:"snapshot_version,omitempty"`
	IndexCatalogVersion int64                  `json:"index_catalog_version,omitempty"`
	Objects             int                    `json:"objects"`
	Bytes               int64                  `json:"bytes"`
	Issues              []IntegrityAuditIssue  `json:"issues,omitempty"`
	Checks              []IntegrityObjectCheck `json:"checks,omitempty"`
}

type IntegrityAuditIssue struct {
	Code         string `json:"code"`
	Severity     string `json:"severity"`
	ResourceType string `json:"resource_type,omitempty"`
	ResourceID   string `json:"resource_id,omitempty"`
	ObjectKey    string `json:"object_key,omitempty"`
	Message      string `json:"message"`
}

type IntegrityObjectCheck struct {
	Role        string `json:"role"`
	Key         string `json:"key"`
	Status      string `json:"status"`
	Bytes       int64  `json:"bytes,omitempty"`
	RowCount    int    `json:"row_count,omitempty"`
	ContentHash string `json:"content_hash,omitempty"`
	SchemaHash  string `json:"schema_hash,omitempty"`
}

func (s *TenantStore) AuditIntegrity(ctx context.Context, tenantID string, options IntegrityAuditOptions) (IntegrityAuditReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IntegrityAuditReport{}, err
	}
	report := IntegrityAuditReport{
		TenantID:  tenantID,
		Status:    "ok",
		CheckedAt: time.Now().UTC(),
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		report.addIssue("manifest_unreadable", "error", "manifest", "", s.manifestKey(tenantID), err.Error())
		report.finish()
		return report, nil
	}
	report.ManifestVersion = manifest.Version
	report.SnapshotVersion = manifest.SnapshotVersion
	s.auditManifestObject(ctx, tenantID, manifest, &report)
	s.auditSnapshotCatalog(ctx, tenantID, manifest, &report)
	s.auditIndexCatalog(ctx, tenantID, manifest, options, &report)
	report.finish()
	return report, nil
}

func (s *TenantStore) auditManifestObject(ctx context.Context, tenantID string, manifest Manifest, report *IntegrityAuditReport) {
	key := s.manifestKey(tenantID)
	data, _, ok := s.auditReadObject(ctx, key, "tenant_manifest", true, report)
	if !ok {
		return
	}
	decoded, err := decodeParquetManifest(ctx, data)
	if err != nil {
		report.addIssue("manifest_decode_failed", "error", "manifest", "", key, err.Error())
		return
	}
	if decoded.TenantID != "" && decoded.TenantID != tenantID {
		report.addIssue("manifest_tenant_mismatch", "error", "manifest", "", key, "manifest tenant does not match request tenant")
	}
	if decoded.Version != manifest.Version || decoded.SnapshotVersion != manifest.SnapshotVersion || decoded.SnapshotCatalogKey != manifest.SnapshotCatalogKey {
		report.addIssue("manifest_read_mismatch", "error", "manifest", "", key, "manifest object changed during audit")
	}
	contentHash, err := manifestContentHash(decoded)
	if err == nil {
		report.updateLastCheck(1, contentHash, "")
	}
}

func (s *TenantStore) auditReadObject(ctx context.Context, key string, role string, required bool, report *IntegrityAuditReport) ([]byte, ObjectMeta, bool) {
	if key == "" {
		report.addIssue(role+"_key_missing", severityForRequired(required), role, "", "", role+" key is missing")
		return nil, ObjectMeta{}, false
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		code := role + "_missing"
		if !errors.Is(err, ErrNotFound) {
			code = role + "_read_failed"
		}
		report.addIssue(code, severityForRequired(required), role, "", key, err.Error())
		return nil, meta, false
	}
	report.Objects++
	report.Bytes += int64(len(data))
	report.Checks = append(report.Checks, IntegrityObjectCheck{
		Role:        role,
		Key:         key,
		Status:      "ok",
		Bytes:       int64(len(data)),
		ContentHash: objectContentHash(data),
	})
	return data, meta, true
}

func (r *IntegrityAuditReport) addIssue(code string, severity string, resourceType string, resourceID string, objectKey string, message string) {
	if severity == "" {
		severity = "error"
	}
	r.Issues = append(r.Issues, IntegrityAuditIssue{
		Code:         code,
		Severity:     severity,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		ObjectKey:    objectKey,
		Message:      message,
	})
}

func (r *IntegrityAuditReport) updateLastCheck(rowCount int, contentHash string, schemaHash string) {
	if len(r.Checks) == 0 {
		return
	}
	check := &r.Checks[len(r.Checks)-1]
	check.RowCount = rowCount
	if contentHash != "" {
		check.ContentHash = contentHash
	}
	if schemaHash != "" {
		check.SchemaHash = schemaHash
	}
}

func (r *IntegrityAuditReport) finish() {
	r.Status = "ok"
	for _, issue := range r.Issues {
		if issue.Severity == "error" {
			r.Status = "error"
			return
		}
		if r.Status == "ok" {
			r.Status = issue.Severity
		}
	}
}

func severityForRequired(required bool) string {
	if required {
		return "error"
	}
	return "warn"
}
