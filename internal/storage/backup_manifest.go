package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"
)

const backupManifestFormat = "graphdb-tenant-backup-manifest-v1"

type TenantBackupManifest struct {
	Format          string              `json:"format"`
	TenantID        string              `json:"tenant_id"`
	BackupID        string              `json:"backup_id"`
	Version         int64               `json:"version"`
	CreatedAt       time.Time           `json:"created_at"`
	BackupRecordKey string              `json:"backup_record_key"`
	Objects         []BackupObjectRef   `json:"objects,omitempty"`
	Stats           BackupManifestStats `json:"stats"`
}

type BackupObjectRef struct {
	Role         string `json:"role"`
	Key          string `json:"key"`
	Kind         string `json:"kind,omitempty"`
	Field        string `json:"field,omitempty"`
	RelationType string `json:"relation_type,omitempty"`
	Shard        string `json:"shard,omitempty"`
	IndexType    string `json:"index_type,omitempty"`
	Bytes        int64  `json:"bytes,omitempty"`
	ETag         string `json:"etag,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
	RowCount     int    `json:"row_count,omitempty"`
	ContentHash  string `json:"content_hash,omitempty"`
	SchemaHash   string `json:"schema_hash,omitempty"`
	Required     bool   `json:"required,omitempty"`
}

type BackupManifestStats struct {
	ObjectCount int   `json:"object_count"`
	TotalBytes  int64 `json:"total_bytes"`
	Entities    int   `json:"entities"`
	Edges       int   `json:"edges"`
}

type BackupIntegrityReport struct {
	Status      string    `json:"status"`
	CheckedAt   time.Time `json:"checked_at"`
	Objects     int       `json:"objects"`
	Bytes       int64     `json:"bytes"`
	ManifestKey string    `json:"manifest_key,omitempty"`
	Issues      []string  `json:"issues,omitempty"`
}

type RestoreIntegrityReport struct {
	Status              string    `json:"status"`
	CheckedAt           time.Time `json:"checked_at"`
	ManifestVersion     int64     `json:"manifest_version,omitempty"`
	SnapshotVersion     int64     `json:"snapshot_version,omitempty"`
	IndexCatalogVersion int64     `json:"index_catalog_version,omitempty"`
	Issues              []string  `json:"issues,omitempty"`
}

func (s *TenantStore) putBackupManifest(ctx context.Context, tenantID string, backupID string, manifest TenantBackupManifest) (string, error) {
	manifest.Format = backupManifestFormat
	manifest.TenantID = tenantID
	manifest.BackupID = backupID
	key := s.backupManifestKey(tenantID, backupID)
	data, err := marshalParquetTaskResult(ctx, tenantID, backupID+"-manifest", taskResult(manifest))
	if err != nil {
		return "", err
	}
	if err := s.putTenantGenerationObject(ctx, tenantID, key, data); err != nil {
		return "", err
	}
	return key, nil
}

func (s *TenantStore) loadBackupManifest(ctx context.Context, key string) (TenantBackupManifest, error) {
	tenantID, backupID, ok := s.backupManifestIdentityFromKey(key)
	if !ok {
		return TenantBackupManifest{}, fmt.Errorf("invalid backup manifest key")
	}
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return TenantBackupManifest{}, err
	}
	return s.loadBackupManifestFromBytes(ctx, data, tenantID, backupID)
}

func (s *TenantStore) loadBackupManifestFromBytes(ctx context.Context, data []byte, tenantID string, backupID string) (TenantBackupManifest, error) {
	result, err := decodeParquetTaskResult(ctx, data, tenantID, backupID+"-manifest")
	if err != nil {
		return TenantBackupManifest{}, err
	}
	var manifest TenantBackupManifest
	payload, err := json.Marshal(result)
	if err != nil {
		return TenantBackupManifest{}, err
	}
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return TenantBackupManifest{}, err
	}
	if manifest.Format != backupManifestFormat || manifest.TenantID == "" || manifest.BackupRecordKey == "" {
		return TenantBackupManifest{}, fmt.Errorf("invalid backup manifest")
	}
	return manifest, nil
}

func (s *TenantStore) backupManifestIdentityFromKey(key string) (string, string, bool) {
	prefix := path.Join(s.Prefix, "tenants") + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	tenantID, tail, ok := strings.Cut(rest, "/backups/")
	if !ok || ValidateTenantID(tenantID) != nil {
		return "", "", false
	}
	backupID, file, ok := strings.Cut(tail, "/")
	if !ok || file != "manifest.parquet" {
		return "", "", false
	}
	decoded, err := url.PathUnescape(backupID)
	if err != nil || decoded == "" {
		return "", "", false
	}
	return tenantID, decoded, true
}

func (s *TenantStore) buildBackupManifest(
	ctx context.Context,
	tenantID string,
	backupID string,
	record TenantBackupRecord,
	backupRecordKey string,
) (TenantBackupManifest, error) {
	ref, err := s.backupObjectRef(ctx, BackupObjectRef{Role: "backup_record", Key: backupRecordKey, RowCount: 1, Required: true})
	if err != nil {
		return TenantBackupManifest{}, err
	}
	return TenantBackupManifest{
		Format: backupManifestFormat, TenantID: tenantID, BackupID: backupID,
		Version: record.Version, CreatedAt: record.CreatedAt, BackupRecordKey: backupRecordKey,
		Objects: []BackupObjectRef{ref},
		Stats:   BackupManifestStats{ObjectCount: 1, TotalBytes: ref.Bytes, Entities: len(record.Snapshot.Entities), Edges: len(record.Snapshot.Edges)},
	}, nil
}

func (s *TenantStore) backupObjectRef(ctx context.Context, ref BackupObjectRef) (BackupObjectRef, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, ref.Key)
	if err != nil {
		return BackupObjectRef{}, err
	}
	ref.Bytes = int64(len(data))
	ref.ETag = meta.ETag
	ref.SHA256 = objectContentHash(data)
	return ref, nil
}

func (s *TenantStore) validateBackupManifest(ctx context.Context, manifest TenantBackupManifest) BackupIntegrityReport {
	report := BackupIntegrityReport{Status: "ok", CheckedAt: time.Now().UTC(), ManifestKey: s.backupManifestKey(manifest.TenantID, manifest.BackupID)}
	foundRecord := false
	for _, ref := range manifest.Objects {
		// Older manifests also described live indexes and heads. They are not
		// recovery inputs: the backup record contains the complete snapshot.
		if ref.Role != "backup_record" || ref.Key != manifest.BackupRecordKey {
			continue
		}
		foundRecord = true
		data, _, err := s.Objects.GetWithMeta(ctx, ref.Key)
		if err != nil {
			if ref.Required {
				report.Issues = append(report.Issues, "missing required object "+ref.Key+": "+err.Error())
			} else {
				report.Issues = append(report.Issues, "missing optional object "+ref.Key+": "+err.Error())
			}
			continue
		}
		report.Objects++
		report.Bytes += int64(len(data))
		if ref.Bytes > 0 && int64(len(data)) != ref.Bytes {
			report.Issues = append(report.Issues, "object "+ref.Key+" bytes mismatch")
		}
		if ref.SHA256 != "" && objectContentHash(data) != ref.SHA256 {
			report.Issues = append(report.Issues, "object "+ref.Key+" sha256 mismatch")
		}
	}
	if !foundRecord {
		report.Issues = append(report.Issues, "backup record checksum reference is missing")
	}
	if len(report.Issues) > 0 {
		report.Status = "error"
	}
	return report
}

func (s *TenantStore) restoreIntegrityReport(ctx context.Context, tenantID string) RestoreIntegrityReport {
	report := RestoreIntegrityReport{Status: "ok", CheckedAt: time.Now().UTC()}
	health, err := s.IndexHealth(ctx, tenantID)
	if err != nil {
		report.Status = "error"
		report.Issues = []string{err.Error()}
		return report
	}
	report.ManifestVersion = health.ManifestVersion
	report.SnapshotVersion = health.SnapshotVersion
	report.IndexCatalogVersion = health.CatalogVersion
	report.Issues = append(report.Issues, health.Issues...)
	if health.Status != "ready" {
		report.Status = health.Status
		if report.Status == "" {
			report.Status = "error"
		}
	}
	return report
}
