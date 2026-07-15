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

func (s *TenantStore) buildBackupManifest(ctx context.Context, tenantID string, backupID string, record TenantBackupRecord, backupRecordKey string, manifest Manifest) (TenantBackupManifest, error) {
	refs := []BackupObjectRef{}
	addRef := func(ref BackupObjectRef) error {
		if ref.Key == "" {
			return nil
		}
		filled, err := s.backupObjectRef(ctx, ref)
		if err != nil {
			return err
		}
		refs = append(refs, filled)
		return nil
	}
	addKey := func(role string, key string, rowCount int, contentHash string, schemaHash string, required bool) error {
		return addRef(BackupObjectRef{Role: role, Key: key, RowCount: rowCount, ContentHash: contentHash, SchemaHash: schemaHash, Required: required})
	}
	if err := addKey("backup_record", backupRecordKey, 1, "", "", true); err != nil {
		return TenantBackupManifest{}, err
	}
	if err := addKey("tenant_manifest", s.manifestKey(tenantID), 1, "", "", true); err != nil {
		return TenantBackupManifest{}, err
	}
	if err := addKey("snapshot_record", manifest.SnapshotKey, 1, "", "", false); err != nil {
		return TenantBackupManifest{}, err
	}
	if err := s.backupSnapshotCatalogRefs(ctx, tenantID, manifest, addRef); err != nil {
		return TenantBackupManifest{}, err
	}
	if catalog, err := s.GetIndexCatalog(ctx, tenantID); err == nil && catalog.Version == manifest.Version {
		catalogHash, err := indexCatalogContentHash(catalog)
		if err != nil {
			return TenantBackupManifest{}, err
		}
		if err := addRef(BackupObjectRef{Role: "index_catalog", Key: s.indexCatalogKey(tenantID), RowCount: 1, ContentHash: catalogHash, SchemaHash: parquetIndexCatalogSchemaHash()}); err != nil {
			return TenantBackupManifest{}, err
		}
		for _, index := range catalog.Indexes {
			for _, object := range index.Objects {
				if err := addRef(BackupObjectRef{
					Role:        "secondary_index",
					Key:         object.Key,
					Kind:        index.Kind,
					Field:       index.Field,
					IndexType:   index.Type,
					RowCount:    object.RowCount,
					ContentHash: object.ContentHash,
					SchemaHash:  object.SchemaHash,
				}); err != nil {
					return TenantBackupManifest{}, err
				}
			}
		}
		for _, shard := range catalog.EdgeShards {
			for _, object := range shard.Objects {
				if err := addRef(BackupObjectRef{
					Role:         "edge_shard",
					Key:          object.Key,
					RelationType: shard.RelationType,
					Shard:        shard.Shard,
					RowCount:     object.RowCount,
					ContentHash:  object.ContentHash,
					SchemaHash:   object.SchemaHash,
				}); err != nil {
					return TenantBackupManifest{}, err
				}
			}
		}
		for _, page := range catalog.EntityPages {
			for _, object := range page.Objects {
				if err := addRef(BackupObjectRef{
					Role:        "entity_page",
					Key:         object.Key,
					Shard:       page.Shard,
					RowCount:    object.RowCount,
					ContentHash: page.ContentHash,
					SchemaHash:  object.SchemaHash,
				}); err != nil {
					return TenantBackupManifest{}, err
				}
			}
		}
	}
	stats := BackupManifestStats{ObjectCount: len(refs), Entities: len(record.Snapshot.Entities), Edges: len(record.Snapshot.Edges)}
	for _, ref := range refs {
		stats.TotalBytes += ref.Bytes
	}
	return TenantBackupManifest{
		Format:          backupManifestFormat,
		TenantID:        tenantID,
		BackupID:        backupID,
		Version:         record.Version,
		CreatedAt:       record.CreatedAt,
		BackupRecordKey: backupRecordKey,
		Objects:         refs,
		Stats:           stats,
	}, nil
}

func (s *TenantStore) backupSnapshotCatalogRefs(ctx context.Context, tenantID string, manifest Manifest, addRef func(BackupObjectRef) error) error {
	if manifest.SnapshotCatalogKey == "" {
		return nil
	}
	catalog, err := s.getShardedSnapshotCatalog(ctx, tenantID, manifest.SnapshotCatalogKey)
	if err != nil {
		return err
	}
	if err := addRef(BackupObjectRef{Role: "snapshot_catalog", Key: manifest.SnapshotCatalogKey, RowCount: 1, ContentHash: shardedSnapshotCatalogContentHash(catalog), SchemaHash: parquetSnapshotCatalogSchemaHash(), Required: true}); err != nil {
		return err
	}
	if err := addRef(BackupObjectRef{Role: "snapshot_schema", Key: catalog.Schema.Key, RowCount: 1, ContentHash: catalog.Schema.ContentHash, Required: true}); err != nil {
		return err
	}
	for _, page := range catalog.EntityPages {
		if err := addRef(BackupObjectRef{Role: "snapshot_entity_page", Key: page.Key, Shard: page.Shard, RowCount: page.EntityCount, ContentHash: page.ContentHash, SchemaHash: parquetEntityPageSchemaHash(), Required: true}); err != nil {
			return err
		}
	}
	for _, shard := range catalog.EdgeShards {
		if err := addRef(BackupObjectRef{Role: "snapshot_edge_shard", Key: shard.Key, RelationType: shard.RelationType, Shard: shard.Shard, RowCount: shard.EdgeCount, ContentHash: shard.ContentHash, SchemaHash: parquetEdgeShardSchemaHash(), Required: true}); err != nil {
			return err
		}
	}
	return nil
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
	for _, ref := range manifest.Objects {
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
		report.Issues = append(report.Issues, s.validateBackupObjectPayload(ctx, manifest, ref, data)...)
	}
	if len(report.Issues) > 0 {
		report.Status = "error"
	}
	return report
}

func (s *TenantStore) validateBackupObjectPayload(ctx context.Context, manifest TenantBackupManifest, ref BackupObjectRef, data []byte) []string {
	switch ref.Role {
	case "snapshot_entity_page", "entity_page":
		page, err := decodeParquetEntityPage(ctx, data, manifest.TenantID, ref.Shard, manifest.Version)
		if err != nil {
			return []string{"object " + ref.Key + " decode failed: " + err.Error()}
		}
		return backupContentIssues(ref, len(page.Entities), entityPageContentHash(page), parquetEntityPageSchemaHash())
	case "snapshot_edge_shard", "edge_shard":
		shard, err := decodeParquetEdgeShard(ctx, data, manifest.TenantID, ref.RelationType, ref.Shard, manifest.Version)
		if err != nil {
			return []string{"object " + ref.Key + " decode failed: " + err.Error()}
		}
		return backupContentIssues(ref, len(shard.Edges), edgeShardContentHash(shard), parquetEdgeShardSchemaHash())
	case "secondary_index":
		index, err := decodeParquetSecondaryIndex(ctx, data, manifest.TenantID, ref.Kind, ref.Field, manifest.Version, strings.Contains(ref.IndexType, "unique"))
		if err != nil {
			return []string{"object " + ref.Key + " decode failed: " + err.Error()}
		}
		return backupContentIssues(ref, secondaryIndexEntryCount(index), secondaryIndexContentHash(index), parquetSecondaryIndexSchemaHash())
	case "snapshot_schema":
		schema, err := decodeParquetSnapshotSchema(ctx, data)
		if err != nil {
			return []string{"object " + ref.Key + " decode failed: " + err.Error()}
		}
		return backupContentIssues(ref, 1, snapshotSchemaContentHash(schema), "")
	case "snapshot_catalog":
		catalog, err := decodeParquetShardedSnapshotCatalog(ctx, data)
		if err != nil {
			return []string{"object " + ref.Key + " decode failed: " + err.Error()}
		}
		return backupContentIssues(ref, 1, shardedSnapshotCatalogContentHash(catalog), parquetSnapshotCatalogSchemaHash())
	case "index_catalog":
		catalog, err := decodeParquetIndexCatalog(ctx, data)
		if err != nil {
			return []string{"object " + ref.Key + " decode failed: " + err.Error()}
		}
		contentHash, err := indexCatalogContentHash(catalog)
		if err != nil {
			return []string{"object " + ref.Key + " content_hash unavailable: " + err.Error()}
		}
		return backupContentIssues(ref, 1, contentHash, parquetIndexCatalogSchemaHash())
	default:
		return nil
	}
}

func backupContentIssues(ref BackupObjectRef, rowCount int, contentHash string, schemaHash string) []string {
	issues := []string{}
	if ref.RowCount > 0 && ref.RowCount != rowCount {
		issues = append(issues, "object "+ref.Key+" row_count mismatch")
	}
	if ref.ContentHash != "" && ref.ContentHash != contentHash {
		issues = append(issues, "object "+ref.Key+" content_hash mismatch")
	}
	if ref.SchemaHash != "" && schemaHash != "" && ref.SchemaHash != schemaHash {
		issues = append(issues, "object "+ref.Key+" schema_hash mismatch")
	}
	return issues
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
