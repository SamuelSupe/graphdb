package storage

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const tenantMigrationSampleLimit = 100

type TenantMigrationOptions struct {
	DryRun    bool `json:"dry_run,omitempty"`
	Overwrite bool `json:"overwrite,omitempty"`
}

type TenantMigrationReport struct {
	SourceTenantID string                  `json:"source_tenant_id"`
	TargetTenantID string                  `json:"target_tenant_id"`
	SourcePrefix   string                  `json:"source_prefix"`
	TargetPrefix   string                  `json:"target_prefix"`
	DryRun         bool                    `json:"dry_run,omitempty"`
	Overwrite      bool                    `json:"overwrite,omitempty"`
	TargetExists   bool                    `json:"target_exists,omitempty"`
	Objects        int                     `json:"objects"`
	Bytes          int64                   `json:"bytes"`
	Copied         int                     `json:"copied,omitempty"`
	Skipped        int                     `json:"skipped,omitempty"`
	Samples        []TenantMigrationObject `json:"samples,omitempty"`
	StartedAt      time.Time               `json:"started_at"`
	FinishedAt     time.Time               `json:"finished_at"`
}

type TenantMigrationObject struct {
	SourceKey string `json:"source_key"`
	TargetKey string `json:"target_key"`
	Bytes     int64  `json:"bytes,omitempty"`
	ETag      string `json:"etag,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

func CopyTenantObjects(ctx context.Context, source *TenantStore, sourceTenantID string, target *TenantStore, targetTenantID string, options TenantMigrationOptions) (TenantMigrationReport, error) {
	if source == nil || target == nil {
		return TenantMigrationReport{}, fmt.Errorf("source and target stores are required")
	}
	if targetTenantID == "" {
		targetTenantID = sourceTenantID
	}
	if err := ValidateTenantID(sourceTenantID); err != nil {
		return TenantMigrationReport{}, err
	}
	if err := ValidateTenantID(targetTenantID); err != nil {
		return TenantMigrationReport{}, err
	}
	if sourceTenantID != targetTenantID {
		return TenantMigrationReport{}, fmt.Errorf("cross-tenant migration rewrites embedded tenant ids; use tenant backup/restore instead")
	}

	started := time.Now().UTC()
	sourcePrefix := source.tenantObjectPrefix(sourceTenantID)
	targetPrefix := target.tenantObjectPrefix(targetTenantID)
	report := TenantMigrationReport{
		SourceTenantID: sourceTenantID,
		TargetTenantID: targetTenantID,
		SourcePrefix:   sourcePrefix,
		TargetPrefix:   targetPrefix,
		DryRun:         options.DryRun,
		Overwrite:      options.Overwrite,
		StartedAt:      started,
	}

	sourceObjects, err := source.Objects.List(ctx, sourcePrefix)
	if err != nil {
		return report, err
	}
	if len(sourceObjects) == 0 {
		return report, ErrNotFound
	}
	targetObjects, err := target.Objects.List(ctx, targetPrefix)
	if err != nil {
		return report, err
	}
	rewrites := map[string][]byte{}
	if !options.DryRun {
		rewrites, err = source.prepareTenantMigrationRewrites(ctx, sourceTenantID, targetTenantID, sourceObjects, targetPrefix)
		if err != nil {
			return report, err
		}
	}
	report.TargetExists = len(targetObjects) > 0
	if report.TargetExists && !options.Overwrite && !options.DryRun {
		return report, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, targetTenantID)
	}
	if report.TargetExists && options.Overwrite && !options.DryRun {
		for _, object := range targetObjects {
			if err := target.Objects.Delete(ctx, object.Key); err != nil {
				return report, err
			}
		}
	}

	for _, object := range sourceObjects {
		if err := objectContextErr(ctx); err != nil {
			return report, err
		}
		relative := strings.TrimPrefix(object.Key, sourcePrefix)
		targetKey := targetPrefix + relative
		report.Objects++
		report.Bytes += object.Size
		if relative == "control/writer-lease.parquet" {
			report.Skipped++
			continue
		}
		if len(report.Samples) < tenantMigrationSampleLimit {
			report.Samples = append(report.Samples, TenantMigrationObject{SourceKey: object.Key, TargetKey: targetKey, Bytes: object.Size, ETag: object.ETag})
		}
		if options.DryRun {
			report.Skipped++
			continue
		}
		data := rewrites[object.Key]
		if len(data) == 0 {
			data, err = source.Objects.Get(ctx, object.Key)
			if err != nil {
				return report, err
			}
		}
		if err := target.Objects.Put(ctx, targetKey, data); err != nil {
			return report, err
		}
		report.Copied++
		if len(report.Samples) > 0 && report.Samples[len(report.Samples)-1].SourceKey == object.Key {
			report.Samples[len(report.Samples)-1].SHA256 = objectContentHash(data)
		}
	}
	if !options.DryRun {
		if err := target.addTenantToRegistry(ctx, targetTenantID); err != nil {
			return report, err
		}
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func (s *TenantStore) prepareTenantMigrationRewrites(ctx context.Context, tenantID string, targetTenantID string, objects []ObjectInfo, targetPrefix string) (map[string][]byte, error) {
	sourcePrefix := s.tenantObjectPrefix(tenantID)
	if sourcePrefix == targetPrefix && tenantID == targetTenantID {
		return nil, nil
	}
	rewrites := map[string][]byte{}
	segmentHashes := map[string]string{}
	for _, object := range objects {
		relative := strings.TrimPrefix(object.Key, sourcePrefix)
		if !strings.HasPrefix(relative, "commits/segments/") {
			continue
		}
		data, err := s.Objects.Get(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		rewritten, hash, err := rewriteCommitSegmentObject(ctx, data, tenantID, sourcePrefix, targetPrefix)
		if err != nil {
			return nil, err
		}
		rewrites[object.Key] = rewritten
		segmentHashes[object.Key] = hash
	}
	for _, object := range objects {
		if _, ok := rewrites[object.Key]; ok {
			continue
		}
		data, err := s.Objects.Get(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		rewritten, changed, err := s.rewriteTenantMigrationObject(ctx, data, object.Key, tenantID, targetTenantID, sourcePrefix, targetPrefix, segmentHashes)
		if err != nil {
			return nil, err
		}
		if changed {
			rewrites[object.Key] = rewritten
		}
	}
	return rewrites, nil
}

func (s *TenantStore) rewriteTenantMigrationObject(ctx context.Context, data []byte, key string, tenantID string, targetTenantID string, sourcePrefix string, targetPrefix string, segmentHashes map[string]string) ([]byte, bool, error) {
	relative := strings.TrimPrefix(key, sourcePrefix)
	switch {
	case relative == "manifest.parquet":
		manifest, err := decodeParquetManifest(ctx, data)
		if err != nil {
			return nil, false, err
		}
		manifest.TenantID = targetTenantID
		manifest.SnapshotKey = rewriteTenantObjectKey(manifest.SnapshotKey, sourcePrefix, targetPrefix)
		manifest.SnapshotCatalogKey = rewriteTenantObjectKey(manifest.SnapshotCatalogKey, sourcePrefix, targetPrefix)
		for i := range manifest.CommitKeys {
			manifest.CommitKeys[i] = rewriteTenantObjectKey(manifest.CommitKeys[i], sourcePrefix, targetPrefix)
		}
		for i := range manifest.CommitSegments {
			oldKey := manifest.CommitSegments[i].Key
			manifest.CommitSegments[i].Key = rewriteTenantObjectKey(oldKey, sourcePrefix, targetPrefix)
			if hash := segmentHashes[oldKey]; hash != "" {
				manifest.CommitSegments[i].ContentHash = hash
			}
		}
		rewritten, err := marshalParquetManifest(ctx, manifest)
		return rewritten, true, err
	case strings.HasPrefix(relative, "snapshots/sharded/") && strings.HasSuffix(relative, "/catalog.parquet"):
		catalog, err := decodeParquetShardedSnapshotCatalog(ctx, data)
		if err != nil {
			return nil, false, err
		}
		catalog.TenantID = targetTenantID
		catalog.Key = rewriteTenantObjectKey(catalog.Key, sourcePrefix, targetPrefix)
		catalog.Schema.Key = rewriteTenantObjectKey(catalog.Schema.Key, sourcePrefix, targetPrefix)
		for i := range catalog.EntityPages {
			catalog.EntityPages[i].Key = rewriteTenantObjectKey(catalog.EntityPages[i].Key, sourcePrefix, targetPrefix)
		}
		for i := range catalog.EdgeShards {
			catalog.EdgeShards[i].Key = rewriteTenantObjectKey(catalog.EdgeShards[i].Key, sourcePrefix, targetPrefix)
		}
		rewritten, err := marshalParquetShardedSnapshotCatalog(ctx, catalog)
		return rewritten, true, err
	case relative == "indexes/catalog.parquet":
		catalog, err := decodeParquetIndexCatalog(ctx, data)
		if err != nil {
			return nil, false, err
		}
		catalog.TenantID = targetTenantID
		for i := range catalog.Indexes {
			for j := range catalog.Indexes[i].Objects {
				catalog.Indexes[i].Objects[j].Key = rewriteTenantObjectKey(catalog.Indexes[i].Objects[j].Key, sourcePrefix, targetPrefix)
			}
		}
		for i := range catalog.EdgeShards {
			for j := range catalog.EdgeShards[i].Objects {
				catalog.EdgeShards[i].Objects[j].Key = rewriteTenantObjectKey(catalog.EdgeShards[i].Objects[j].Key, sourcePrefix, targetPrefix)
			}
		}
		for i := range catalog.EntityPages {
			for j := range catalog.EntityPages[i].Objects {
				catalog.EntityPages[i].Objects[j].Key = rewriteTenantObjectKey(catalog.EntityPages[i].Objects[j].Key, sourcePrefix, targetPrefix)
			}
		}
		rewritten, err := marshalParquetIndexCatalog(ctx, catalog)
		return rewritten, true, err
	case strings.HasPrefix(relative, "indexes/entities/by-id/"):
		id, _, err := s.entityIDFromRecordKey(tenantID, key)
		if err != nil {
			return nil, false, err
		}
		record, err := decodeParquetEntityRecord(ctx, data, tenantID, id)
		if err != nil {
			return nil, false, err
		}
		record.TenantID = targetTenantID
		record.Page = rewriteTenantObjectKey(record.Page, sourcePrefix, targetPrefix)
		record.ContentHash = entityRecordContentHash(record)
		rewritten, err := marshalParquetEntityRecord(ctx, record)
		return rewritten, true, err
	case strings.HasPrefix(relative, "backups/") && strings.HasSuffix(relative, "/manifest.parquet"):
		_, backupID, ok := s.backupManifestIdentityFromKey(key)
		if !ok {
			return nil, false, fmt.Errorf("invalid backup manifest key")
		}
		manifest, err := s.loadBackupManifestFromBytes(ctx, data, tenantID, backupID)
		if err != nil {
			return nil, false, err
		}
		manifest.TenantID = targetTenantID
		manifest.BackupRecordKey = rewriteTenantObjectKey(manifest.BackupRecordKey, sourcePrefix, targetPrefix)
		for i := range manifest.Objects {
			manifest.Objects[i].Key = rewriteTenantObjectKey(manifest.Objects[i].Key, sourcePrefix, targetPrefix)
		}
		rewritten, err := marshalParquetTaskResult(ctx, targetTenantID, backupID+"-manifest", taskResult(manifest))
		return rewritten, true, err
	default:
		return nil, false, nil
	}
}

func rewriteCommitSegmentObject(ctx context.Context, data []byte, tenantID string, sourcePrefix string, targetPrefix string) ([]byte, string, error) {
	_, items, err := decodeParquetCommitSegmentObject(ctx, data, tenantID, CommitSegmentRef{})
	if err != nil {
		return nil, "", err
	}
	for i := range items {
		items[i].Key = rewriteTenantObjectKey(items[i].Key, sourcePrefix, targetPrefix)
	}
	payload, err := marshalCommitSegmentPayload(items)
	if err != nil {
		return nil, "", err
	}
	rewritten, err := marshalParquetCommitSegment(ctx, tenantID, items)
	if err != nil {
		return nil, "", err
	}
	return rewritten, objectContentHash(payload), nil
}

func rewriteTenantObjectKey(key string, sourcePrefix string, targetPrefix string) string {
	if key == "" || !strings.HasPrefix(key, sourcePrefix) {
		return key
	}
	return targetPrefix + strings.TrimPrefix(key, sourcePrefix)
}
