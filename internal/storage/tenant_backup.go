package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type TenantBackupRecord struct {
	TenantID     string              `json:"tenant_id"`
	Version      int64               `json:"version"`
	CreatedAt    time.Time           `json:"created_at"`
	Metadata     TenantMetadata      `json:"metadata,omitempty"`
	Config       *TenantConfig       `json:"config,omitempty"`
	SourcePolicy *graph.SourcePolicy `json:"source_policy,omitempty"`
	Snapshot     graph.Snapshot      `json:"snapshot"`
}

type TenantRestoreReport struct {
	TenantID            string                 `json:"tenant_id"`
	BackupKey           string                 `json:"backup_key"`
	BackupManifestKey   string                 `json:"backup_manifest_key,omitempty"`
	SourceTenantID      string                 `json:"source_tenant_id"`
	Version             int64                  `json:"version"`
	Entities            int                    `json:"entities"`
	Edges               int                    `json:"edges"`
	IndexCatalogVersion int64                  `json:"index_catalog_version,omitempty"`
	DryRun              bool                   `json:"dry_run,omitempty"`
	TargetExists        bool                   `json:"target_exists,omitempty"`
	Overwrote           bool                   `json:"overwrote,omitempty"`
	BackupIntegrity     BackupIntegrityReport  `json:"backup_integrity"`
	RestoreIntegrity    RestoreIntegrityReport `json:"restore_integrity,omitempty"`
	RestoredAt          time.Time              `json:"restored_at"`
}

func (s *TenantStore) tenantBackupTask(ctx context.Context, task Task) (map[string]any, string, error) {
	total := taskProgressTotal(task.Type)
	if summary, resultKey, ok, err := s.resumeTenantBackupTask(ctx, task); err != nil {
		return nil, "", err
	} else if ok {
		_ = s.updateTaskActionProgress(ctx, task, "backup_done", total, total, taskActionUpdate{
			ID:     "validate_backup_manifest",
			Status: "completed",
			Output: map[string]any{"backup_key": resultKey, "backup_manifest_key": summary["backup_manifest_key"], "resumed": true},
			Verification: map[string]any{
				"backup_integrity": "ok",
			},
		}, map[string]any{"phase": "backup_done", "backup_key": resultKey, "backup_manifest_key": summary["backup_manifest_key"], "resumed": true})
		return summary, resultKey, nil
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_load_snapshot", 1, total, taskActionUpdate{
		ID:     "load_snapshot_metadata",
		Status: "running",
	}, nil); err != nil {
		return nil, "", err
	}
	g, manifest, err := s.Load(ctx, task.TenantID)
	if err != nil {
		_ = s.updateTaskActionProgress(context.Background(), task, "backup_load_snapshot", 1, total, taskActionUpdate{ID: "load_snapshot_metadata", Err: err}, nil)
		return nil, "", err
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_load_metadata", 2, total, taskActionUpdate{
		ID:     "load_snapshot_metadata",
		Status: "completed",
		Output: map[string]any{
			"version":              manifest.Version,
			"snapshot_key":         manifest.SnapshotKey,
			"snapshot_catalog_key": manifest.SnapshotCatalogKey,
			"entities":             len(g.Snapshot().Entities),
			"edges":                len(g.Snapshot().Edges),
		},
	}, map[string]any{"phase": "backup_load_metadata", "version": manifest.Version, "source_manifest_version": manifest.Version, "source_snapshot_key": manifest.SnapshotKey, "source_snapshot_catalog_key": manifest.SnapshotCatalogKey}); err != nil {
		return nil, "", err
	}
	metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, task.TenantID)
	if err != nil {
		return nil, "", err
	}
	if !configured {
		metadata = legacyTenantMetadata(task.TenantID)
	}
	record := TenantBackupRecord{
		TenantID:  task.TenantID,
		Version:   manifest.Version,
		CreatedAt: time.Now().UTC(),
		Metadata:  metadata,
		Snapshot:  g.Snapshot(),
	}
	if config, ok, err := s.GetTenantConfig(ctx, task.TenantID); err != nil {
		return nil, "", err
	} else if ok {
		record.Config = &config
	}
	if policy, ok, err := s.GetSourcePolicy(ctx, task.TenantID); err != nil {
		return nil, "", err
	} else if ok {
		record.SourcePolicy = &policy
	}
	resultKey := s.taskResultKey(task.TenantID, task.ID)
	backupRecordReady := false
	if checkpointKey := taskCheckpointString(s.taskStateOrLocal(ctx, task), "backup_key"); checkpointKey != "" {
		if loaded, err := s.loadTenantBackupRecord(ctx, checkpointKey); err == nil && loaded.TenantID == task.TenantID && loaded.Version == manifest.Version {
			record = loaded
			resultKey = checkpointKey
			backupRecordReady = true
			if err := s.updateTaskActionProgress(ctx, task, "backup_result_written", 3, total, taskActionUpdate{
				ID:     "write_backup_record",
				Status: "completed",
				Output: map[string]any{"backup_key": resultKey, "version": record.Version, "resumed": true},
				Verification: map[string]any{
					"record_readable": true,
				},
			}, map[string]any{"phase": "backup_result_written", "version": record.Version, "backup_key": resultKey}); err != nil {
				return nil, "", err
			}
		}
	}
	if !backupRecordReady {
		if err := s.updateTaskActionProgress(ctx, task, "backup_write_result", 3, total, taskActionUpdate{
			ID:     "write_backup_record",
			Status: "running",
			Input:  map[string]any{"backup_key": resultKey, "version": manifest.Version},
		}, map[string]any{"phase": "backup_write_result", "version": manifest.Version, "backup_key": resultKey}); err != nil {
			return nil, "", err
		}
		if err := s.putTaskResult(ctx, task.TenantID, task.ID, taskResult(record)); err != nil {
			_ = s.updateTaskActionProgress(context.Background(), task, "backup_write_result", 3, total, taskActionUpdate{ID: "write_backup_record", Err: err}, nil)
			return nil, "", err
		}
		if err := s.updateTaskActionProgress(ctx, task, "backup_result_written", 3, total, taskActionUpdate{
			ID:     "write_backup_record",
			Status: "completed",
			Output: map[string]any{"backup_key": resultKey, "version": manifest.Version},
			Verification: map[string]any{
				"record_readable": true,
			},
		}, map[string]any{"phase": "backup_result_written", "version": manifest.Version, "backup_key": resultKey}); err != nil {
			return nil, "", err
		}
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_write_manifest", 4, total, taskActionUpdate{
		ID:     "write_backup_manifest",
		Status: "running",
		Input:  map[string]any{"backup_key": resultKey, "version": record.Version},
	}, map[string]any{"phase": "backup_write_manifest", "version": record.Version, "backup_key": resultKey}); err != nil {
		return nil, "", err
	}
	backupManifestKey := taskCheckpointString(s.taskStateOrLocal(ctx, task), "backup_manifest_key")
	var backupManifest TenantBackupManifest
	if backupManifestKey != "" {
		backupManifest, err = s.loadBackupManifest(ctx, backupManifestKey)
		if err != nil {
			backupManifestKey = ""
		}
	}
	if backupManifestKey == "" {
		backupManifest, err = s.buildBackupManifest(ctx, task.TenantID, task.ID, record, resultKey, manifest)
		if err != nil {
			_ = s.updateTaskActionProgress(context.Background(), task, "backup_write_manifest", 4, total, taskActionUpdate{ID: "write_backup_manifest", Err: err}, nil)
			return nil, "", err
		}
		backupManifestKey, err = s.putBackupManifest(ctx, task.TenantID, task.ID, backupManifest)
		if err != nil {
			_ = s.updateTaskActionProgress(context.Background(), task, "backup_write_manifest", 4, total, taskActionUpdate{ID: "write_backup_manifest", Err: err}, nil)
			return nil, "", err
		}
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_manifest_written", 4, total, taskActionUpdate{
		ID:     "write_backup_manifest",
		Status: "completed",
		Output: map[string]any{"backup_manifest_key": backupManifestKey, "objects": backupManifest.Stats.ObjectCount, "bytes": backupManifest.Stats.TotalBytes},
		Verification: map[string]any{
			"manifest_readable": true,
		},
	}, map[string]any{"phase": "backup_manifest_written", "version": record.Version, "backup_key": resultKey, "backup_manifest_key": backupManifestKey}); err != nil {
		return nil, "", err
	}
	integrity := s.validateBackupManifest(ctx, backupManifest)
	if integrity.Status != "ok" {
		_ = s.updateTaskActionProgress(context.Background(), task, "backup_validate_manifest", 4, total, taskActionUpdate{
			ID:     "validate_backup_manifest",
			Status: "failed",
			Verification: map[string]any{
				"backup_integrity": integrity.Status,
				"issues":           len(integrity.Issues),
			},
		}, nil)
		return nil, "", fmt.Errorf("backup integrity failed: %s", strings.Join(integrity.Issues, "; "))
	}
	_ = s.updateTaskActionProgress(ctx, task, "backup_done", total, total, taskActionUpdate{
		ID:     "validate_backup_manifest",
		Status: "completed",
		Output: map[string]any{"backup_manifest_key": backupManifestKey, "objects": integrity.Objects, "bytes": integrity.Bytes},
		Verification: map[string]any{
			"backup_integrity": integrity.Status,
		},
	}, map[string]any{"phase": "backup_done", "version": record.Version, "backup_key": resultKey, "backup_manifest_key": backupManifestKey, "integrity_status": integrity.Status})
	return tenantBackupTaskSummary(record, resultKey, backupManifestKey, integrity, backupManifest.Stats, false), resultKey, nil
}

func (s *TenantStore) resumeTenantBackupTask(ctx context.Context, task Task) (map[string]any, string, bool, error) {
	resultKey := taskCheckpointString(task, "backup_key")
	manifestKey := taskCheckpointString(task, "backup_manifest_key")
	if resultKey == "" || manifestKey == "" {
		return nil, "", false, nil
	}
	manifest, err := s.loadBackupManifest(ctx, manifestKey)
	if errors.Is(err, ErrNotFound) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	integrity := s.validateBackupManifest(ctx, manifest)
	if integrity.Status != "ok" {
		return nil, "", false, fmt.Errorf("backup integrity failed: %s", strings.Join(integrity.Issues, "; "))
	}
	record, err := s.loadTenantBackupRecord(ctx, resultKey)
	if errors.Is(err, ErrNotFound) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	return tenantBackupTaskSummary(record, resultKey, manifestKey, integrity, manifest.Stats, true), resultKey, true, nil
}

func tenantBackupTaskSummary(record TenantBackupRecord, resultKey string, manifestKey string, integrity BackupIntegrityReport, stats BackupManifestStats, resumed bool) map[string]any {
	out := map[string]any{
		"tenant_id":             record.TenantID,
		"version":               record.Version,
		"entities":              len(record.Snapshot.Entities),
		"edges":                 len(record.Snapshot.Edges),
		"has_config":            record.Config != nil,
		"has_policy":            record.SourcePolicy != nil,
		"result_key":            resultKey,
		"backup_key":            resultKey,
		"backup_manifest_key":   manifestKey,
		"backup_created":        record.CreatedAt,
		"backup_integrity":      integrity,
		"backup_manifest_stats": stats,
	}
	if resumed {
		out["resumed"] = true
	}
	return out
}

func (s *TenantStore) restoreSnapshotMatches(ctx context.Context, tenantID string, version int64) bool {
	manifest, err := s.CurrentManifest(ctx, tenantID)
	if err != nil {
		return false
	}
	return manifest.Version == version && manifest.SnapshotVersion == version && manifest.SnapshotCatalogKey != ""
}

func (s *TenantStore) tenantRestoreTask(ctx context.Context, task Task) (TenantRestoreReport, error) {
	total := taskProgressTotal(task.Type)
	backupKey := stringTaskParam(task.Params, "backup_key")
	if err := s.updateTaskActionProgress(ctx, task, "restore_load_backup", 1, total, taskActionUpdate{
		ID:     "load_backup",
		Status: "running",
		Input:  map[string]any{"backup_key": backupKey},
	}, map[string]any{"phase": "restore_load_backup", "backup_key": backupKey}); err != nil {
		return TenantRestoreReport{}, err
	}
	input, err := s.loadTenantBackupInput(ctx, backupKey)
	if err != nil {
		_ = s.updateTaskActionProgress(context.Background(), task, "restore_load_backup", 1, total, taskActionUpdate{ID: "load_backup", Err: err}, nil)
		return TenantRestoreReport{}, err
	}
	if err := s.updateTaskActionProgress(ctx, task, "restore_backup_loaded", 1, total, taskActionUpdate{
		ID:     "load_backup",
		Status: "completed",
		Output: map[string]any{"backup_key": backupKey, "backup_manifest_key": input.ManifestKey, "source_tenant_id": input.Record.TenantID, "version": input.Record.Version},
		Verification: map[string]any{
			"backup_integrity": input.Integrity.Status,
		},
	}, map[string]any{"phase": "restore_backup_loaded", "backup_key": backupKey, "backup_manifest_key": input.ManifestKey, "source_tenant_id": input.Record.TenantID, "version": input.Record.Version}); err != nil {
		return TenantRestoreReport{}, err
	}
	return s.restoreTenantBackupInputTask(ctx, task, backupKey, input)
}

func (s *TenantStore) restoreTenantBackupInputTask(ctx context.Context, task Task, backupKey string, input tenantBackupInput) (TenantRestoreReport, error) {
	total := taskProgressTotal(task.Type)
	overwrite := boolTaskParam(task.Params, "overwrite")
	dryRun := boolTaskParam(task.Params, "dry_run")
	record := input.Record
	if err := ValidateTenantID(task.TenantID); err != nil {
		return TenantRestoreReport{}, err
	}
	exists, err := s.tenantRestoreDataExists(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	baseReport := TenantRestoreReport{
		TenantID:          task.TenantID,
		BackupKey:         backupKey,
		BackupManifestKey: input.ManifestKey,
		SourceTenantID:    record.TenantID,
		Version:           record.Version,
		Entities:          len(record.Snapshot.Entities),
		Edges:             len(record.Snapshot.Edges),
		DryRun:            dryRun,
		TargetExists:      exists,
		BackupIntegrity:   input.Integrity,
		RestoredAt:        time.Now().UTC(),
	}
	if dryRun {
		_ = s.updateTaskActionProgress(ctx, task, "restore_dry_run_done", total, total, taskActionUpdate{
			ID:     "dry_run",
			Status: "completed",
			Output: map[string]any{"target_exists": exists, "version": record.Version},
			Verification: map[string]any{
				"backup_integrity": input.Integrity.Status,
			},
		}, map[string]any{"phase": "restore_dry_run_done", "backup_key": backupKey, "backup_integrity": input.Integrity.Status, "target_exists": exists})
		return baseReport, nil
	}
	if input.Integrity.Status == "error" {
		return TenantRestoreReport{}, fmt.Errorf("backup integrity failed: %s", strings.Join(input.Integrity.Issues, "; "))
	}
	snapshotWritten := taskCheckpointBool(task, "snapshot_written") && s.restoreSnapshotMatches(ctx, task.TenantID, record.Version)
	if exists && !snapshotWritten {
		if !overwrite {
			return TenantRestoreReport{}, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, task.TenantID)
		}
		if !taskCheckpointBool(task, "purged_existing") {
			if err := s.updateTaskActionProgress(ctx, task, "restore_purge_existing", 2, total, taskActionUpdate{
				ID:     "purge_existing",
				Status: "running",
				Input:  map[string]any{"tenant_id": task.TenantID},
			}, map[string]any{"phase": "restore_purge_existing", "backup_key": backupKey}); err != nil {
				return TenantRestoreReport{}, err
			}
			purge, err := s.PurgeTenant(ctx, task.TenantID, true)
			if err != nil {
				_ = s.updateTaskActionProgress(context.Background(), task, "restore_purge_existing", 2, total, taskActionUpdate{ID: "purge_existing", Err: err}, nil)
				return TenantRestoreReport{}, err
			}
			if err := s.updateTaskActionProgress(ctx, task, "restore_purge_done", 2, total, taskActionUpdate{
				ID:     "purge_existing",
				Status: "completed",
				Output: map[string]any{"deleted": purge.Deleted},
			}, map[string]any{"phase": "restore_purge_done", "backup_key": backupKey, "purged_existing": true}); err != nil {
				return TenantRestoreReport{}, err
			}
		}
	}
	snapshot := record.Snapshot
	manifest := Manifest{TenantID: task.TenantID, Version: snapshot.Version, SnapshotVersion: snapshot.Version}
	if !snapshotWritten {
		if err := s.updateTaskActionProgress(ctx, task, "restore_write_snapshot", 3, total, taskActionUpdate{
			ID:     "write_snapshot",
			Status: "running",
			Input:  map[string]any{"source_tenant_id": record.TenantID, "version": record.Version, "entities": len(snapshot.Entities), "edges": len(snapshot.Edges)},
		}, map[string]any{"phase": "restore_write_snapshot", "backup_key": backupKey, "source_tenant_id": record.TenantID, "version": record.Version}); err != nil {
			return TenantRestoreReport{}, err
		}
		unlock := s.lockTenant(task.TenantID)
		if err := s.acquireWriterLease(ctx, task.TenantID); err != nil {
			unlock()
			return TenantRestoreReport{}, err
		}
		catalog, err := s.putShardedSnapshot(ctx, task.TenantID, snapshot)
		if err != nil {
			unlock()
			return TenantRestoreReport{}, err
		}
		snapshotKey := s.snapshotKey(task.TenantID, snapshot.Version)
		snapshotRecord := snapshotRecord{LayoutVersion: CurrentObjectLayoutVersion, TenantID: task.TenantID, Snapshot: snapshot}
		if err := s.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, snapshotRecord); err != nil {
			unlock()
			return TenantRestoreReport{}, err
		}
		manifest = Manifest{
			LayoutVersion:      CurrentObjectLayoutVersion,
			TenantID:           task.TenantID,
			Version:            snapshot.Version,
			SnapshotKey:        snapshotKey,
			SnapshotCatalogKey: catalog.Key,
			SnapshotVersion:    snapshot.Version,
			UpdatedAt:          time.Now().UTC(),
		}
		if _, err := s.putManifestMeta(ctx, task.TenantID, manifest, ObjectMeta{Key: s.manifestKey(task.TenantID)}); err != nil {
			unlock()
			return TenantRestoreReport{}, err
		}
		unlock()
		if err := s.updateTaskActionProgress(ctx, task, "restore_snapshot_done", 3, total, taskActionUpdate{
			ID:     "write_snapshot",
			Status: "completed",
			Output: map[string]any{"version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey},
			Verification: map[string]any{
				"snapshot_matches": s.restoreSnapshotMatches(ctx, task.TenantID, record.Version),
			},
		}, map[string]any{"phase": "restore_snapshot_done", "backup_key": backupKey, "version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey, "snapshot_written": true}); err != nil {
			return TenantRestoreReport{}, err
		}
	} else {
		current, err := s.CurrentManifest(ctx, task.TenantID)
		if err != nil {
			return TenantRestoreReport{}, err
		}
		manifest = current
		if err := s.updateTaskActionProgress(ctx, task, "restore_snapshot_done", 3, total, taskActionUpdate{
			ID:     "write_snapshot",
			Status: "completed",
			Output: map[string]any{"version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey, "resumed": true},
			Verification: map[string]any{
				"snapshot_matches": true,
			},
		}, map[string]any{"phase": "restore_snapshot_done", "backup_key": backupKey, "version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey, "snapshot_written": true, "resumed": true}); err != nil {
			return TenantRestoreReport{}, err
		}
	}
	if !taskCheckpointBool(task, "metadata_written") {
		if err := s.updateTaskActionProgress(ctx, task, "restore_write_metadata", 4, total, taskActionUpdate{
			ID:     "write_metadata",
			Status: "running",
			Input:  map[string]any{"tenant_id": task.TenantID, "has_config": record.Config != nil, "has_policy": record.SourcePolicy != nil},
		}, map[string]any{"phase": "restore_write_metadata", "backup_key": backupKey, "version": manifest.Version}); err != nil {
			return TenantRestoreReport{}, err
		}
		metadata := restoredTenantMetadata(record.Metadata, task.TenantID)
		if err := s.clearTenantPurgeTombstone(ctx, task.TenantID); err != nil {
			return TenantRestoreReport{}, err
		}
		if err := s.putTenantMetadata(ctx, task.TenantID, metadata); err != nil {
			return TenantRestoreReport{}, err
		}
		if record.Config != nil {
			if err := validateTenantConfig(*record.Config); err != nil {
				return TenantRestoreReport{}, err
			}
			meta, err := s.putTenantConfigRecordWithMeta(ctx, task.TenantID, tenantConfigRecord{TenantID: task.TenantID, Config: *record.Config}, ObjectMeta{Key: s.tenantConfigKey(task.TenantID)})
			if err != nil {
				return TenantRestoreReport{}, err
			}
			s.setCachedTenantConfig(task.TenantID, *record.Config, true, meta)
		}
		if record.SourcePolicy != nil {
			normalized, err := graph.NormalizeSourcePolicy(*record.SourcePolicy)
			if err != nil {
				return TenantRestoreReport{}, err
			}
			meta, err := s.putSourcePolicyRecordWithMeta(ctx, task.TenantID, sourcePolicyRecord{TenantID: task.TenantID, SourcePolicy: normalized}, ObjectMeta{Key: s.sourcePolicyKey(task.TenantID)})
			if err != nil {
				return TenantRestoreReport{}, err
			}
			s.setCachedSourcePolicy(task.TenantID, normalized, true, meta)
		}
		if err := s.addTenantToRegistry(ctx, task.TenantID); err != nil {
			return TenantRestoreReport{}, err
		}
		if err := s.updateTaskActionProgress(ctx, task, "restore_metadata_done", 4, total, taskActionUpdate{
			ID:     "write_metadata",
			Status: "completed",
			Output: map[string]any{"tenant_id": task.TenantID, "has_config": record.Config != nil, "has_policy": record.SourcePolicy != nil},
			Verification: map[string]any{
				"metadata_written": true,
			},
		}, map[string]any{"phase": "restore_metadata_done", "backup_key": backupKey, "version": manifest.Version, "metadata_written": true}); err != nil {
			return TenantRestoreReport{}, err
		}
	}
	var catalogIndex IndexCatalog
	if taskCheckpointBool(task, "indexes_rebuilt") {
		if catalog, err := s.GetIndexCatalog(ctx, task.TenantID); err == nil && catalog.Version == manifest.Version {
			catalogIndex = catalog
		}
	}
	if catalogIndex.Version != manifest.Version {
		if err := s.updateTaskActionProgress(ctx, task, "restore_rebuild_indexes", 5, total, taskActionUpdate{
			ID:     "rebuild_indexes",
			Status: "running",
			Input:  map[string]any{"version": manifest.Version},
		}, map[string]any{"phase": "restore_rebuild_indexes", "backup_key": backupKey, "version": manifest.Version}); err != nil {
			return TenantRestoreReport{}, err
		}
		var err error
		catalogIndex, err = s.RebuildIndexes(ctx, task.TenantID)
		if err != nil {
			_ = s.updateTaskActionProgress(context.Background(), task, "restore_rebuild_indexes", 5, total, taskActionUpdate{ID: "rebuild_indexes", Err: err}, nil)
			return TenantRestoreReport{}, err
		}
		if err := s.updateTaskActionProgress(ctx, task, "restore_indexes_done", 6, total, taskActionUpdate{
			ID:     "rebuild_indexes",
			Status: "completed",
			Output: map[string]any{"index_catalog_version": catalogIndex.Version},
			Verification: map[string]any{
				"catalog_version": catalogIndex.Version,
			},
		}, map[string]any{"phase": "restore_indexes_done", "backup_key": backupKey, "version": manifest.Version, "index_catalog_version": catalogIndex.Version, "indexes_rebuilt": true}); err != nil {
			return TenantRestoreReport{}, err
		}
	} else {
		_ = s.updateTaskActionProgress(ctx, task, "restore_indexes_done", 6, total, taskActionUpdate{
			ID:     "rebuild_indexes",
			Status: "completed",
			Output: map[string]any{"index_catalog_version": catalogIndex.Version, "resumed": true},
			Verification: map[string]any{
				"catalog_version": catalogIndex.Version,
			},
		}, map[string]any{"phase": "restore_indexes_done", "backup_key": backupKey, "version": manifest.Version, "index_catalog_version": catalogIndex.Version, "indexes_rebuilt": true})
	}
	restoreIntegrity := s.restoreIntegrityReport(ctx, task.TenantID)
	_ = s.updateTaskActionProgress(ctx, task, "restore_done", total, total, taskActionUpdate{
		ID:     "verify_restore",
		Status: "completed",
		Output: map[string]any{"version": manifest.Version, "index_catalog_version": catalogIndex.Version},
		Verification: map[string]any{
			"restore_integrity": restoreIntegrity.Status,
			"issues":            len(restoreIntegrity.Issues),
		},
	}, map[string]any{"phase": "restore_done", "backup_key": backupKey, "version": manifest.Version, "index_catalog_version": catalogIndex.Version, "restore_integrity": restoreIntegrity.Status})
	baseReport.Version = manifest.Version
	baseReport.IndexCatalogVersion = catalogIndex.Version
	baseReport.Overwrote = exists
	baseReport.RestoreIntegrity = restoreIntegrity
	baseReport.RestoredAt = time.Now().UTC()
	return baseReport, nil
}

type tenantBackupInput struct {
	Record      TenantBackupRecord
	ManifestKey string
	Integrity   BackupIntegrityReport
}

func (s *TenantStore) loadTenantBackupInput(ctx context.Context, backupKey string) (tenantBackupInput, error) {
	if _, _, ok := s.backupManifestIdentityFromKey(backupKey); ok {
		manifest, err := s.loadBackupManifest(ctx, backupKey)
		if err != nil {
			return tenantBackupInput{}, err
		}
		integrity := s.validateBackupManifest(ctx, manifest)
		record, err := s.loadTenantBackupRecord(ctx, manifest.BackupRecordKey)
		if err != nil {
			return tenantBackupInput{}, err
		}
		if record.Version != manifest.Version || record.TenantID != manifest.TenantID {
			return tenantBackupInput{}, fmt.Errorf("backup manifest does not match backup record")
		}
		return tenantBackupInput{Record: record, ManifestKey: backupKey, Integrity: integrity}, nil
	}
	record, err := s.loadTenantBackupRecord(ctx, backupKey)
	if err != nil {
		return tenantBackupInput{}, err
	}
	return tenantBackupInput{
		Record: record,
		Integrity: BackupIntegrityReport{
			Status:      "legacy",
			CheckedAt:   time.Now().UTC(),
			Objects:     1,
			ManifestKey: "",
			Issues:      []string{"backup manifest is missing; restored from legacy backup record only"},
		},
	}, nil
}

func (s *TenantStore) loadTenantBackupRecord(ctx context.Context, backupKey string) (TenantBackupRecord, error) {
	data, err := s.Objects.Get(ctx, backupKey)
	if err != nil {
		return TenantBackupRecord{}, err
	}
	resultTenantID, resultTaskID, ok := s.taskResultIdentityFromKey(backupKey)
	if !ok {
		return TenantBackupRecord{}, fmt.Errorf("invalid tenant backup key")
	}
	result, err := decodeParquetTaskResult(ctx, data, resultTenantID, resultTaskID)
	if err != nil {
		return TenantBackupRecord{}, err
	}
	var record TenantBackupRecord
	payload, err := json.Marshal(result)
	if err != nil {
		return TenantBackupRecord{}, err
	}
	if err := json.Unmarshal(payload, &record); err != nil {
		return TenantBackupRecord{}, err
	}
	if record.TenantID == "" || record.Snapshot.Version != record.Version {
		return TenantBackupRecord{}, fmt.Errorf("invalid tenant backup record")
	}
	return record, nil
}

func restoredTenantMetadata(metadata TenantMetadata, tenantID string) TenantMetadata {
	now := time.Now().UTC()
	metadata.TenantID = tenantID
	metadata.Status = TenantStatusActive
	metadata.DisabledAt = time.Time{}
	metadata.DeletedAt = time.Time{}
	metadata.CreatedAt = firstTime(metadata.CreatedAt, now)
	metadata.UpdatedAt = now
	return metadata
}

func (s *TenantStore) taskResultIdentityFromKey(key string) (string, string, bool) {
	prefix := path.Join(s.Prefix, "tenants") + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	tenantID, tail, ok := strings.Cut(rest, "/tasks/results/")
	if !ok || ValidateTenantID(tenantID) != nil {
		return "", "", false
	}
	name := path.Base(tail)
	if !strings.HasSuffix(name, ".parquet") {
		return "", "", false
	}
	taskID, err := url.PathUnescape(strings.TrimSuffix(name, ".parquet"))
	if err != nil || taskID == "" {
		return "", "", false
	}
	return tenantID, taskID, true
}

func (s *TenantStore) tenantRestoreDataExists(ctx context.Context, tenantID string) (bool, error) {
	return s.tenantDataExists(ctx, tenantID)
}
