package storage

import (
	"context"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) compactTask(ctx context.Context, task Task) (map[string]any, string, error) {
	total := taskProgressTotal(task.Type)
	unlock := s.lockTenant(task.TenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, task.TenantID)
	if err != nil {
		return nil, "", err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, task.TenantID); err != nil {
		return nil, "", err
	}
	loaded, err := s.loadForWriteLocked(ctx, task.TenantID)
	if err != nil {
		return nil, "", err
	}
	g := loaded.Graph
	current := loaded.Manifest
	snapshot := g.Snapshot()
	version := snapshot.Version
	if manifestCompacted(current, version) {
		_ = s.updateTaskActionProgress(ctx, task, "compact_done", total, total, taskActionUpdate{
			ID:     "publish_manifest",
			Status: "completed",
			Output: map[string]any{"version": current.Version, "snapshot_catalog_key": current.SnapshotCatalogKey, "resumed": true},
			Verification: map[string]any{
				"manifest_version": current.Version,
				"snapshot_version": current.SnapshotVersion,
			},
		}, map[string]any{"version": current.Version, "snapshot_catalog_key": current.SnapshotCatalogKey})
		return taskResult(current), "", nil
	}
	dataMD5 := loaded.DataMD5
	if dataMD5 == "" {
		dataMD5, err = g.ContentMD5()
		if err != nil {
			return nil, "", err
		}
	}
	if err := s.updateTaskActionProgress(ctx, task, "compact_load_current", 1, total, taskActionUpdate{
		ID:     "load_current",
		Status: "completed",
		Output: map[string]any{
			"version":          version,
			"commit_keys":      len(current.CommitKeys),
			"commit_segments":  len(current.CommitSegments),
			"snapshot_version": current.SnapshotVersion,
		},
	}, map[string]any{"version": version}); err != nil {
		return nil, "", err
	}
	catalog, err := s.compactTaskSnapshotCatalog(ctx, task, snapshot, total)
	if err != nil {
		return nil, "", err
	}
	snapshotKey := s.snapshotKey(task.TenantID, snapshot.Version)
	if err := s.compactTaskSnapshotRecord(ctx, task, snapshotKey, snapshot, total); err != nil {
		return nil, "", err
	}
	manifest := Manifest{
		LayoutVersion:      CurrentObjectLayoutVersion,
		TenantID:           task.TenantID,
		Version:            snapshot.Version,
		SnapshotKey:        snapshotKey,
		SnapshotCatalogKey: catalog.Key,
		SnapshotVersion:    snapshot.Version,
		UpdatedAt:          time.Now().UTC(),
		DataMD5:            dataMD5,
	}
	if err := s.updateTaskActionProgress(ctx, task, "compact_publish_manifest", total-1, total, taskActionUpdate{
		ID:     "publish_manifest",
		Status: "running",
		Input:  map[string]any{"version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey},
	}, map[string]any{"version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey}); err != nil {
		return nil, "", err
	}
	meta, err := s.putManifestMeta(ctx, task.TenantID, manifest, loaded.Meta)
	if err != nil {
		s.deleteWriteCache(task.TenantID)
		_ = s.updateTaskActionProgress(context.WithoutCancel(ctx), task, "compact_publish_manifest", total-1, total, taskActionUpdate{
			ID:  "publish_manifest",
			Err: err,
		}, nil)
		return nil, "", err
	}
	s.setWriteCache(task.TenantID, loadedGraph{Graph: g, Manifest: manifest, Meta: meta, DataMD5: dataMD5})
	_ = s.updateTaskActionProgress(ctx, task, "compact_done", total, total, taskActionUpdate{
		ID:     "publish_manifest",
		Status: "completed",
		Output: map[string]any{"version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey},
		Verification: map[string]any{
			"manifest_version": manifest.Version,
			"snapshot_version": manifest.SnapshotVersion,
		},
	}, map[string]any{"phase": "compact_done", "version": manifest.Version, "snapshot_key": manifest.SnapshotKey, "snapshot_catalog_key": manifest.SnapshotCatalogKey})
	return taskResult(manifest), "", nil
}

func (s *TenantStore) compactTaskSnapshotCatalog(ctx context.Context, task Task, snapshot graph.Snapshot, total int) (ShardedSnapshotCatalog, error) {
	current := s.taskStateOrLocal(ctx, task)
	catalogKey := taskCheckpointString(current, "snapshot_catalog_key")
	if taskActionCompleted(current, "write_snapshot_catalog") && s.snapshotCatalogMatches(ctx, task.TenantID, catalogKey, snapshot.Version) {
		catalog, err := s.getShardedSnapshotCatalog(ctx, task.TenantID, catalogKey)
		if err == nil {
			_ = s.updateTaskActionProgress(ctx, task, "compact_snapshot_catalog_done", 2, total, taskActionUpdate{
				ID:     "write_snapshot_catalog",
				Status: "completed",
				Output: map[string]any{"snapshot_catalog_key": catalog.Key, "version": catalog.Version, "resumed": true},
				Verification: map[string]any{
					"catalog_version": catalog.Version,
					"entity_pages":    len(catalog.EntityPages),
					"edge_shards":     len(catalog.EdgeShards),
				},
			}, map[string]any{"snapshot_catalog_key": catalog.Key})
			return catalog, nil
		}
	}
	if err := s.updateTaskActionProgress(ctx, task, "compact_write_snapshot_catalog", 2, total, taskActionUpdate{
		ID:     "write_snapshot_catalog",
		Status: "running",
		Input:  map[string]any{"version": snapshot.Version, "entities": len(snapshot.Entities), "edges": len(snapshot.Edges)},
	}, nil); err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	catalog, err := s.putShardedSnapshot(ctx, task.TenantID, snapshot)
	if err != nil {
		_ = s.updateTaskActionProgress(context.WithoutCancel(ctx), task, "compact_write_snapshot_catalog", 2, total, taskActionUpdate{ID: "write_snapshot_catalog", Err: err}, nil)
		return ShardedSnapshotCatalog{}, err
	}
	if err := s.updateTaskActionProgress(ctx, task, "compact_snapshot_catalog_done", 2, total, taskActionUpdate{
		ID:     "write_snapshot_catalog",
		Status: "completed",
		Output: map[string]any{"snapshot_catalog_key": catalog.Key, "version": catalog.Version},
		Verification: map[string]any{
			"catalog_version": catalog.Version,
			"entity_pages":    len(catalog.EntityPages),
			"edge_shards":     len(catalog.EdgeShards),
		},
	}, map[string]any{"snapshot_catalog_key": catalog.Key}); err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	return catalog, nil
}

func (s *TenantStore) compactTaskSnapshotRecord(ctx context.Context, task Task, snapshotKey string, snapshot graph.Snapshot, total int) error {
	current := s.taskStateOrLocal(ctx, task)
	if taskActionCompleted(current, "write_snapshot_record") && s.snapshotRecordMatches(ctx, snapshotKey, task.TenantID, snapshot.Version) {
		return s.updateTaskActionProgress(ctx, task, "compact_snapshot_record_done", 3, total, taskActionUpdate{
			ID:     "write_snapshot_record",
			Status: "completed",
			Output: map[string]any{"snapshot_key": snapshotKey, "version": snapshot.Version, "resumed": true},
			Verification: map[string]any{
				"snapshot_version": snapshot.Version,
			},
		}, map[string]any{"snapshot_key": snapshotKey})
	}
	if err := s.updateTaskActionProgress(ctx, task, "compact_write_snapshot_record", 3, total, taskActionUpdate{
		ID:     "write_snapshot_record",
		Status: "running",
		Input:  map[string]any{"snapshot_key": snapshotKey, "version": snapshot.Version},
	}, nil); err != nil {
		return err
	}
	record := snapshotRecord{LayoutVersion: CurrentObjectLayoutVersion, TenantID: task.TenantID, Snapshot: snapshot}
	if err := s.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, record); err != nil {
		_ = s.updateTaskActionProgress(context.WithoutCancel(ctx), task, "compact_write_snapshot_record", 3, total, taskActionUpdate{ID: "write_snapshot_record", Err: err}, nil)
		return err
	}
	return s.updateTaskActionProgress(ctx, task, "compact_snapshot_record_done", 3, total, taskActionUpdate{
		ID:     "write_snapshot_record",
		Status: "completed",
		Output: map[string]any{"snapshot_key": snapshotKey, "version": snapshot.Version},
		Verification: map[string]any{
			"snapshot_version": snapshot.Version,
		},
	}, map[string]any{"snapshot_key": snapshotKey})
}

func manifestCompacted(manifest Manifest, version int64) bool {
	return manifest.Version == version && manifest.SnapshotVersion == version && manifest.SnapshotKey != "" && manifest.SnapshotCatalogKey != "" && len(manifest.CommitKeys) == 0 && len(manifest.CommitSegments) == 0
}

func (s *TenantStore) snapshotCatalogMatches(ctx context.Context, tenantID string, key string, version int64) bool {
	if key == "" {
		return false
	}
	catalog, err := s.getShardedSnapshotCatalog(ctx, tenantID, key)
	return err == nil && catalog.Version == version
}

func (s *TenantStore) snapshotRecordMatches(ctx context.Context, key string, tenantID string, version int64) bool {
	if key == "" {
		return false
	}
	record, err := s.loadSnapshotRecord(ctx, key)
	return err == nil && record.TenantID == tenantID && record.Snapshot.Version == version
}
