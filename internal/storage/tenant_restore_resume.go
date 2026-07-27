package storage

import (
	"context"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) restoreSnapshotCanResume(
	ctx context.Context,
	task Task,
	backupKey string,
	record TenantBackupRecord,
) bool {
	if !s.restoreSnapshotMatches(ctx, task.TenantID, record.Version) {
		return false
	}
	if taskCheckpointBool(task, "snapshot_written") {
		return true
	}
	if taskActionStatus(task, "write_snapshot") != "running" ||
		taskCheckpointString(task, "backup_key") != backupKey ||
		taskCheckpointString(task, "source_tenant_id") != record.TenantID ||
		taskCheckpointInt64(task, "version") != record.Version {
		return false
	}
	return s.restoreSnapshotContentMatches(ctx, task.TenantID, record.Snapshot)
}

func (s *TenantStore) restoreSnapshotContentMatches(
	ctx context.Context,
	tenantID string,
	snapshot graph.Snapshot,
) bool {
	loaded, err := s.loadWithMeta(ctx, tenantID)
	if err != nil || loaded.Manifest.Version != snapshot.Version {
		return false
	}
	expected, err := graph.FromSnapshot(snapshot)
	if err != nil {
		return false
	}
	gotHash, err := loaded.Graph.ContentMD5()
	if err != nil {
		return false
	}
	wantHash, err := expected.ContentMD5()
	return err == nil && gotHash == wantHash
}
