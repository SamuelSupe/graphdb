package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/backupstore"
)

func (s *TenantStore) tenantObjectBackupTask(ctx context.Context, task Task) (map[string]any, string, error) {
	if s.Backups == nil {
		return nil, "", fmt.Errorf("object backups are not configured")
	}
	// Keep the durable capture available to this task while GC and tenant purge
	// run. Normal commits remain free to advance the tenant during the upload.
	release, err := s.PinReadView(ctx, task.TenantID)
	if err != nil {
		return nil, "", err
	}
	defer release()
	backupID := taskCheckpointString(task, "remote_backup_id")
	if backupID == "" {
		backupID = task.ID
	}
	uri, err := s.Backups.URI(task.TenantID, backupID)
	if err != nil {
		return nil, "", err
	}
	resultKey := s.taskResultKey(task.TenantID, backupID)
	if err := s.updateTaskActionProgress(ctx, task, "backup_capture", 1, 5, taskActionUpdate{ID: "load_snapshot_metadata", Status: "running"}, map[string]any{
		"remote_backup_id": backupID, "source_result_key": resultKey,
	}); err != nil {
		return nil, "", err
	}
	// The identity is checkpointed before writing. A crash after the file rename
	// reuses that exact capture even if its subsequent checkpoint was not saved.
	record, err := s.loadTenantBackupRecord(ctx, resultKey)
	if errors.Is(err, ErrNotFound) {
		if taskCheckpointBool(task, "snapshot_captured") {
			return nil, "", fmt.Errorf("captured backup file is missing; cannot resume at another version")
		}
		_, record, _, err = s.captureTenantBackup(ctx, task.TenantID)
		if err == nil {
			err = s.putTaskResult(ctx, task.TenantID, backupID, taskResult(record))
		}
	}
	if err != nil {
		return nil, "", err
	}
	if record.TenantID != task.TenantID {
		return nil, "", fmt.Errorf("captured backup tenant mismatch")
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_captured", 2, 5, taskActionUpdate{ID: "load_snapshot_metadata", Status: "completed", Output: map[string]any{"version": record.Version}}, nil); err != nil {
		return nil, "", err
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_upload", 3, 5, taskActionUpdate{ID: "write_backup_record", Status: "completed", Output: map[string]any{"result_key": resultKey}}, map[string]any{
		"snapshot_captured": true, "version": record.Version, "backup_key": uri,
	}); err != nil {
		return nil, "", err
	}
	manifest := backupstore.Manifest{TenantID: record.TenantID, BackupID: backupID, Version: record.Version, CreatedAt: record.CreatedAt}
	entities, edges := len(record.Snapshot.Entities), len(record.Snapshot.Edges)
	// The durable file is the upload source; do not retain its decoded graph
	// for the duration of a potentially slow network transfer.
	record = TenantBackupRecord{}
	source, err := openFileReader(ctx, s.Objects, resultKey)
	if err != nil {
		return nil, "", err
	}
	defer source.Close()
	size, err := source.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, "", err
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_upload", 3, 5, taskActionUpdate{ID: "publish_object_backup", Status: "running", Input: map[string]any{"backup_key": uri}}, nil); err != nil {
		return nil, "", err
	}
	entry, err := s.Backups.Publish(ctx, manifest, io.NewSectionReader(source, 0, size))
	if err != nil {
		_ = s.updateTaskActionProgress(context.WithoutCancel(ctx), task, "backup_upload", 3, 5, taskActionUpdate{ID: "publish_object_backup", Err: err}, nil)
		return nil, "", err
	}
	if err := s.updateTaskActionProgress(ctx, task, "backup_done", 5, 5, taskActionUpdate{ID: "publish_object_backup", Status: "completed", Output: map[string]any{"backup_key": entry.BackupKey}}, map[string]any{
		"backup_key": entry.BackupKey, "remote_snapshot_sha256": entry.SHA256,
	}); err != nil {
		return nil, "", err
	}
	return map[string]any{
		"tenant_id": manifest.TenantID, "version": manifest.Version, "destination": "object",
		"backup_key": entry.BackupKey, "backup_manifest": entry.Manifest,
		"entities": entities, "edges": edges,
	}, resultKey, nil
}

func (s *TenantStore) ListObjectBackups(ctx context.Context, tenantID, cursor string, limit int) (backupstore.Page, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return backupstore.Page{}, err
	}
	if s.Backups == nil {
		return backupstore.Page{}, fmt.Errorf("object backups are not configured")
	}
	return s.Backups.List(ctx, tenantID, cursor, limit)
}

func (s *TenantStore) loadObjectBackupInput(ctx context.Context, uri string) (tenantBackupInput, error) {
	if s.Backups == nil {
		return tenantBackupInput{}, fmt.Errorf("object backups are not configured")
	}
	m, err := s.Backups.ReadManifest(ctx, uri)
	if err != nil {
		return tenantBackupInput{}, err
	}
	file, closeFile, err := s.backupStagingFile(ctx)
	if err != nil {
		return tenantBackupInput{}, err
	}
	defer closeFile()
	if err := s.Backups.Download(ctx, m, file); err != nil {
		return tenantBackupInput{}, err
	}
	result, err := decodeParquetTaskResultReader(ctx, file, m.TenantID, m.BackupID)
	if err != nil {
		return tenantBackupInput{}, err
	}
	record, err := tenantBackupRecordFromResult(result)
	if err != nil {
		return tenantBackupInput{}, err
	}
	if record.TenantID != m.TenantID || record.Version != m.Version || !record.CreatedAt.Equal(m.CreatedAt) {
		return tenantBackupInput{}, fmt.Errorf("object backup manifest does not match snapshot")
	}
	return tenantBackupInput{Record: record, ManifestKey: uri, SHA256: m.SHA256, Integrity: BackupIntegrityReport{
		Status: "ok", CheckedAt: time.Now().UTC(), Objects: 1, Bytes: m.Bytes, ManifestKey: uri,
	}}, nil
}

func (s *TenantStore) backupStagingFile(ctx context.Context) (*os.File, func(), error) {
	files := s.localFileStore()
	if files == nil {
		return nil, nil, fmt.Errorf("object restore requires local disk storage")
	}
	release, err := files.beginLifecycleOperation(ctx)
	if err != nil {
		return nil, nil, err
	}
	unlock, err := files.lockDirectoryIO(ctx)
	if err != nil {
		release()
		return nil, nil, err
	}
	defer unlock()
	if err := files.ensureSafeParent(filepath.Join(files.root, ".tmp-backup")); err != nil {
		release()
		return nil, nil, err
	}
	file, err := os.CreateTemp(files.root, ".tmp-backup-*")
	if err != nil {
		release()
		return nil, nil, err
	}
	// An unlinked staging file survives an overwrite of the target tenant and
	// is reclaimed by the OS on cancellation, normal close, or process death.
	if err := os.Remove(file.Name()); err != nil {
		file.Close()
		release()
		return nil, nil, err
	}
	return file, func() { file.Close(); release() }, nil
}
