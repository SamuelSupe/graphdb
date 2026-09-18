package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (s *TenantStore) restoreLocalTenantBackupTask(ctx context.Context, task Task, backupKey string, input tenantBackupInput) (TenantRestoreReport, error) {
	if err := ValidateTenantID(task.TenantID); err != nil {
		return TenantRestoreReport{}, err
	}
	if input.Integrity.Status == "error" {
		return TenantRestoreReport{}, fmt.Errorf("backup integrity failed: %s", strings.Join(input.Integrity.Issues, "; "))
	}
	_, dataMD5, err := prepareTenantRestoreContext(input.Record, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	resumeIngest, err := s.pauseLocalIngest(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	defer resumeIngest()
	releaseViews, err := s.lockReadViews(ctx, task.TenantID, true)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	defer releaseViews()
	unlock, err := s.lockTenantMaintenance(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	defer unlock()
	exists, err := s.tenantRestoreDataExists(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	if exists && !boolTaskParam(task.Params, "overwrite") {
		current, err := s.CurrentManifest(ctx, task.TenantID)
		if err != nil {
			return TenantRestoreReport{}, err
		}
		if current.DataMD5 != dataMD5 || !s.restoreSnapshotCanResume(ctx, task, backupKey, input.Record) {
			return TenantRestoreReport{}, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, task.TenantID)
		}
	}
	if err := s.updateTaskProgress(ctx, task, "restore_stage_snapshot", 2, taskProgressTotal(task.Type), nil); err != nil {
		return TenantRestoreReport{}, err
	}
	files := s.localFileStore()
	dir, err := files.newRestoreDirectory()
	if err != nil {
		return TenantRestoreReport{}, err
	}
	// An interrupted pre-publication build owns no live data. Once journaled,
	// only the recovery routine may remove this directory.
	defer func() {
		if _, err := os.Lstat(filepath.Join(dir, "journal.json")); os.IsNotExist(err) {
			_ = removeRestoreDirectory(dir)
		}
	}()
	stageFiles := NewFileStore(filepath.Join(dir, "build"))
	stage := NewTenantStore(stageFiles, s.Prefix)
	stage.InstanceID = s.InstanceID
	// Constructor defaults enable per-entity files, unlike the service defaults.
	// Preserve the live index layout and pack budget during the staged rebuild.
	stage.WriteEntityRecords = s.WriteEntityRecords
	stage.UseEntityRecordsForRead = s.UseEntityRecordsForRead
	stage.EntityPagePackMaxBytes = s.EntityPagePackMaxBytes
	// Keeping the writer fence allows the original task to finalize after the
	// switch without adopting a new lease or resurrecting the old manifest.
	lease, err := files.Get(ctx, s.writerLeaseKey(task.TenantID))
	if err != nil {
		return TenantRestoreReport{}, err
	}
	if err := stageFiles.Put(ctx, s.writerLeaseKey(task.TenantID), lease); err != nil {
		return TenantRestoreReport{}, err
	}
	stagedTask := task
	stagedTask.Checkpoint = nil
	if input.SHA256 != "" {
		stagedTask.Checkpoint = map[string]any{"remote_snapshot_sha256": input.SHA256}
	}
	stagedTask.Params = map[string]any{"backup_key": backupKey}
	stagedTask.Status = TaskStatusRunning
	if err := stage.saveTask(ctx, stagedTask); err != nil {
		return TenantRestoreReport{}, err
	}
	report, err := stage.restoreTenantBackupInputTask(ctx, stagedTask, backupKey, input)
	if err != nil {
		return report, err
	}
	stagedTask, err = stage.getTaskObject(ctx, task.TenantID, task.ID)
	if err != nil {
		return report, err
	}
	stagedTask.Params = task.Params
	stagedTask.Checkpoint["local_restore_published"] = true
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := s.advanceLocalIngestGeneration(ctx, task.TenantID); err != nil {
		return report, err
	}
	generation, err := s.localIngestGeneration(ctx, task.TenantID)
	if err != nil {
		return report, err
	}
	report.TargetExists, report.Overwrote = exists, exists
	stagedTask.Checkpoint["local_restore_generation"] = generation
	stagedTask.Checkpoint["local_restore_report"] = taskResult(report)
	if err := s.addTenantToRegistry(ctx, task.TenantID); err != nil {
		return report, err
	}
	err = s.publishLocalTenantRestore(ctx, files, stage, dir, stagedTask)
	s.invalidateLocalRestoredTenant(task.TenantID)
	return report, err
}

// The checkpoint is installed with the restored directory. Retrying after a
// lost terminal task write must never install that snapshot a second time.
func (s *TenantStore) resumePublishedLocalRestore(ctx context.Context, task Task) (TenantRestoreReport, error) {
	ctx, release, err := s.ReadViewContext(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	defer release()
	generation, err := s.localIngestGeneration(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	if expected := taskCheckpointInt64(task, "local_restore_generation"); expected <= 0 || generation != expected {
		return TenantRestoreReport{}, fmt.Errorf("%w: restore was already published; its tenant generation cannot be resumed", ErrConflict)
	}
	data, err := json.Marshal(task.Checkpoint["local_restore_report"])
	var report TenantRestoreReport
	if err != nil || json.Unmarshal(data, &report) != nil || report.TenantID != task.TenantID ||
		report.BackupKey != stringTaskParam(task.Params, "backup_key") || report.RestoredAt.IsZero() {
		return TenantRestoreReport{}, fmt.Errorf("%w: published restore report is unavailable", ErrConflict)
	}
	manifest, err := s.CurrentManifest(ctx, task.TenantID)
	if err != nil {
		return TenantRestoreReport{}, err
	}
	if manifest.Version < report.Version {
		return TenantRestoreReport{}, fmt.Errorf("%w: tenant version precedes the published restore", ErrConflict)
	}
	return report, nil
}

func (s *TenantStore) publishLocalTenantRestore(ctx context.Context, files *FileStore, stage *TenantStore, dir string, task Task) error {
	unlock, err := files.lockDirectoryIOWeight(ctx, directoryIOCapacity)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := files.syncPendingDirectories(); err != nil {
		return err
	}
	targetKey := strings.TrimSuffix(s.tenantObjectPrefix(task.TenantID), "/")
	target, err := files.path(targetKey)
	if err != nil {
		return err
	}
	incoming := filepath.Join(dir, "build", filepath.FromSlash(targetKey))
	for _, name := range []string{"tasks", "backups"} {
		destination := filepath.Join(incoming, name)
		if err := os.RemoveAll(destination); err != nil {
			return err
		}
		if err := linkRestoreTree(ctx, filepath.Join(target, name), destination); err != nil {
			return err
		}
	}
	if err := stage.saveTask(ctx, task); err != nil {
		return err
	}
	return files.publishTenantDirectory(ctx, dir, targetKey)
}

// The caller holds the tenant view and the directory IO gate through publication.
func (files *FileStore) publishTenantDirectory(ctx context.Context, dir, targetKey string) error {
	err := files.publishRestoreDirectory(ctx, dir, targetKey)
	_, journalErr := os.Lstat(filepath.Join(dir, "journal.json"))
	r := files.runtime
	r.mu.Lock()
	if err != nil && !os.IsNotExist(journalErr) {
		// If rollback could not finish, further acknowledged writes might be
		// undone by startup recovery. Require reopening the directory first.
		r.restoreErr = fmt.Errorf("%w: restore recovery requires reopening the data directory: %v", ErrObjectStoreUnavailable, err)
	}
	r.generation++
	clear(r.etags)
	clear(r.manifests)
	r.manifestBytes = 0
	r.mu.Unlock()
	return err
}

func (s *TenantStore) invalidateLocalRestoredTenant(tenantID string) {
	s.deleteWriteCache(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	s.deleteCachedTenantConfig(tenantID)
	s.deleteCachedSourcePolicy(tenantID)
	s.deleteCachedIndexCatalog(tenantID)
	s.clearObjectKeyPrefix(s.tenantObjectPrefix(tenantID))
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(s.tenantObjectPrefix(tenantID))
	}
	files := s.localFileStore()
	files.changed(s.manifestKey(tenantID), "")
	files.changed(s.tenantMetadataKey(tenantID), "")
}
