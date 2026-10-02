package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
)

type replicatedRestoreKey struct{}
type replicatedBackupKey struct{}
type replicatedImportKey struct{}

// PrepareReplicatedTask materializes and verifies a restore before proposal.
// Its bytes are majority-persisted in Raft; application never fetches S3.
func (s *TenantStore) PrepareReplicatedTask(ctx context.Context, task Task) ([]byte, error) {
	if err := s.CheckTaskDiskSpace(ctx, task); err != nil {
		return nil, err
	}
	if task.Type == TaskTypeBulkImport {
		key := stringTaskParam(task.Params, "source_key")
		if err := s.validateImportSourceKey(task.TenantID, key); err != nil {
			return nil, err
		}
		data, err := s.Objects.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		digest := sha256Hex(data)
		if expected := stringTaskParam(task.Checkpoint, importSourceDigestCheckpoint); expected != "" && expected != digest {
			return nil, fmt.Errorf("import source %q differs from the admitted input", key)
		}
		return json.Marshal(digest)
	}
	if task.Type == TaskTypeTenantBackup && stringTaskParam(task.Params, "destination") == "object" {
		backupID := task.ID
		if boolTaskParam(task.Params, "automatic") {
			backupID = "scheduled-" + backupID
		}
		record, err := s.loadTenantBackupRecord(ctx, s.taskResultKey(task.TenantID, backupID))
		if err != nil {
			return nil, err
		}
		source, err := openFileReader(ctx, s.Objects, s.taskResultKey(task.TenantID, backupID))
		if err != nil {
			return nil, err
		}
		defer source.Close()
		size, err := source.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		if s.Backups == nil {
			return nil, fmt.Errorf("object backups are not configured")
		}
		entry, err := s.Backups.Publish(ctx, backupstore.Manifest{Automatic: boolTaskParam(task.Params, "automatic"), TenantID: record.TenantID, BackupID: backupID, Version: record.Version, CreatedAt: record.CreatedAt}, io.NewSectionReader(source, 0, size))
		if err != nil {
			return nil, err
		}
		return json.Marshal(entry)
	}
	if task.Type != TaskTypeTenantRestore && task.Type != TaskTypeTenantRestoreDrill {
		return nil, nil
	}
	// A new leader must reproduce the same transfer digest, including the
	// integrity report timestamp, when resuming a queued restore.
	input, err := s.loadTenantBackupInput(ReplicatedContext(ctx, task.ID, task.StartedAt), stringTaskParam(task.Params, "backup_key"))
	if err != nil {
		return nil, err
	}
	if input.Integrity.Status == "error" {
		return nil, fmt.Errorf("restore backup integrity failed")
	}
	if err := s.checkRestoreDiskSpace(ctx, input.Record); err != nil {
		return nil, err
	}
	return json.Marshal(input)
}

func (s *TenantStore) RunReplicatedTask(ctx context.Context, tenantID, id string, restore []byte) (Task, error) {
	var source io.Reader
	if len(restore) > 0 {
		source = bytes.NewReader(restore)
	}
	return s.RunReplicatedTaskFromReader(ctx, tenantID, id, source)
}

// RunReplicatedTaskFromReader consumes verified, majority-persisted input.
// Restore decoding still materializes the graph; the reader avoids joining
// all replicated transfer parts into a second full-size byte buffer.
func (s *TenantStore) RunReplicatedTaskFromReader(ctx context.Context, tenantID, id string, restore io.Reader) (Task, error) {
	task, err := s.getTaskObject(ctx, tenantID, id)
	if err != nil {
		return Task{}, err
	}
	if taskTerminal(task.Status) {
		return task, nil
	}
	if restore != nil {
		decoder := json.NewDecoder(restore)
		if task.Type == TaskTypeBulkImport {
			var digest string
			if err := decoder.Decode(&digest); err != nil {
				return Task{}, err
			}
			if len(digest) != 64 {
				return Task{}, fmt.Errorf("invalid replicated import source digest")
			}
			ctx = context.WithValue(ctx, replicatedImportKey{}, digest)
		} else if task.Type == TaskTypeTenantBackup {
			var entry backupstore.Entry
			if err := decoder.Decode(&entry); err != nil {
				return Task{}, err
			}
			ctx = context.WithValue(ctx, replicatedBackupKey{}, entry)
		} else {
			var input tenantBackupInput
			if err := decoder.Decode(&input); err != nil {
				return Task{}, err
			}
			ctx = context.WithValue(ctx, replicatedRestoreKey{}, input)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return Task{}, fmt.Errorf("replicated task input has trailing data: %v", err)
		}
	}
	task.Status = TaskStatusRunning
	task.UpdatedAt = mutationTime(ctx)
	inlineCtx, finish := s.inlineTask(ctx, task)
	defer finish()
	if err := s.saveTask(inlineCtx, task); err != nil {
		return Task{}, err
	}
	result, key, operationErr := s.runTaskOperation(inlineCtx, task)
	task.Status = TaskStatusSucceeded
	task.Phase = TaskStatusSucceeded
	task.Result = result
	task.ResultKey = key
	task.FinishedAt = mutationTime(ctx)
	task.UpdatedAt = task.FinishedAt
	if operationErr != nil {
		if ctx.Err() != nil {
			return Task{}, ctx.Err()
		}
		task.Status = TaskStatusFailed
		task.Phase = TaskStatusFailed
		task.Error = operationErr.Error()
	}
	if err := s.saveTask(inlineCtx, task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func (s *TenantStore) FailReplicatedTask(ctx context.Context, tenantID, id, message string) (Task, error) {
	task, err := s.getTaskObject(ctx, tenantID, id)
	if err != nil || taskTerminal(task.Status) {
		return task, err
	}
	task.Status = TaskStatusFailed
	task.Phase = TaskStatusFailed
	task.Error = message
	task.UpdatedAt = mutationTime(ctx)
	task.FinishedAt = task.UpdatedAt
	return task, s.saveTask(ctx, task)
}

func (s *TenantStore) CaptureReplicatedObjectBackup(ctx context.Context, tenantID, id string) error {
	task, err := s.getTaskObject(ctx, tenantID, id)
	if err != nil {
		return err
	}
	backupID := task.ID
	if boolTaskParam(task.Params, "automatic") {
		backupID = "scheduled-" + backupID
	}
	key := s.taskResultKey(tenantID, backupID)
	if _, err := s.loadTenantBackupRecord(ctx, key); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	_, record, _, err := s.captureTenantBackup(ctx, tenantID)
	if err != nil {
		return err
	}
	return s.putTaskResult(ctx, tenantID, backupID, taskResult(record))
}

func (s *TenantStore) ReplicatedObjectBackupCaptured(ctx context.Context, task Task) (bool, error) {
	backupID := task.ID
	if boolTaskParam(task.Params, "automatic") {
		backupID = "scheduled-" + backupID
	}
	_, err := s.loadTenantBackupRecord(ctx, s.taskResultKey(task.TenantID, backupID))
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
