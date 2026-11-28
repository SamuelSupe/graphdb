package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
)

type replicatedRestoreKey struct{}
type replicatedBackupKey struct{}
type replicatedImportKey struct{}
type taskProgressMutedKey struct{}
type restoreDrillSourceUsageKey struct{}

type replicatedBackupPayload struct {
	backupstore.Entry
	Automation map[string]any `json:"automation,omitempty"`
}

func objectBackupID(task Task) string {
	if id := taskCheckpointString(task, "remote_backup_id"); id != "" {
		return id
	}
	if boolTaskParam(task.Params, "automatic") {
		return "scheduled-" + task.ID
	}
	return task.ID
}

// ReplicatedTaskPreparation holds local state captured under the application
// read lock so network I/O, restore drills, and payload marshaling run outside
// the lock in Build.
type ReplicatedTaskPreparation struct {
	store             *TenantStore
	task              Task
	kind              string
	importKey         string
	importData        []byte
	backupManifest    backupstore.Manifest
	backupSource      fileReader
	backupSize        int64
	backupAutomatic   bool
	backupSourceUsage *TenantUsageReport
	restoreBackupKey  string
	remoteRestore     bool
	localRestoreInput tenantBackupInput
}

func (p *ReplicatedTaskPreparation) Close() error {
	if p == nil || p.backupSource == nil {
		return nil
	}
	err := p.backupSource.Close()
	p.backupSource = nil
	return err
}

// CaptureReplicatedTaskPreparation captures local state required to prepare a
// replicated task without performing remote object-store network I/O or restore
// drills while holding the caller's read lock.
func (s *TenantStore) CaptureReplicatedTaskPreparation(ctx context.Context, task Task) (*ReplicatedTaskPreparation, error) {
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
		return &ReplicatedTaskPreparation{
			store:      s,
			task:       task,
			kind:       "import",
			importKey:  key,
			importData: data,
		}, nil
	}
	if task.Type == TaskTypeTenantBackup && stringTaskParam(task.Params, "destination") == "object" {
		if s.Backups == nil {
			return nil, fmt.Errorf("object backups are not configured")
		}
		backupID := objectBackupID(task)
		key := s.taskResultKey(task.TenantID, backupID)
		record, err := s.loadTenantBackupRecord(ctx, key)
		if err != nil {
			return nil, err
		}
		source, err := openFileReader(ctx, s.Objects, key)
		if err != nil {
			return nil, err
		}
		size, err := source.Seek(0, io.SeekEnd)
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		automatic := boolTaskParam(task.Params, "automatic")
		prep := &ReplicatedTaskPreparation{
			store: s,
			task:  task,
			kind:  "object_backup",
			backupManifest: backupstore.Manifest{
				Automatic: automatic,
				TenantID:  record.TenantID,
				BackupID:  backupID,
				Version:   record.Version,
				CreatedAt: record.CreatedAt,
			},
			backupSource:    source,
			backupSize:      size,
			backupAutomatic: automatic,
		}
		if automatic && boolTaskParam(task.Params, "restore_drill") {
			usage, err := s.TenantUsage(ctx, task.TenantID)
			if err != nil {
				_ = source.Close()
				return nil, err
			}
			prep.backupSourceUsage = &usage
		}
		return prep, nil
	}
	if task.Type != TaskTypeTenantRestore && task.Type != TaskTypeTenantRestoreDrill {
		return &ReplicatedTaskPreparation{store: s, task: task}, nil
	}
	backupKey := stringTaskParam(task.Params, "backup_key")
	if strings.HasPrefix(backupKey, "s3://") {
		return &ReplicatedTaskPreparation{
			store:            s,
			task:             task,
			kind:             "restore",
			restoreBackupKey: backupKey,
			remoteRestore:    true,
		}, nil
	}
	// A new leader must reproduce the same transfer digest, including the
	// integrity report timestamp, when resuming a queued restore.
	input, err := s.loadTenantBackupInputForTenant(ReplicatedContext(ctx, task.ID, task.StartedAt), backupKey, requiredTenantRestoreSource(task))
	if err != nil {
		return nil, err
	}
	return &ReplicatedTaskPreparation{
		store:             s,
		task:              task,
		kind:              "restore",
		restoreBackupKey:  backupKey,
		localRestoreInput: input,
	}, nil
}

// Build executes any network I/O, restore drill verification, and JSON marshaling
// for a captured replicated task preparation.
func (p *ReplicatedTaskPreparation) Build(ctx context.Context) ([]byte, error) {
	if p == nil || p.kind == "" {
		return nil, nil
	}
	s := p.store
	task := p.task
	switch p.kind {
	case "import":
		digest := sha256Hex(p.importData)
		if expected := stringTaskParam(task.Checkpoint, importSourceDigestCheckpoint); expected != "" && expected != digest {
			return nil, fmt.Errorf("%w: import source %q differs from the admitted input", ErrInvalidArgument, p.importKey)
		}
		return json.Marshal(digest)
	case "object_backup":
		if s.Backups == nil {
			return nil, fmt.Errorf("object backups are not configured")
		}
		entry, err := s.Backups.Publish(ctx, p.backupManifest, io.NewSectionReader(p.backupSource, 0, p.backupSize))
		if err != nil {
			return nil, err
		}
		payload := replicatedBackupPayload{Entry: entry}
		if p.backupAutomatic {
			prepCtx := s.withProgressReporter(ReplicatedContext(ctx, task.ID, mutationTime(ctx)), MutedReporter{})
			if p.backupSourceUsage != nil {
				prepCtx = context.WithValue(prepCtx, restoreDrillSourceUsageKey{}, *p.backupSourceUsage)
			}
			automation, err := s.finishAutomaticBackup(prepCtx, task, entry)
			if err != nil {
				return nil, err
			}
			payload.Automation = automation
		}
		return json.Marshal(payload)
	case "restore":
		input := p.localRestoreInput
		if p.remoteRestore {
			var err error
			input, err = s.loadTenantBackupInputForTenant(ReplicatedContext(ctx, task.ID, task.StartedAt), p.restoreBackupKey, requiredTenantRestoreSource(task))
			if err != nil {
				return nil, err
			}
		}
		if input.Integrity.Status == "error" {
			return nil, fmt.Errorf("%w: restore backup integrity failed", ErrInvalidArgument)
		}
		if err := validateTenantRestoreSource(task, input.Record); err != nil {
			return nil, err
		}
		if err := s.checkRestoreDiskSpace(ctx, input.Record); err != nil {
			return nil, err
		}
		return json.Marshal(input)
	default:
		return nil, nil
	}
}

// PrepareReplicatedTask materializes and verifies a restore before proposal.
// Its bytes are majority-persisted in Raft; application never fetches S3.
func (s *TenantStore) PrepareReplicatedTask(ctx context.Context, task Task) ([]byte, error) {
	prep, err := s.CaptureReplicatedTaskPreparation(ctx, task)
	if err != nil {
		return nil, err
	}
	defer prep.Close()
	return prep.Build(ctx)
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
			var payload replicatedBackupPayload
			if err := decoder.Decode(&payload); err != nil {
				return Task{}, err
			}
			ctx = context.WithValue(ctx, replicatedBackupKey{}, payload)
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
	if taskTerminal(task.Status) {
		return nil
	}
	backupID := objectBackupID(task)
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
	backupID := objectBackupID(task)
	_, err := s.loadTenantBackupRecord(ctx, s.taskResultKey(task.TenantID, backupID))
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
