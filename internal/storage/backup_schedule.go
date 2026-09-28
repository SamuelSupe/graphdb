package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

type BackupAutomationStatus struct {
	TaskID        string    `json:"task_id,omitempty"`
	NextRun       time.Time `json:"next_run,omitempty"`
	LastSuccess   time.Time `json:"last_success,omitempty"`
	LastBackupKey string    `json:"last_backup_key,omitempty"`
	LastDrill     time.Time `json:"last_drill,omitempty"`
	Failures      int       `json:"consecutive_failures"`
	LastError     string    `json:"last_error,omitempty"`
	// A planned task is written before launch; recovery uses the same identity.
	PlannedParams  map[string]any `json:"planned_params,omitempty"`
	ObservedTaskID string         `json:"observed_task_id,omitempty"`
}

func (s BackupAutomationStatus) Public() BackupAutomationStatus {
	s.PlannedParams, s.ObservedTaskID = nil, ""
	return s
}

func (s *TenantStore) backupScheduleKey(tenant string) string {
	return path.Join(s.tenantObjectPrefix(tenant), "config/backup-schedule.parquet")
}

func (s *TenantStore) BackupAutomationStatus(ctx context.Context, tenant string) (BackupAutomationStatus, error) {
	if err := ValidateTenantID(tenant); err != nil {
		return BackupAutomationStatus{}, err
	}
	data, err := s.Objects.Get(ctx, s.backupScheduleKey(tenant))
	if errors.Is(err, ErrNotFound) {
		return BackupAutomationStatus{}, nil
	}
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	value, err := decodeParquetTaskResult(ctx, data, tenant, "backup-schedule")
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	var state BackupAutomationStatus
	err = json.Unmarshal(encoded, &state)
	return state, err
}

func (s *TenantStore) saveBackupSchedule(ctx context.Context, tenant string, state BackupAutomationStatus) error {
	data, err := marshalParquetTaskResult(ctx, tenant, "backup-schedule", taskResult(state))
	if err != nil {
		return err
	}
	return s.putTenantGenerationObject(ctx, tenant, s.backupScheduleKey(tenant), data)
}

// ResetBackupAutomation abandons a retry checkpoint explicitly. No remote
// backup is deleted; an enabled policy captures a new snapshot on its next tick.
func (s *TenantStore) ResetBackupAutomation(ctx context.Context, tenant string) (BackupAutomationStatus, error) {
	if err := ValidateTenantID(tenant); err != nil {
		return BackupAutomationStatus{}, err
	}
	unlock, err := s.lockTenantForeground(ctx, tenant)
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	defer unlock()
	ctx, err = s.acquireAndBindWriterFence(ctx, tenant)
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	if err := s.EnsureTenantWritable(ctx, tenant); err != nil {
		return BackupAutomationStatus{}, err
	}
	s.taskMu.Lock()
	active := s.taskActive[taskActiveKey(tenant, TaskTypeTenantBackup)]
	s.taskMu.Unlock()
	if active.ID != "" {
		return BackupAutomationStatus{}, fmt.Errorf("%w: wait for or cancel backup task %q before resetting automation", ErrConflict, active.ID)
	}
	state, err := s.BackupAutomationStatus(ctx, tenant)
	if err != nil {
		return state, err
	}
	if state.TaskID != "" {
		previous, err := s.GetTask(ctx, tenant, state.TaskID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return state, err
		}
		if err == nil {
			if err := s.cleanupAutomaticBackupCapture(ctx, previous); err != nil {
				return state, err
			}
		}
	}
	state = state.Public()
	state.TaskID, state.LastError, state.Failures = "", "", 0
	state.NextRun = time.Time{}
	return state, s.saveBackupSchedule(ctx, tenant, state)
}

func (s *TenantStore) cleanupAutomaticBackupCapture(ctx context.Context, task Task) error {
	id := taskCheckpointString(task, "remote_backup_id")
	if id == "" || task.Type != TaskTypeTenantBackup || !boolTaskParam(task.Params, "automatic") {
		return nil
	}
	if !strings.HasPrefix(id, "scheduled-") || !taskTerminal(task.Status) {
		return fmt.Errorf("automatic capture is not eligible for cleanup")
	}
	err := s.deleteTenantGenerationObject(ctx, task.TenantID, s.taskResultKey(task.TenantID, id))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// ScheduleObjectBackup performs only local admission and state persistence.
// Capture, S3 traffic, verification and retention run in the bounded task pool.
func (s *TenantStore) ScheduleObjectBackup(ctx context.Context, tenant string, now time.Time) (BackupAutomationStatus, error) {
	if err := ValidateTenantID(tenant); err != nil {
		return BackupAutomationStatus{}, err
	}
	unlock, err := s.lockTenantForeground(ctx, tenant)
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	defer unlock()
	config, _, _, err := s.getTenantConfigForWrite(ctx, tenant)
	if err != nil {
		return BackupAutomationStatus{}, err
	}
	policy := backupPolicy(config.Backup)
	state, err := s.BackupAutomationStatus(ctx, tenant)
	if err != nil || (!*policy.Enabled && state.TaskID == "") {
		return state, err
	}
	ctx, err = s.acquireAndBindWriterFence(ctx, tenant)
	if err != nil {
		return state, err
	}
	if err := s.EnsureTenantWritable(ctx, tenant); err != nil {
		return state, err
	}
	s.taskMu.Lock()
	active := s.taskActive[taskActiveKey(tenant, TaskTypeTenantBackup)]
	s.taskMu.Unlock()
	if active.ID != "" {
		return state, nil
	}
	var previous Task
	if state.TaskID != "" {
		previous, err = s.GetTask(ctx, tenant, state.TaskID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return state, err
		}
		if err == nil {
			if !taskTerminal(previous.Status) {
				return state, nil
			}
			state.PlannedParams = nil
			if state.ObservedTaskID != previous.ID {
				state.ObservedTaskID = previous.ID
				if previous.Status == TaskStatusSucceeded {
					state.Failures, state.LastError = 0, ""
					state.LastSuccess = previous.FinishedAt
					state.LastBackupKey = stringTaskParam(previous.Result, "backup_key")
					if boolTaskParam(previous.Params, "restore_drill") {
						state.LastDrill = previous.FinishedAt
					}
					state.NextRun = previous.FinishedAt.Add(time.Duration(*policy.IntervalSeconds) * time.Second)
				} else {
					state.Failures = min(state.Failures+1, 64)
					state.LastError = previous.Error
					delay := *policy.RetryInitialSeconds
					for n := 1; n < state.Failures && delay < *policy.RetryMaxSeconds; n++ {
						delay = min(delay*2, *policy.RetryMaxSeconds)
					}
					state.NextRun = now.Add(time.Duration(delay) * time.Second)
				}
				if err := s.saveBackupSchedule(ctx, tenant, state); err != nil {
					return state, err
				}
			}
			if previous.Status == TaskStatusSucceeded {
				if err := s.cleanupAutomaticBackupCapture(ctx, previous); err != nil {
					return state, err
				}
			}
		} else if state.PlannedParams == nil {
			// A missing checkpoint is not permission to recapture a failed cycle
			// at a newer version. Surface corruption or external task deletion.
			return state, fmt.Errorf("scheduled backup task %q is missing", state.TaskID)
		}
	}
	if !*policy.Enabled || now.Before(state.NextRun) {
		return state, nil
	}
	if s.Backups == nil {
		return state, fmt.Errorf("automatic backup requires GRAPHDB_BACKUP_S3_BUCKET")
	}
	if state.PlannedParams == nil {
		id, err := newCommitID()
		if err != nil {
			return state, err
		}
		params := map[string]any{"destination": "object", "automatic": true,
			"keep_count": *policy.KeepCount, "max_age_seconds": *policy.MaxAgeSeconds,
			"restore_drill": *policy.RestoreDrillIntervalSeconds > 0 && !now.Before(state.LastDrill.Add(time.Duration(*policy.RestoreDrillIntervalSeconds)*time.Second)),
		}
		if state.Failures > 0 {
			params = retryTaskParams(previous)
			params["retry_of"] = previous.ID
		}
		state.TaskID, state.PlannedParams = id, params
		if err := s.saveBackupSchedule(ctx, tenant, state); err != nil {
			return state, err
		}
	}
	_, err = s.startTaskIDLocked(ctx, tenant, TaskTypeTenantBackup, cloneTaskParams(state.PlannedParams), false, state.TaskID)
	return state, err
}
