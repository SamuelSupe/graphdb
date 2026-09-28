package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
)

func (s *TenantStore) finishAutomaticBackup(ctx context.Context, task Task, entry backupstore.Entry) (map[string]any, error) {
	result := map[string]any{}
	if boolTaskParam(task.Params, "restore_drill") {
		report, err := s.automaticBackupDrill(ctx, task, entry)
		if err != nil {
			return nil, fmt.Errorf("automatic restore drill: %w", err)
		}
		if !report.Recoverable {
			return nil, fmt.Errorf("automatic restore drill did not establish recoverability: %s", report.Proof.Message)
		}
		result["restore_drill"] = report
	} else if _, err := s.Backups.Verify(ctx, entry.BackupKey); err != nil {
		return nil, fmt.Errorf("verify uploaded backup: %w", err)
	}
	// Restore input already verifies the entire download; do not transfer it twice.
	result["verified_at"] = time.Now().UTC()
	deleted, err := s.retainObjectBackups(ctx, task, entry)
	if err != nil {
		return nil, err
	}
	result["retention_deleted"] = deleted
	return result, nil
}

func (s *TenantStore) automaticBackupDrill(ctx context.Context, task Task, entry backupstore.Entry) (TenantRestoreDrillReport, error) {
	if !acquireTaskSlot(ctx, s.taskResidentSlots) {
		return TenantRestoreDrillReport{}, ctx.Err()
	}
	defer releaseTaskSlot(s.taskResidentSlots)
	if admission, ok := ctx.Value(taskIngestAdmissionKey{}).(*taskExecutionAdmission); ok {
		if !admission.acquire(ctx) {
			return TenantRestoreDrillReport{}, ctx.Err()
		}
		defer admission.release()
	}
	task.Params = map[string]any{"backup_key": entry.BackupKey, "cleanup": true, "automatic": true, "target_tenant_id": "backup-verification"}
	return s.tenantRestoreDrillTask(ctx, task)
}

// Keep only a bounded newest set in memory. Listing or decoding errors stop
// retention, and the freshly verified backup is always retained.
func (s *TenantStore) retainObjectBackups(ctx context.Context, task Task, verified backupstore.Entry) (int, error) {
	keep := intTaskParam(task.Params, "keep_count")
	age := int64TaskParam(task.Params, "max_age_seconds")
	if keep < 0 || keep > 1000 || age < 0 || age > 10*365*86400 {
		return 0, fmt.Errorf("invalid automatic backup retention")
	}
	if keep == 0 && age == 0 {
		return 0, nil
	}
	var newest []backupstore.Entry
	walk := func(visit func(backupstore.Entry) (bool, error)) error {
		cursor := ""
		for {
			page, err := s.Backups.List(ctx, task.TenantID, cursor, 100)
			if err != nil {
				return err
			}
			for _, entry := range page.Backups {
				if entry.Automatic {
					if more, err := visit(entry); err != nil || !more {
						return err
					}
				}
			}
			if page.NextCursor == "" {
				return nil
			}
			if page.NextCursor <= cursor {
				return fmt.Errorf("backup retention cursor did not advance")
			}
			cursor = page.NextCursor
		}
	}
	newer := func(a, b backupstore.Entry) int {
		if order := b.CreatedAt.Compare(a.CreatedAt); order != 0 {
			return order
		}
		return strings.Compare(b.BackupKey, a.BackupKey)
	}
	if err := walk(func(entry backupstore.Entry) (bool, error) {
		if i, _ := slices.BinarySearchFunc(newest, entry, newer); i < max(1, keep) {
			newest = slices.Insert(newest, i, entry)
			newest = newest[:min(len(newest), max(1, keep))]
		}
		return true, nil
	}); err != nil {
		return 0, err
	}
	if len(newest) == 0 {
		return 0, fmt.Errorf("verified automatic backup missing from listing")
	}
	retained := make(map[string]bool, len(newest))
	for _, entry := range newest {
		retained[entry.BackupKey] = true
	}
	deleted := 0
	remove := func(entry backupstore.Entry) error {
		if entry.BackupKey == verified.BackupKey || entry.BackupKey == newest[0].BackupKey {
			return nil
		}
		if entry.TenantID != task.TenantID {
			return fmt.Errorf("retention checkpoint tenant mismatch")
		}
		if err := s.updateTaskProgress(ctx, task, "backup_retention", deleted, 0, map[string]any{"retention_pending": taskResult(entry)}); err != nil {
			return err
		}
		if err := s.Backups.DeleteAutomatic(ctx, entry); err != nil {
			if errors.Is(err, backupstore.ErrBackupInUse) {
				return nil
			}
			return err
		}
		deleted++
		return s.updateTaskProgress(ctx, task, "backup_retention", deleted, 0, map[string]any{"retention_pending": nil})
	}
	state := s.taskStateOrLocal(ctx, task)
	if raw := state.Checkpoint["retention_pending"]; raw != nil {
		var entry backupstore.Entry
		encoded, err := json.Marshal(raw)
		if err != nil {
			return deleted, err
		}
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return deleted, err
		}
		if err := remove(entry); err != nil {
			return deleted, err
		}
	}
	cutoff := time.Now().UTC().Add(-time.Duration(age) * time.Second)
	err := walk(func(entry backupstore.Entry) (bool, error) {
		if (keep > 0 && !retained[entry.BackupKey]) || (age > 0 && entry.CreatedAt.Before(cutoff)) {
			if err := remove(entry); err != nil {
				return false, err
			}
		}
		return deleted < 100, nil
	})
	return deleted, err
}
