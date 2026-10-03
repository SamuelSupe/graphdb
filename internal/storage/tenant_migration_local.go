package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func copyTenantMigrationStaged(ctx context.Context, source *TenantStore, sourceTenantID string, target *TenantStore, targetTenantID string, options TenantMigrationOptions, report TenantMigrationReport, manifest Manifest) (TenantMigrationReport, error) {
	resume, err := target.pauseLocalIngest(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	defer resume()
	unpin, err := target.lockReadViews(ctx, targetTenantID, true)
	if err != nil {
		return report, err
	}
	defer unpin()
	unlock, err := target.lockTenantMaintenance(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	defer unlock()
	report.TargetExists, err = target.tenantDataExists(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	if report.TargetExists && !options.Overwrite {
		return report, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, targetTenantID)
	}
	ctx, err = target.acquireAndBindWriterFence(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	report.TargetExists, err = target.tenantDataExists(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	if report.TargetExists && !options.Overwrite {
		return report, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, targetTenantID)
	}
	files := target.localFileStore()
	var dir string
	if files != nil {
		dir, err = files.newRestoreDirectory()
	} else {
		dir, err = os.MkdirTemp("", "graphdb-tenant-copy-")
	}
	if err != nil {
		return report, err
	}
	defer func() {
		if _, err := os.Lstat(filepath.Join(dir, "journal.json")); os.IsNotExist(err) {
			_ = removeRestoreDirectory(dir)
		}
	}()
	stageFiles := NewFileStore(filepath.Join(dir, "build"))
	stage := NewTenantStore(stageFiles, target.Prefix)
	stage.InstanceID = target.InstanceID
	lease, err := target.Objects.Get(ctx, target.writerLeaseKey(targetTenantID))
	if err != nil {
		return report, err
	}
	if err := stageFiles.Put(ctx, stage.writerLeaseKey(targetTenantID), lease); err != nil {
		return report, err
	}
	stagedReport, err := copyTenantObjects(ctx, source, sourceTenantID, stage, targetTenantID, TenantMigrationOptions{}, report, manifest)
	stagedReport.TargetExists = report.TargetExists
	if err != nil {
		return stagedReport, err
	}
	if err := ValidateTenantMigrationSource(ctx, stage, targetTenantID, nil); err != nil {
		return stagedReport, err
	}
	if files == nil {
		// Non-local object stores have no atomic directory publication. Staging
		// still keeps source and validation failures ahead of target replacement.
		stagedManifest, err := stage.CurrentManifest(ctx, targetTenantID)
		if err != nil {
			return stagedReport, err
		}
		_, err = copyTenantObjects(ctx, stage, targetTenantID, target, targetTenantID, options, report, stagedManifest)
		stagedReport.FinishedAt = mutationTime(ctx)
		return stagedReport, err
	}
	if err := target.addTenantToRegistry(ctx, targetTenantID); err != nil {
		return stagedReport, err
	}
	targetKey := strings.TrimSuffix(target.tenantObjectPrefix(targetTenantID), "/")
	unlockIO, err := files.beginDirectoryChange(ctx, targetKey)
	if err != nil {
		return stagedReport, err
	}
	defer unlockIO()
	defer target.invalidateLocalRestoredTenant(targetTenantID)
	err = files.publishTenantDirectory(ctx, dir, targetKey)
	stagedReport.FinishedAt = mutationTime(ctx)
	return stagedReport, err
}
