package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Initialization uses the same recoverable directory switch as restore. Until
// publication, neither a manifest nor active metadata can expose half a tenant.
func (s *TenantStore) publishLocalTenantLifecycle(ctx context.Context, tenantID string, requireEmpty bool, build func(*TenantStore) (TenantInfo, error)) (TenantInfo, error) {
	resume, err := s.pauseLocalIngest(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	defer resume()
	unpin, err := s.lockReadViews(ctx, tenantID, true)
	if err != nil {
		return TenantInfo{}, err
	}
	defer unpin()
	unlock, err := s.lockTenantMaintenance(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	defer unlock()
	if requireEmpty {
		if exists, err := s.tenantDataExists(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		} else if exists {
			return TenantInfo{}, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, tenantID)
		}
	} else {
		metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
		if err != nil {
			return TenantInfo{}, err
		}
		if configured && metadata.Status != TenantStatusDeleted {
			if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
				return TenantInfo{}, err
			}
			return s.tenantInfoFromMetadata(ctx, metadata, true)
		}
	}
	if err := s.prepareTenantCreateLease(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	files := s.localFileStore()
	dir, err := files.newRestoreDirectory()
	if err != nil {
		return TenantInfo{}, err
	}
	defer func() {
		if _, err := os.Lstat(filepath.Join(dir, "journal.json")); os.IsNotExist(err) {
			_ = removeRestoreDirectory(dir)
		}
	}()
	stageFiles := NewFileStore(filepath.Join(dir, "build"))
	stage := NewTenantStore(stageFiles, s.Prefix)
	stage.InstanceID = s.InstanceID
	// Only these records are needed to preserve create-on-legacy and reactivation
	// semantics. All untouched files are linked at publication, so task progress
	// and idempotency finalization written during the build are not rolled back.
	for _, key := range []string{s.writerLeaseKey(tenantID), s.manifestKey(tenantID), s.tenantMetadataKey(tenantID)} {
		data, err := files.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return TenantInfo{}, err
		}
		if err := stageFiles.Put(ctx, key, data); err != nil {
			return TenantInfo{}, err
		}
	}
	info, err := build(stage)
	if err != nil {
		return TenantInfo{}, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	unlockIO, err := files.lockDirectoryIOWeight(ctx, directoryIOCapacity)
	if err != nil {
		return TenantInfo{}, err
	}
	defer s.invalidateLocalRestoredTenant(tenantID)
	defer unlockIO()
	if err := files.syncPendingDirectories(); err != nil {
		return TenantInfo{}, err
	}
	targetKey := strings.TrimSuffix(s.tenantObjectPrefix(tenantID), "/")
	target, err := files.path(targetKey)
	if err != nil {
		return TenantInfo{}, err
	}
	incoming := filepath.Join(dir, "build", filepath.FromSlash(targetKey))
	if err := linkTenantTree(ctx, target, incoming, true); err != nil {
		return TenantInfo{}, err
	}
	err = files.publishTenantDirectory(ctx, dir, targetKey)
	if err != nil {
		return TenantInfo{}, err
	}
	return info, nil
}
