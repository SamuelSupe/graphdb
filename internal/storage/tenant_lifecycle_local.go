package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
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
	// Check the live directory before building an empty stage; the stage does
	// not yet contain the existing graph objects needed to detect a lost head.
	liveManifest, liveManifestMeta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if err := s.prepareTenantCreateLease(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	files := s.localFileStore()
	dir, err := files.newRestoreDirectory()
	if err != nil {
		return TenantInfo{}, err
	}
	defer files.scheduleRestoreCleanup(dir)
	stageFiles := NewFileStore(filepath.Join(dir, "build"))
	stage := NewTenantStore(stageFiles, s.Prefix)
	stage.InstanceID = s.InstanceID
	// Only these records are needed to preserve create-on-legacy and reactivation
	// semantics. All untouched files are linked at publication, so task progress
	// and idempotency finalization written during the build are not rolled back.
	for _, key := range []string{s.writerLeaseKey(tenantID), s.manifestKey(tenantID), s.tenantMetadataKey(tenantID)} {
		if key == s.tenantMetadataKey(tenantID) {
			metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
			if err != nil {
				return TenantInfo{}, err
			}
			if configured {
				data, err := marshalParquetTenantMetadata(ctx, metadata)
				if err != nil {
					return TenantInfo{}, err
				}
				if err := stageFiles.Put(ctx, key, data); err != nil {
					return TenantInfo{}, err
				}
			}
			continue
		}
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
	if !requireEmpty && liveManifestMeta.Exists {
		published, err := s.publishStagedTenantMetadata(ctx, tenantID, stage, liveManifest)
		if err != nil || published {
			return info, err
		}
	}
	targetKey := strings.TrimSuffix(s.tenantObjectPrefix(tenantID), "/")
	unlockMetadata, err := s.lockLocalTenantMetadata(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	defer unlockMetadata()
	unlockIO, err := files.beginDirectoryChange(ctx, targetKey)
	if err != nil {
		return TenantInfo{}, err
	}
	defer s.invalidateLocalRestoredTenant(tenantID)
	defer unlockIO()
	if err := files.syncPendingDirectories(); err != nil {
		return TenantInfo{}, err
	}
	target, err := files.path(targetKey)
	if err != nil {
		return TenantInfo{}, err
	}
	incoming := filepath.Join(dir, "build", filepath.FromSlash(targetKey))
	if err := linkTenantTree(ctx, target, incoming, true); err != nil {
		return TenantInfo{}, err
	}
	// The completed stage supersedes any intent from an interrupted lifecycle
	// write in the old tree. Keep that intent only in the rollback directory.
	if err := stage.Objects.Delete(ctx, stage.tenantMetadataPendingKey(tenantID)); err != nil {
		return TenantInfo{}, err
	}
	err = files.publishTenantDirectory(ctx, dir, targetKey)
	if err != nil {
		return TenantInfo{}, err
	}
	return info, nil
}

// An implicit tenant already has a published graph. Registering its management
// metadata needs one atomic file replacement, not a copy of every graph file.
// Templates and graph changes still use the journaled directory transaction.
func (s *TenantStore) publishStagedTenantMetadata(ctx context.Context, tenantID string, stage *TenantStore, live Manifest) (bool, error) {
	staged, _, err := stage.getManifest(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(staged, live) {
		return false, nil
	}
	objects, err := stage.Objects.List(ctx, stage.tenantObjectPrefix(tenantID))
	if err != nil {
		return false, err
	}
	for _, object := range objects {
		if object.Key != s.manifestKey(tenantID) && object.Key != s.writerLeaseKey(tenantID) && object.Key != s.tenantMetadataKey(tenantID) && object.Key != s.tenantMetadataBackupKey(tenantID) {
			return false, nil
		}
	}
	metadata, configured, _, err := stage.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if !configured {
		return false, nil
	}
	_, _, meta, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return false, err
	}
	ctx, err = s.bindCurrentWriterFence(ctx, tenantID)
	if err != nil {
		return false, err
	}
	_, err = s.putTenantMetadataWithMeta(ctx, tenantID, metadata, meta)
	return err == nil, err
}
