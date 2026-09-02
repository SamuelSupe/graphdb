package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"path"
	"strings"
	"time"
)

type restoreDrillOwnership struct {
	taskID      string
	objectCount int
	fingerprint string
}

type restoreDrillClaim struct {
	fence writerFenceRef
}

func (s *TenantStore) validateRestoreDrillTarget(targetPrefix string, targetTenantID string) error {
	targetRoot := cleanPrefix(targetPrefix)
	if targetRoot == "" {
		return fmt.Errorf("restore drill target prefix is required")
	}
	sourceTenantRoot := path.Join(s.Prefix, "tenants") + "/"
	targetTenantPrefix := path.Join(targetRoot, "tenants", targetTenantID) + "/"
	if targetRoot == s.Prefix || strings.HasPrefix(targetTenantPrefix, sourceTenantRoot) || strings.HasPrefix(sourceTenantRoot, targetTenantPrefix) {
		return fmt.Errorf("restore drill target must be isolated from the source tenant namespace")
	}
	return nil
}

func (s *TenantStore) claimRestoreDrillTarget(
	ctx context.Context,
	tenantID string,
) (claim restoreDrillClaim, returnErr error) {
	if err := s.requireEmptyRestoreDrillTarget(ctx, tenantID); err != nil {
		return restoreDrillClaim{}, err
	}
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return restoreDrillClaim{}, err
	}
	defer func() {
		if returnErr == nil {
			return
		}
		releaseCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 5*time.Second,
		)
		defer cancel()
		if err := s.releaseWriterLeaseForPurge(releaseCtx, tenantID); err != nil {
			returnErr = errors.Join(
				returnErr,
				fmt.Errorf("release failed restore drill claim: %w", err),
			)
		}
	}()
	lease, _, ok := s.getCachedWriterLeaseAny(tenantID)
	if !ok || lease.FenceToken == "" || lease.FenceEpoch <= 0 {
		return restoreDrillClaim{}, fmt.Errorf(
			"%w: restore drill target tenant %q has no writer fence",
			ErrLeaseHeld, tenantID,
		)
	}
	hasOtherObjects, err := s.restoreDrillTargetObjectMatches(
		ctx,
		tenantID,
		func(object ObjectInfo) bool { return object.Key != s.writerLeaseKey(tenantID) },
	)
	if err != nil {
		return restoreDrillClaim{}, err
	}
	if hasOtherObjects {
		return restoreDrillClaim{}, fmt.Errorf(
			"%w: restore drill target tenant %q changed while claiming ownership",
			ErrConflict, tenantID,
		)
	}
	claim = restoreDrillClaim{fence: writerFenceRef{
		ownerID: lease.OwnerID,
		token:   lease.FenceToken,
		epoch:   lease.FenceEpoch,
	}}
	return claim, nil
}

func (s *TenantStore) requireEmptyRestoreDrillTarget(ctx context.Context, tenantID string) error {
	hasObjects, err := s.restoreDrillTargetObjectMatches(
		ctx, tenantID, func(ObjectInfo) bool { return true },
	)
	if err != nil {
		return err
	}
	if hasObjects {
		return fmt.Errorf("%w: restore drill target tenant %q is not empty", ErrConflict, tenantID)
	}
	purged, err := s.tenantPurgeTombstoneExists(ctx, tenantID)
	if err != nil {
		return err
	}
	if purged {
		return fmt.Errorf("%w: restore drill target tenant %q has a purge tombstone", ErrConflict, tenantID)
	}
	tenants, err := s.ListManagedTenants(ctx)
	if err != nil {
		return err
	}
	for _, existing := range tenants {
		if existing == tenantID {
			return fmt.Errorf("%w: restore drill target tenant %q is registered", ErrConflict, tenantID)
		}
	}
	return nil
}

func (s *TenantStore) captureRestoreDrillOwnership(ctx context.Context, tenantID string, taskID string) (restoreDrillOwnership, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err != nil {
		return restoreDrillOwnership{}, fmt.Errorf("load restore drill ownership task: %w", err)
	}
	if task.ID != taskID || task.TenantID != tenantID || task.Type != TaskTypeTenantRestore || task.Status != TaskStatusSucceeded {
		return restoreDrillOwnership{}, fmt.Errorf("%w: restore drill ownership task mismatch", ErrConflict)
	}
	objectCount, fingerprint, err := s.restoreDrillObjectFingerprint(ctx, tenantID)
	if err != nil {
		return restoreDrillOwnership{}, err
	}
	return restoreDrillOwnership{
		taskID:      taskID,
		objectCount: objectCount,
		fingerprint: fingerprint,
	}, nil
}

func (s *TenantStore) cleanupOwnedRestoreDrillTarget(
	ctx context.Context,
	tenantID string,
	ownership restoreDrillOwnership,
	claim restoreDrillClaim,
	verifyOwnership bool,
) (TenantPurgeReport, bool, error) {
	unlock, err := s.lockTenantMaintenance(ctx, tenantID)
	if err != nil {
		return TenantPurgeReport{}, false, err
	}
	defer unlock()
	s.deleteCachedWriterLease(tenantID)
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return TenantPurgeReport{}, false, err
	}
	if err := s.ensureBoundWriterLease(ctx, tenantID, claim.fence); err != nil {
		return TenantPurgeReport{}, false, err
	}
	if verifyOwnership {
		current, err := s.captureRestoreDrillOwnership(ctx, tenantID, ownership.taskID)
		if err != nil {
			return TenantPurgeReport{}, false, err
		}
		if ownership.objectCount != current.objectCount || ownership.fingerprint != current.fingerprint {
			return TenantPurgeReport{}, false, fmt.Errorf("%w: restore drill target tenant %q changed after restore", ErrConflict, tenantID)
		}
	}
	report, err := s.cleanupClaimedRestoreDrillTargetLocked(ctx, tenantID, claim)
	return report, true, err
}

func (s *TenantStore) cleanupClaimedRestoreDrillTarget(
	ctx context.Context,
	tenantID string,
	claim restoreDrillClaim,
) (TenantPurgeReport, error) {
	unlock, err := s.lockTenantMaintenance(ctx, tenantID)
	if err != nil {
		return TenantPurgeReport{}, err
	}
	defer unlock()
	s.deleteCachedWriterLease(tenantID)
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return TenantPurgeReport{}, err
	}
	if err := s.ensureBoundWriterLease(ctx, tenantID, claim.fence); err != nil {
		return TenantPurgeReport{}, err
	}
	return s.cleanupClaimedRestoreDrillTargetLocked(ctx, tenantID, claim)
}

func (s *TenantStore) cleanupClaimedRestoreDrillTargetLocked(
	ctx context.Context,
	tenantID string,
	claim restoreDrillClaim,
) (TenantPurgeReport, error) {
	report := TenantPurgeReport{TenantID: tenantID}
	leaseKey := s.writerLeaseKey(tenantID)
	prefix := s.tenantObjectPrefix(tenantID)
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(prefix)
	}
	err := scanObjectPrefixFresh(ctx, s.Objects, prefix, func(objects []ObjectInfo) error {
		if err := s.ensureBoundWriterLease(ctx, tenantID, claim.fence); err != nil {
			return err
		}
		filtered := objects[:0]
		for _, object := range objects {
			if object.Key != leaseKey {
				filtered = append(filtered, object)
			}
		}
		deletedKeys, err := s.deleteTenantPurgePage(ctx, tenantID, filtered, 0)
		report.Deleted += len(deletedKeys)
		report.recordDeletedKeys(deletedKeys)
		return err
	})
	if err != nil {
		return report, err
	}
	residual, err := s.restoreDrillTargetObjectMatches(
		ctx,
		tenantID,
		func(object ObjectInfo) bool { return object.Key != leaseKey },
	)
	if err != nil {
		return report, err
	}
	if residual {
		return report, fmt.Errorf(
			"%w: restore drill target tenant %q changed during cleanup",
			ErrConflict, tenantID,
		)
	}
	if err := s.removeTenantFromRegistry(ctx, tenantID); err != nil {
		return report, err
	}
	if err := s.releaseWriterLeaseForPurge(ctx, tenantID); err != nil {
		return report, err
	}
	s.deleteWriteCache(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	s.deleteCachedTenantConfig(tenantID)
	s.deleteCachedSourcePolicy(tenantID)
	s.deleteCachedIndexCatalog(tenantID)
	s.deleteCachedTenantPurgeTombstone(tenantID)
	s.clearObjectKeyPrefix(s.tenantObjectPrefix(tenantID))
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(s.tenantObjectPrefix(tenantID))
	}
	return report, nil
}

func (s *TenantStore) restoreDrillObjectFingerprint(ctx context.Context, tenantID string) (int, string, error) {
	prefix := s.tenantObjectPrefix(tenantID)
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(prefix)
	}
	digest := sha256.New()
	count := 0
	err := scanObjectPrefixFresh(ctx, s.Objects, prefix, func(objects []ObjectInfo) error {
		for _, object := range objects {
			if err := ctx.Err(); err != nil {
				return err
			}
			relative := strings.TrimPrefix(object.Key, prefix)
			if strings.HasPrefix(relative, "control/") {
				continue
			}
			etag := object.ETag
			if etag == "" {
				_, meta, err := s.Objects.GetWithMeta(ctx, object.Key)
				if errors.Is(err, ErrNotFound) {
					return fmt.Errorf("%w: restore drill object %q disappeared", ErrConflict, object.Key)
				}
				if err != nil {
					return err
				}
				etag = meta.ETag
			}
			if etag == "" {
				return fmt.Errorf("%w: restore drill object %q has no ETag", ErrObjectStoreUnavailable, object.Key)
			}
			writeRestoreDrillFingerprintValue(digest, object.Key)
			writeRestoreDrillFingerprintValue(digest, etag)
			count++
		}
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return count, hex.EncodeToString(digest.Sum(nil)), nil
}

func (s *TenantStore) restoreDrillTargetObjectMatches(
	ctx context.Context,
	tenantID string,
	match func(ObjectInfo) bool,
) (bool, error) {
	prefix := s.tenantObjectPrefix(tenantID)
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(prefix)
	}
	return objectPrefixMatches(ctx, s.Objects, prefix, match)
}

func writeRestoreDrillFingerprintValue(digest hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write([]byte(value))
}
