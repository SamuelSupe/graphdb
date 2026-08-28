package storage

import (
	"context"
	"fmt"
)

func (s *TenantStore) purgeCoordinatedTenantCandidate(
	ctx context.Context,
	tenantID string,
) (TenantPurgeReport, error) {
	candidate, exists, _, err := s.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil {
		return TenantPurgeReport{}, err
	}
	if !exists {
		return TenantPurgeReport{}, ErrCoordinatorHeadMissing
	}
	report := TenantPurgeReport{TenantID: tenantID}
	candidateKey := s.coordinatedTenantCandidateKey(tenantID)
	err = scanObjectPrefix(
		ctx,
		s.Objects,
		s.tenantObjectPrefix(tenantID),
		func(objects []ObjectInfo) error {
			if err := s.ensureCoordinatedCandidateCurrent(
				ctx, tenantID, candidate,
			); err != nil {
				return err
			}
			filtered := objects[:0]
			for _, object := range objects {
				if object.Key != candidateKey {
					filtered = append(filtered, object)
				}
			}
			deletedKeys, err := s.deleteTenantPurgePage(
				ctx, tenantID, filtered, 0,
			)
			report.Deleted += len(deletedKeys)
			report.recordDeletedKeys(deletedKeys)
			return err
		},
	)
	if err != nil {
		return report, err
	}
	if err := s.ensureCoordinatedCandidateCurrent(
		ctx, tenantID, candidate,
	); err != nil {
		return report, err
	}
	residual, err := objectPrefixMatches(
		ctx,
		s.Objects,
		s.tenantObjectPrefix(tenantID),
		func(object ObjectInfo) bool { return object.Key != candidateKey },
	)
	if err != nil {
		return report, err
	}
	if residual {
		return report, fmt.Errorf(
			"%w: target tenant %q candidate data changed during purge",
			ErrConflict, tenantID,
		)
	}
	if err := s.removeTenantFromRegistry(ctx, tenantID); err != nil {
		return report, err
	}
	if err := s.completeCoordinatedTenantCandidate(
		ctx, tenantID, candidate,
	); err != nil {
		return report, err
	}
	report.Deleted++
	report.recordDeletedKeys([]string{candidateKey})
	s.clearTenantCachesAfterCandidatePurge(tenantID)
	return report, nil
}

func (s *TenantStore) ensureCoordinatedCandidateCurrent(
	ctx context.Context,
	tenantID string,
	expected coordinatedTenantCandidate,
) error {
	if _, exists, err := s.Coordinator.Head(ctx, tenantID); err != nil {
		return err
	} else if exists {
		return fmt.Errorf(
			"%w: tenant %q acquired a coordinator head during candidate purge",
			ErrConflict, tenantID,
		)
	}
	current, exists, _, err := s.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil {
		return err
	}
	if !exists || current != expected {
		return fmt.Errorf(
			"%w: target tenant %q lifecycle candidate changed",
			ErrConflict, tenantID,
		)
	}
	return nil
}

func (s *TenantStore) clearTenantCachesAfterCandidatePurge(tenantID string) {
	s.deleteWriteCache(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	s.deleteCachedTenantConfig(tenantID)
	s.deleteCachedSourcePolicy(tenantID)
	s.deleteCachedIndexCatalog(tenantID)
	s.deleteCachedRetrievalSnapshot(tenantID)
	s.deleteCachedWriterLease(tenantID)
	s.clearObjectKeyPrefix(s.tenantObjectPrefix(tenantID))
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(s.tenantObjectPrefix(tenantID))
	}
}
