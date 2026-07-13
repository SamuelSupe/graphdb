package storage

import (
	"context"
	"errors"
	"fmt"
)

func (s *TenantStore) directCommitPreparedPublished(ctx context.Context, tenantID string, record DirectCommitRecord) (published bool, decisive bool, err error) {
	targetVersion := record.Result.Version
	targetCommitID := record.Result.HeadCommitID
	if targetVersion < 1 || targetCommitID == "" {
		return false, false, fmt.Errorf("%w: prepared commit idempotency record has no commit identity", ErrConflict)
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	if manifest.Version < targetVersion {
		return false, true, nil
	}
	if manifest.Version == targetVersion {
		return manifest.HeadCommitID == targetCommitID, true, nil
	}
	if manifest.SnapshotVersion >= targetVersion {
		return false, false, nil
	}

	foundVersion := false
	for _, ref := range manifest.CommitSegments {
		if targetVersion < ref.FirstVersion || targetVersion > ref.LastVersion {
			continue
		}
		items, err := s.loadCommitSegment(ctx, tenantID, ref)
		if err != nil {
			return false, false, err
		}
		for _, item := range items {
			if item.Commit.Version != targetVersion {
				continue
			}
			foundVersion = true
			if item.Commit.ID == targetCommitID {
				return true, true, nil
			}
		}
	}
	for _, key := range manifest.CommitKeys {
		commit, err := s.getCommitObject(ctx, key)
		if err != nil {
			return false, false, err
		}
		if commit.Version != targetVersion {
			continue
		}
		foundVersion = true
		if commit.ID == targetCommitID {
			return true, true, nil
		}
	}
	if foundVersion {
		return false, true, nil
	}
	return false, false, fmt.Errorf("%w: current manifest does not identify commit version %d", ErrConflict, targetVersion)
}
