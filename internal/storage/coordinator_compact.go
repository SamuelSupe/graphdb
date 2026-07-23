package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *TenantStore) publishCoordinatedCompaction(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	snapshotKey string,
	snapshotCatalogKey string,
	dataMD5 string,
) (Manifest, ObjectMeta, error) {
	baseToken, err := parseCoordinatedHeadToken(loaded.Meta)
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	snapshotVersion := loaded.Manifest.Version
	attempts := s.CoordinatorRetryLimit + 1
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 0; attempt < attempts; attempt++ {
		current, currentMeta, err := s.getManifest(ctx, tenantID)
		if err != nil {
			return Manifest{}, ObjectMeta{}, err
		}
		currentToken, err := parseCoordinatedHeadToken(currentMeta)
		if err != nil {
			return Manifest{}, ObjectMeta{}, err
		}
		if baseToken.Revision != 0 &&
			(baseToken.Generation != currentToken.Generation || currentToken.Revision == 0) {
			return Manifest{}, ObjectMeta{}, fmt.Errorf(
				"%w: tenant %q generation changed while compacting",
				ErrConflict, tenantID,
			)
		}
		if current.Version < snapshotVersion {
			return Manifest{}, ObjectMeta{}, fmt.Errorf(
				"%w: tenant %q head version %d is behind snapshot version %d",
				ErrConflict, tenantID, current.Version, snapshotVersion,
			)
		}
		if current.SnapshotVersion >= snapshotVersion &&
			current.SnapshotKey != "" &&
			current.SnapshotCatalogKey != "" {
			return current, currentMeta, nil
		}

		candidate := current
		candidate.LayoutVersion = CurrentObjectLayoutVersion
		candidate.TenantID = tenantID
		candidate.SnapshotKey = snapshotKey
		candidate.SnapshotCatalogKey = snapshotCatalogKey
		candidate.SnapshotVersion = snapshotVersion
		candidate.CommitSegments, candidate.CommitKeys, err = s.commitTailAfterVersion(
			ctx, tenantID, current, snapshotVersion,
		)
		if err != nil {
			return Manifest{}, ObjectMeta{}, err
		}
		candidate.UpdatedAt = time.Now().UTC()
		if candidate.Version == snapshotVersion {
			candidate.DataMD5 = dataMD5
		}

		meta, err := s.putManifestMeta(ctx, tenantID, candidate, currentMeta)
		if err == nil {
			return candidate, meta, nil
		}
		if !errors.Is(err, ErrConflict) {
			return Manifest{}, ObjectMeta{}, err
		}
		if attempt+1 >= attempts {
			break
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return Manifest{}, ObjectMeta{}, err
		}
	}
	return Manifest{}, ObjectMeta{}, fmt.Errorf(
		"%w: tenant %q head changed while publishing compacted snapshot after %d attempts",
		ErrWriteConflict, tenantID, attempts,
	)
}

func (s *TenantStore) commitTailAfterVersion(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	version int64,
) ([]CommitSegmentRef, []string, error) {
	segments := make([]CommitSegmentRef, 0, len(manifest.CommitSegments))
	for _, ref := range manifest.CommitSegments {
		lastVersion := ref.LastVersion
		if lastVersion <= 0 {
			items, err := s.loadCommitSegment(ctx, tenantID, ref)
			if err != nil {
				return nil, nil, err
			}
			if len(items) == 0 {
				return nil, nil, fmt.Errorf("empty commit segment %q", ref.Key)
			}
			lastVersion = items[len(items)-1].Commit.Version
		}
		if lastVersion > version {
			segments = append(segments, ref)
		}
	}

	keys := make([]string, 0, len(manifest.CommitKeys))
	for _, key := range manifest.CommitKeys {
		commitVersion, _, ok := commitIdentityFromKey(key)
		if !ok {
			return nil, nil, fmt.Errorf("invalid commit key %q", key)
		}
		if commitVersion > version {
			keys = append(keys, key)
		}
	}
	return segments, keys, nil
}
