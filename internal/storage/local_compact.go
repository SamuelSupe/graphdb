package storage

import (
	"context"
	"fmt"
	"time"
)

func (s *TenantStore) publishLocalCompaction(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	snapshotKey string,
	snapshotCatalogKey string,
	dataMD5 string,
) (Manifest, ObjectMeta, error) {
	current, currentMeta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	snapshotVersion := loaded.Manifest.Version
	if current.Version < snapshotVersion {
		return Manifest{}, ObjectMeta{}, fmt.Errorf(
			"%w: tenant %q head version %d is behind snapshot version %d",
			ErrConflict, tenantID, current.Version, snapshotVersion,
		)
	}
	if current.SnapshotVersion >= snapshotVersion &&
		current.SnapshotKey != "" && current.SnapshotCatalogKey != "" {
		return current, currentMeta, nil
	}
	if current.Version == snapshotVersion {
		if !cachedManifestMatches(loaded, current, currentMeta) {
			return Manifest{}, ObjectMeta{}, fmt.Errorf(
				"%w: manifest changed while compacting tenant %q",
				ErrConflict, tenantID,
			)
		}
	} else if err := s.requireManifestDescendsFrom(
		ctx, tenantID, current, snapshotVersion, loaded.Manifest.HeadCommitID,
	); err != nil {
		return Manifest{}, ObjectMeta{}, err
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
	if candidate.Version == snapshotVersion {
		candidate.DataMD5 = dataMD5
	}
	candidate.UpdatedAt = time.Now().UTC()
	meta, err := s.putManifestMeta(ctx, tenantID, candidate, currentMeta)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return Manifest{}, ObjectMeta{}, err
	}
	s.updateWriteCacheAfterLocalCompaction(
		tenantID, loaded, current, currentMeta, candidate, meta,
	)
	return candidate, meta, nil
}

func (s *TenantStore) requireManifestDescendsFrom(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	version int64,
	commitID string,
) error {
	if commitID == "" {
		return fmt.Errorf(
			"%w: cannot verify tenant %q history at version %d",
			ErrConflict, tenantID, version,
		)
	}
	for _, key := range manifest.CommitKeys {
		keyVersion, keyCommitID, ok := commitIdentityFromKey(key)
		if ok && keyVersion == version {
			if keyCommitID == commitID {
				return nil
			}
			break
		}
	}
	for _, ref := range manifest.CommitSegments {
		if ref.FirstVersion > 0 && version < ref.FirstVersion {
			continue
		}
		if ref.LastVersion > 0 && version > ref.LastVersion {
			continue
		}
		items, err := s.loadCommitSegment(ctx, tenantID, ref)
		if err != nil {
			return err
		}
		for _, item := range items {
			if item.Commit.Version != version {
				continue
			}
			if item.Commit.ID == commitID {
				return nil
			}
			break
		}
	}
	return fmt.Errorf(
		"%w: tenant %q history changed at compacted version %d",
		ErrConflict, tenantID, version,
	)
}

func (s *TenantStore) updateWriteCacheAfterLocalCompaction(
	tenantID string,
	loaded loadedGraph,
	current Manifest,
	currentMeta ObjectMeta,
	candidate Manifest,
	meta ObjectMeta,
) {
	if candidate.Version == loaded.Manifest.Version {
		loaded.Manifest = candidate
		loaded.Meta = meta
		loaded.CommitTail = emptyCommitTailCache()
		loaded.CacheBytes = writeCacheBytesWithoutCommitTail(loaded)
		s.setWriteCache(tenantID, loaded)
		return
	}
	cached, ok := s.getWriteCache(tenantID)
	if !ok || !cachedManifestMatches(cached, current, currentMeta) {
		s.deleteWriteCache(tenantID)
		return
	}
	tail := compactedCommitTailCache(
		cached.CommitTail, current.CommitKeys, candidate.CommitKeys,
	)
	cached.Manifest = candidate
	cached.Meta = meta
	cached.DataMD5 = candidate.DataMD5
	cached.CacheBytes = addWriteCacheBytes(
		writeCacheBytesWithoutCommitTail(cached), tail.bytes,
	)
	cached.CommitTail = tail
	s.setWriteCache(tenantID, cached)
}

func compactedCommitTailCache(
	cache commitTailCache,
	currentKeys []string,
	remainingKeys []string,
) commitTailCache {
	if len(remainingKeys) == 0 {
		return emptyCommitTailCache()
	}
	if !cache.matches(currentKeys) {
		return commitTailCache{}
	}
	items := make([]commitSegmentItem, 0, len(remainingKeys))
	current := 0
	for _, key := range remainingKeys {
		for current < len(cache.items) && cache.items[current].Key != key {
			current++
		}
		if current == len(cache.items) {
			return commitTailCache{}
		}
		items = append(items, cache.items[current])
		current++
	}
	return buildCommitTailCache(items, remainingKeys)
}
