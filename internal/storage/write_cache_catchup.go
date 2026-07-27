package storage

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) catchUpWriteCache(
	ctx context.Context,
	tenantID string,
	cached loadedGraph,
	manifest Manifest,
	meta ObjectMeta,
) (loadedGraph, bool, error) {
	if cached.Graph == nil ||
		cached.Graph.Version != cached.Manifest.Version ||
		cached.Manifest.Version > manifest.Version ||
		manifest.SnapshotVersion > cached.Manifest.Version {
		return loadedGraph{}, false, nil
	}
	cachedToken, err := parseCoordinatedHeadToken(cached.Meta)
	if err != nil {
		return loadedGraph{}, false, nil
	}
	currentToken, err := parseCoordinatedHeadToken(meta)
	if err != nil ||
		cachedToken.Revision == 0 ||
		currentToken.Revision == 0 ||
		cachedToken.Generation != currentToken.Generation {
		return loadedGraph{}, false, nil
	}
	if cached.Manifest.Version == manifest.Version {
		previousTailBytes := cached.CommitTail.bytes
		if !cached.CommitTail.matches(manifest.CommitKeys) {
			if len(manifest.CommitKeys) == 0 {
				cached.CommitTail = emptyCommitTailCache()
			} else {
				cached.CommitTail = commitTailCache{}
			}
		}
		if cached.CacheBytes >= previousTailBytes {
			cached.CacheBytes = addWriteCacheBytes(
				cached.CacheBytes-previousTailBytes,
				cached.CommitTail.bytes,
			)
		}
		cached.Manifest = manifest
		cached.Meta = meta
		cached.DataMD5 = manifest.DataMD5
		return cached, true, nil
	}

	next := cached.Graph
	copied := false
	tailItems := map[string]commitSegmentItem{}
	tailKeys := make(map[string]struct{}, len(manifest.CommitKeys))
	for _, key := range manifest.CommitKeys {
		tailKeys[key] = struct{}{}
	}
	if cached.CommitTail.matches(cached.Manifest.CommitKeys) {
		for _, item := range cached.CommitTail.items {
			if _, wanted := tailKeys[item.Key]; wanted {
				tailItems[item.Key] = item
			}
		}
	}
	rememberTail := func(item commitSegmentItem) {
		if _, wanted := tailKeys[item.Key]; wanted {
			tailItems[item.Key] = item
		}
	}
	apply := func(key string, commit graph.Commit) error {
		if err := validateCommitObjectIdentity(key, commit); err != nil {
			return err
		}
		if commit.Version <= next.Version {
			return nil
		}
		if commit.Version != next.Version+1 {
			return fmt.Errorf(
				"non-contiguous commit version %d after cached graph version %d",
				commit.Version, next.Version,
			)
		}
		if !copied {
			next, _, err = next.ApplyCommitStorageCopyWithOptions(
				commit, graph.ApplyOptions{},
			)
			copied = true
			return err
		}
		return next.ApplyCommitInPlaceForStorage(commit)
	}

	for _, ref := range manifest.CommitSegments {
		if ref.LastVersion > 0 && ref.LastVersion <= next.Version {
			continue
		}
		items, err := s.loadCommitSegment(ctx, tenantID, ref)
		if err != nil {
			return loadedGraph{}, false, err
		}
		for _, item := range items {
			if item.Commit.TenantID != tenantID {
				return loadedGraph{}, false, errTenantCommitMismatch(
					tenantID, item.Key, item.Commit.TenantID,
				)
			}
			if err := apply(item.Key, item.Commit); err != nil {
				return loadedGraph{}, false, err
			}
			rememberTail(item)
		}
	}
	for _, key := range manifest.CommitKeys {
		version, _, ok := commitIdentityFromKey(key)
		if !ok {
			return loadedGraph{}, false, fmt.Errorf("invalid commit key %q", key)
		}
		if version <= next.Version {
			continue
		}
		commit, err := s.getCommitObject(ctx, key)
		if err != nil {
			return loadedGraph{}, false, err
		}
		if commit.TenantID != tenantID {
			return loadedGraph{}, false, errTenantCommitMismatch(
				tenantID, key, commit.TenantID,
			)
		}
		if err := apply(key, commit); err != nil {
			return loadedGraph{}, false, err
		}
		rememberTail(commitSegmentItem{Key: key, Commit: commit})
	}
	if next.Version != manifest.Version {
		return loadedGraph{}, false, nil
	}
	looseCommits := make(
		[]commitSegmentItem, 0, len(manifest.CommitKeys),
	)
	for _, key := range manifest.CommitKeys {
		if item, ok := tailItems[key]; ok {
			looseCommits = append(looseCommits, item)
		}
	}
	commitTail := buildCommitTailCache(
		looseCommits, manifest.CommitKeys,
	)
	graphCacheBytes := cached.CacheBytes
	if graphCacheBytes >= cached.CommitTail.bytes {
		graphCacheBytes -= cached.CommitTail.bytes
	}
	graphCacheBytes = max(
		graphCacheBytes, writeCacheBytesForGraph(next, 0),
	)
	return loadedGraph{
		Graph: next, Manifest: manifest, Meta: meta,
		DataMD5:    manifest.DataMD5,
		CommitTail: commitTail,
		CacheBytes: addWriteCacheBytes(
			graphCacheBytes, commitTail.bytes,
		),
	}, true, nil
}
