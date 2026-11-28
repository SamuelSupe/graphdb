package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (s *TenantStore) manifestRepairIssues(ctx context.Context, tenantID string, manifest Manifest, manifestErr error) []RepairIssue {
	if manifestErr != nil {
		return []RepairIssue{{
			Code:         "manifest_unreadable",
			Severity:     "error",
			ResourceType: "manifest",
			ResourceID:   s.manifestKey(tenantID),
			Message:      manifestErr.Error(),
			Repairable:   true,
		}}
	}
	if manifest.Version == 0 && !manifest.ProtectedSnapshotSelector().HasSnapshot && manifestCommitTailLength(manifest) == 0 {
		hasObjects, err := s.tenantGraphObjectsExist(ctx, tenantID)
		if err != nil {
			return []RepairIssue{{
				Code:         "manifest_object_scan_failed",
				Severity:     "error",
				ResourceType: "manifest",
				ResourceID:   s.manifestKey(tenantID),
				Message:      err.Error(),
				Repairable:   false,
			}}
		}
		if hasObjects {
			return []RepairIssue{{
				Code:         "manifest_missing",
				Severity:     "error",
				ResourceType: "manifest",
				ResourceID:   s.manifestKey(tenantID),
				Message:      "manifest is empty or missing while graph objects exist",
				Repairable:   true,
			}}
		}
	}
	return nil
}

func (s *TenantStore) repairManifest(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	resumeIngest, err := s.pauseLocalIngest(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	defer resumeIngest()
	releaseViews, err := s.lockReadViews(ctx, tenantID, true)
	if err != nil {
		return Manifest{}, err
	}
	defer releaseViews()
	unlock, err := s.lockTenantMaintenance(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return Manifest{}, err
	}
	loaded, err := s.reconstructManifestFromObjects(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	if err := s.packReconstructedCommitTail(ctx, tenantID, &loaded.Manifest); err != nil {
		return Manifest{}, err
	}
	_, meta, err := s.Objects.GetWithMeta(ctx, s.manifestKey(tenantID))
	if errors.Is(err, ErrNotFound) {
		meta = ObjectMeta{Key: s.manifestKey(tenantID)}
	} else if err != nil {
		return Manifest{}, err
	}
	meta, err = s.putManifestMeta(ctx, tenantID, loaded.Manifest, meta)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return Manifest{}, err
	}
	loaded.Meta = meta
	s.setWriteCacheContext(ctx, tenantID, loaded)
	return loaded.Manifest, nil
}

func (s *TenantStore) reconstructManifestFromObjects(ctx context.Context, tenantID string) (loadedGraph, error) {
	base, snapshotKey, err := s.latestValidSnapshot(ctx, tenantID)
	if err != nil {
		return loadedGraph{}, err
	}
	g := graph.New()
	manifest := Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: mutationTime(ctx)}
	if base != nil {
		var err error
		g, err = graph.FromSnapshot(*base)
		if err != nil {
			return loadedGraph{}, err
		}
		manifest.Version = base.Version
		if snapshotKey == s.snapshotCatalogKey(tenantID, base.Version) {
			manifest.SnapshotCatalogKey = snapshotKey
			singleKey := s.snapshotKey(tenantID, base.Version)
			if record, err := s.loadSnapshotRecord(ctx, singleKey); err == nil &&
				(record.TenantID == "" || record.TenantID == tenantID) &&
				record.Snapshot.Version == base.Version {
				manifest.SnapshotKey = singleKey
			}
		} else {
			manifest.SnapshotKey = snapshotKey
		}
		manifest.SnapshotVersion = base.Version
	}
	scan, err := s.loadCommitObjects(
		ctx, tenantID, nil, manifest.Version,
	)
	if err != nil {
		return loadedGraph{}, err
	}
	segments, _, err := s.loadCommitSegmentObjects(
		ctx, tenantID, nil, manifest.Version,
	)
	if err != nil {
		return loadedGraph{}, err
	}
	identities := make(map[int64]string)
	check := func(commit graph.Commit) error {
		if commit.Version <= manifest.SnapshotVersion {
			return nil
		}
		if previous, exists := identities[commit.Version]; exists && previous != commit.ID {
			return fmt.Errorf("ambiguous commit version %d (%q, %q); restore a verified backup instead of guessing publication history", commit.Version, previous, commit.ID)
		}
		identities[commit.Version] = commit.ID
		return nil
	}
	for _, item := range scan.Items {
		if err := check(item.Commit); err != nil {
			return loadedGraph{}, err
		}
	}
	for _, segment := range segments {
		for _, item := range segment.Items {
			if err := check(item.Commit); err != nil {
				return loadedGraph{}, err
			}
		}
	}
	g = applyReconstructedCommitTail(g, &manifest, segments, scan.Items)
	if manifest.Version != g.Version {
		return loadedGraph{}, fmt.Errorf("reconstructed manifest version %d does not match graph version %d", manifest.Version, g.Version)
	}
	manifest.DataHash, err = g.ContentHash()
	if err != nil {
		return loadedGraph{}, err
	}
	return loadedGraph{Graph: g, Manifest: manifest, DataHash: manifest.DataHash, Meta: ObjectMeta{Key: s.manifestKey(tenantID)}}, nil
}

func (s *TenantStore) packReconstructedCommitTail(ctx context.Context, tenantID string, manifest *Manifest) error {
	// A manifest loads all segments before any loose commits. Pack only the
	// loose runs that must precede an existing segment.
	keys := manifest.CommitKeys
	segments := make([]CommitSegmentRef, 0, len(manifest.CommitSegments))
	for _, segment := range manifest.CommitSegments {
		n := 0
		for n < len(keys) {
			version, _, ok := commitIdentityFromKey(keys[n])
			if !ok {
				return fmt.Errorf("invalid reconstructed commit key %q", keys[n])
			}
			if version >= segment.FirstVersion {
				break
			}
			n++
		}
		for n > 0 {
			count := min(n, commitSegmentTargetCount)
			ref, err := s.putCommitSegmentFromKeys(ctx, tenantID, keys[:count])
			if err != nil {
				return err
			}
			segments = append(segments, ref)
			keys = keys[count:]
			n -= count
		}
		segments = append(segments, segment)
	}
	manifest.CommitSegments, manifest.CommitKeys = segments, keys
	return nil
}

func applyReconstructedCommitTail(g *graph.Graph, manifest *Manifest, segments []commitSegmentObject, loose []commitObject) *graph.Graph {
	looseByVersion := map[int64]commitObject{}
	for _, item := range loose {
		if _, ok := looseByVersion[item.Commit.Version]; !ok {
			looseByVersion[item.Commit.Version] = item
		}
	}
	orderedSegments := append([]commitSegmentObject(nil), segments...)
	sort.Slice(orderedSegments, func(i, j int) bool {
		if orderedSegments[i].Ref.LastVersion != orderedSegments[j].Ref.LastVersion {
			return orderedSegments[i].Ref.LastVersion > orderedSegments[j].Ref.LastVersion
		}
		if orderedSegments[i].Ref.FirstVersion != orderedSegments[j].Ref.FirstVersion {
			return orderedSegments[i].Ref.FirstVersion > orderedSegments[j].Ref.FirstVersion
		}
		return orderedSegments[i].Ref.Key < orderedSegments[j].Ref.Key
	})
	for {
		nextVersion := g.Version + 1
		applied := false
		for _, segment := range orderedSegments {
			if segment.Ref.FirstVersion > nextVersion || segment.Ref.LastVersion < nextVersion {
				continue
			}
			next, ok := applyReconstructedSegment(g, segment)
			if !ok {
				continue
			}
			g = next
			last := segment.Items[len(segment.Items)-1].Commit
			manifest.Version = last.Version
			manifest.HeadCommitID = last.ID
			manifest.CommitSegments = append(manifest.CommitSegments, segment.Ref)
			manifest.UpdatedAt = last.CreatedAt
			applied = true
			break
		}
		if applied {
			continue
		}
		item, ok := looseByVersion[nextVersion]
		if !ok {
			break
		}
		next, _, err := g.ApplyCommitStorageCopyWithOptions(item.Commit, graph.ApplyOptions{})
		if err != nil {
			break
		}
		g = next
		manifest.Version = item.Commit.Version
		manifest.HeadCommitID = item.Commit.ID
		manifest.CommitKeys = append(manifest.CommitKeys, item.Key)
		manifest.UpdatedAt = item.Commit.CreatedAt
	}
	return g
}

func applyReconstructedSegment(g *graph.Graph, segment commitSegmentObject) (*graph.Graph, bool) {
	next := g
	appliedCount := 0
	for _, item := range segment.Items {
		if item.Commit.Version <= next.Version {
			continue
		}
		if item.Commit.Version != next.Version+1 {
			return nil, false
		}
		applied, _, err := next.ApplyCommitStorageCopyWithOptions(item.Commit, graph.ApplyOptions{})
		if err != nil {
			return nil, false
		}
		next = applied
		appliedCount++
	}
	return next, appliedCount > 0
}

func (s *TenantStore) latestValidSnapshot(ctx context.Context, tenantID string) (*graph.Snapshot, string, error) {
	type candidate struct {
		key     string
		version int64
	}
	candidates := make([]candidate, 0)
	err := scanObjectPrefixFresh(
		ctx,
		s.Objects,
		s.snapshotPrefix(tenantID),
		func(objects []ObjectInfo) error {
			for _, object := range objects {
				version, ok := snapshotIdentityFromKey(object.Key)
				if !ok && strings.HasSuffix(object.Key, "/catalog.parquet") {
					version, ok = versionFromObjectKey(s.snapshotPrefix(tenantID)+"sharded/", object.Key)
					ok = ok && object.Key == s.snapshotCatalogKey(tenantID, version)
				}
				if ok {
					candidates = append(candidates, candidate{
						key:     object.Key,
						version: version,
					})
				}
			}
			return nil
		},
	)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].version == candidates[j].version {
			iSharded := candidates[i].key == s.snapshotCatalogKey(tenantID, candidates[i].version)
			jSharded := candidates[j].key == s.snapshotCatalogKey(tenantID, candidates[j].version)
			if iSharded != jSharded {
				return iSharded
			}
			return candidates[i].key < candidates[j].key
		}
		return candidates[i].version > candidates[j].version
	})
	for _, candidate := range candidates {
		if candidate.key == s.snapshotCatalogKey(tenantID, candidate.version) {
			catalog, err := s.getShardedSnapshotCatalog(ctx, tenantID, candidate.key)
			if err != nil || catalog.Version != candidate.version {
				continue
			}
			snapshot, err := s.loadSnapshotFromCatalog(ctx, tenantID, catalog)
			if err != nil {
				continue
			}
			if _, err := graph.FromSnapshot(snapshot); err == nil {
				return &snapshot, candidate.key, nil
			}
			continue
		}
		record, err := s.loadSnapshotRecord(ctx, candidate.key)
		if err != nil {
			continue
		}
		if record.TenantID != "" && record.TenantID != tenantID {
			continue
		}
		if record.Snapshot.Version != candidate.version {
			continue
		}
		if _, err := graph.FromSnapshot(record.Snapshot); err != nil {
			continue
		}
		snapshot := record.Snapshot
		return &snapshot, candidate.key, nil
	}
	return nil, "", nil
}

func (s *TenantStore) tenantGraphObjectsExist(ctx context.Context, tenantID string) (bool, error) {
	for _, prefix := range []string{s.snapshotPrefix(tenantID), s.commitPrefix(tenantID)} {
		exists, err := objectPrefixMatches(
			ctx,
			s.Objects,
			prefix,
			func(ObjectInfo) bool { return true },
		)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}
