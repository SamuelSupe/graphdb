package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
	if manifest.Version == 0 && manifest.SnapshotKey == "" && manifestCommitTailLength(manifest) == 0 {
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
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return Manifest{}, err
	}
	loaded, err := s.reconstructManifestFromObjects(ctx, tenantID)
	if err != nil {
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
	s.setWriteCache(tenantID, loaded)
	return loaded.Manifest, nil
}

func (s *TenantStore) reconstructManifestFromObjects(ctx context.Context, tenantID string) (loadedGraph, error) {
	base, snapshotKey, err := s.latestValidSnapshot(ctx, tenantID)
	if err != nil {
		return loadedGraph{}, err
	}
	g := graph.New()
	manifest := Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: time.Now().UTC()}
	if base != nil {
		var err error
		g, err = graph.FromSnapshot(*base)
		if err != nil {
			return loadedGraph{}, err
		}
		manifest.Version = base.Version
		manifest.SnapshotKey = snapshotKey
		manifest.SnapshotVersion = base.Version
	}
	scan, err := s.loadCommitObjects(ctx, tenantID, nil)
	if err != nil {
		return loadedGraph{}, err
	}
	segments, _, err := s.loadCommitSegmentObjects(ctx, tenantID, nil)
	if err != nil {
		return loadedGraph{}, err
	}
	g = applyReconstructedCommitTail(g, &manifest, segments, scan.Items)
	if manifest.Version != g.Version {
		return loadedGraph{}, fmt.Errorf("reconstructed manifest version %d does not match graph version %d", manifest.Version, g.Version)
	}
	return loadedGraph{Graph: g, Manifest: manifest, Meta: ObjectMeta{Key: s.manifestKey(tenantID)}}, nil
}

func applyReconstructedCommitTail(g *graph.Graph, manifest *Manifest, segments []commitSegmentObject, loose []commitObject) *graph.Graph {
	looseByVersion := map[int64]commitObject{}
	for _, item := range loose {
		if _, ok := looseByVersion[item.Commit.Version]; !ok {
			looseByVersion[item.Commit.Version] = item
		}
	}
	segmentsByFirst := map[int64][]commitSegmentObject{}
	for _, segment := range segments {
		segmentsByFirst[segment.Ref.FirstVersion] = append(segmentsByFirst[segment.Ref.FirstVersion], segment)
	}
	for {
		nextVersion := g.Version + 1
		if candidates := segmentsByFirst[nextVersion]; len(candidates) > 0 {
			applied := false
			for _, segment := range candidates {
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
		}
		item, ok := looseByVersion[nextVersion]
		if !ok {
			break
		}
		next, _, err := g.ApplyCommitCopyWithOptions(item.Commit, graph.ApplyOptions{})
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
	for _, item := range segment.Items {
		if item.Commit.Version != next.Version+1 {
			return nil, false
		}
		applied, _, err := next.ApplyCommitCopyWithOptions(item.Commit, graph.ApplyOptions{})
		if err != nil {
			return nil, false
		}
		next = applied
	}
	return next, true
}

func (s *TenantStore) latestValidSnapshot(ctx context.Context, tenantID string) (*graph.Snapshot, string, error) {
	objects, err := s.Objects.List(ctx, s.snapshotPrefix(tenantID))
	if err != nil {
		return nil, "", err
	}
	var best *graph.Snapshot
	bestKey := ""
	for _, object := range objects {
		version, ok := snapshotIdentityFromKey(object.Key)
		if !ok {
			continue
		}
		record, err := s.loadSnapshotRecord(ctx, object.Key)
		if err != nil {
			continue
		}
		if record.TenantID != "" && record.TenantID != tenantID {
			continue
		}
		if record.Snapshot.Version != version {
			continue
		}
		if _, err := graph.FromSnapshot(record.Snapshot); err != nil {
			continue
		}
		if best == nil || record.Snapshot.Version > best.Version {
			snapshot := record.Snapshot
			best = &snapshot
			bestKey = object.Key
		}
	}
	return best, bestKey, nil
}

func (s *TenantStore) tenantGraphObjectsExist(ctx context.Context, tenantID string) (bool, error) {
	for _, prefix := range []string{s.snapshotPrefix(tenantID), s.commitPrefix(tenantID)} {
		objects, err := s.Objects.List(ctx, prefix)
		if err != nil {
			return false, err
		}
		if len(objects) > 0 {
			return true, nil
		}
	}
	return false, nil
}
