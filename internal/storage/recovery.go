package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type RecoveryReport struct {
	TenantID       string   `json:"tenant_id"`
	StartVersion   int64    `json:"start_version"`
	EndVersion     int64    `json:"end_version"`
	Recovered      int      `json:"recovered"`
	Skipped        int      `json:"skipped"`
	Blocked        int      `json:"blocked"`
	RecoveredKeys  []string `json:"recovered_keys,omitempty"`
	UnappliedKeys  []string `json:"unapplied_keys,omitempty"`
	StaleOrphans   []string `json:"stale_orphans,omitempty"`
	InvalidKeys    []string `json:"invalid_keys,omitempty"`
	IndexWarnings  []string `json:"index_warnings,omitempty"`
	ReferencedKeys int      `json:"referenced_keys"`
}

type CleanupReport struct {
	TenantID        string   `json:"tenant_id"`
	ManifestVersion int64    `json:"manifest_version"`
	Deleted         int      `json:"deleted"`
	KeptFuture      int      `json:"kept_future"`
	ReferencedKeys  int      `json:"referenced_keys"`
	DeletedKeys     []string `json:"deleted_keys,omitempty"`
	FutureKeys      []string `json:"future_keys,omitempty"`
	InvalidKeys     []string `json:"invalid_keys,omitempty"`
}

func (s *TenantStore) RecoverTenant(ctx context.Context, tenantID string) (RecoveryReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RecoveryReport{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return RecoveryReport{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return RecoveryReport{}, err
	}
	loaded, err := s.loadWithMeta(ctx, tenantID)
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{TenantID: tenantID, StartVersion: loaded.Manifest.Version}
	referenced := referencedCommits(loaded.Manifest)
	report.ReferencedKeys = len(referenced)
	scan, err := s.loadCommitObjects(ctx, tenantID, referenced, 0)
	if err != nil {
		return report, err
	}
	report.InvalidKeys = scan.InvalidKeys
	report.Blocked += len(scan.InvalidKeys)
	report.UnappliedKeys = append(report.UnappliedKeys, scan.InvalidKeys...)
	for _, item := range scan.Items {
		if item.Commit.Version <= loaded.Manifest.Version {
			report.Skipped++
			report.StaleOrphans = append(report.StaleOrphans, item.Key)
			continue
		}
		if item.Commit.Version != loaded.Manifest.Version+1 {
			report.Blocked++
			report.UnappliedKeys = append(report.UnappliedKeys, item.Key)
			continue
		}
		nextGraph, applyReport, err := loaded.Graph.ApplyCommitCopyWithOptions(item.Commit, graph.ApplyOptions{})
		if err != nil {
			report.Blocked++
			report.UnappliedKeys = append(report.UnappliedKeys, item.Key)
			continue
		}
		previousGraph := loaded.Graph
		loaded.Manifest.LayoutVersion = CurrentObjectLayoutVersion
		loaded.Manifest.Version = item.Commit.Version
		loaded.Manifest.HeadCommitID = item.Commit.ID
		loaded.Manifest.CommitKeys = append(loaded.Manifest.CommitKeys, item.Key)
		loaded.Manifest.UpdatedAt = item.Commit.CreatedAt
		meta, err := s.putManifestMeta(ctx, tenantID, loaded.Manifest, loaded.Meta)
		if err != nil {
			s.deleteWriteCache(tenantID)
			return report, err
		}
		loaded.Meta = meta
		loaded.Graph = nextGraph
		if err := s.updateIndexesAfterCommit(ctx, tenantID, previousGraph, nextGraph, item.Commit.Mutations, applyReport, item.Commit.Version); err != nil {
			report.IndexWarnings = append(report.IndexWarnings, "incremental index update failed for "+item.Key+": "+err.Error())
		}
		report.Recovered++
		report.RecoveredKeys = append(report.RecoveredKeys, item.Key)
	}
	report.EndVersion = loaded.Manifest.Version
	s.setWriteCache(tenantID, loaded)
	return report, nil
}

func (s *TenantStore) CleanupCommits(ctx context.Context, tenantID string) (CleanupReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return CleanupReport{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return CleanupReport{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return CleanupReport{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return CleanupReport{}, err
	}
	return s.cleanupCommitsLocked(ctx, tenantID, manifest, newGCCheckpointRunner(GCOptions{}))
}

type commitObject struct {
	Key    string
	Commit graph.Commit
}

type commitObjectScan struct {
	Items       []commitObject
	InvalidKeys []string
	NextCursor  string
	Truncated   bool
}

func (s *TenantStore) loadCommitObjects(
	ctx context.Context,
	tenantID string,
	referenced map[string]struct{},
	afterVersion int64,
) (commitObjectScan, error) {
	scan := commitObjectScan{}
	err := scanObjectPrefixFresh(
		ctx,
		s.Objects,
		s.commitPrefix(tenantID),
		func(objects []ObjectInfo) error {
			for _, object := range objects {
				if strings.HasPrefix(
					object.Key,
					s.commitSegmentPrefix(tenantID),
				) {
					continue
				}
				if err := s.appendCommitObject(
					ctx,
					tenantID,
					object,
					referenced,
					afterVersion,
					&scan,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		return commitObjectScan{}, err
	}
	sortCommitObjectScan(&scan)
	return scan, nil
}

func (s *TenantStore) loadCommitObjectsPage(ctx context.Context, tenantID string, referenced map[string]struct{}, cursor string, limit int) (commitObjectScan, error) {
	objects, nextCursor, err := listObjectPage(ctx, s.Objects, s.commitPrefix(tenantID), cursor, limit)
	if err != nil {
		return commitObjectScan{}, err
	}
	capacity := len(objects)
	if limit > 0 && capacity > limit {
		capacity = limit
	}
	scan := commitObjectScan{Items: make([]commitObject, 0, capacity)}
	reachedSegments := false
	for _, object := range objects {
		if strings.HasPrefix(object.Key, s.commitSegmentPrefix(tenantID)) {
			if limit > 0 {
				reachedSegments = true
				break
			}
			continue
		}
		scan.NextCursor = object.Key
		if err := s.appendCommitObject(
			ctx,
			tenantID,
			object,
			referenced,
			0,
			&scan,
		); err != nil {
			return commitObjectScan{}, err
		}
	}
	scan.Truncated = nextCursor != "" && !reachedSegments
	if scan.Truncated {
		scan.NextCursor = nextCursor
	} else {
		scan.NextCursor = ""
	}
	sortCommitObjectScan(&scan)
	return scan, nil
}

func (s *TenantStore) appendCommitObject(
	ctx context.Context,
	tenantID string,
	object ObjectInfo,
	referenced map[string]struct{},
	afterVersion int64,
	scan *commitObjectScan,
) error {
	if _, ok := referenced[object.Key]; ok {
		return nil
	}
	if version, _, ok := commitIdentityFromKey(object.Key); afterVersion > 0 && ok &&
		version <= afterVersion {
		return nil
	}
	data, err := s.Objects.Get(ctx, object.Key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	commit, err := unmarshalCommitObject(data)
	if err != nil || commit.TenantID != tenantID ||
		validateCommitObjectIdentity(object.Key, commit) != nil {
		scan.InvalidKeys = append(scan.InvalidKeys, object.Key)
		return nil
	}
	scan.Items = append(scan.Items, commitObject{
		Key: object.Key, Commit: commit,
	})
	return nil
}

func sortCommitObjectScan(scan *commitObjectScan) {
	sort.Slice(scan.Items, func(i, j int) bool {
		if scan.Items[i].Commit.Version == scan.Items[j].Commit.Version {
			return scan.Items[i].Key < scan.Items[j].Key
		}
		return scan.Items[i].Commit.Version < scan.Items[j].Commit.Version
	})
	sort.Strings(scan.InvalidKeys)
}

func validateCommitObjectIdentity(key string, commit graph.Commit) error {
	version, id, ok := commitIdentityFromKey(key)
	if !ok {
		return fmt.Errorf("invalid commit key %q", key)
	}
	if commit.Version != version || commit.ID != id {
		return fmt.Errorf("commit identity mismatch: key %q has version %d id %q but object has version %d id %q", key, version, id, commit.Version, commit.ID)
	}
	return nil
}

func validateSnapshotObjectIdentity(key string, snapshot graph.Snapshot) error {
	version, ok := snapshotIdentityFromKey(key)
	if !ok {
		return fmt.Errorf("invalid snapshot key %q", key)
	}
	if snapshot.Version != version {
		return fmt.Errorf("snapshot identity mismatch: key %q has version %d but object has version %d", key, version, snapshot.Version)
	}
	return nil
}

func commitIdentityFromKey(key string) (int64, string, bool) {
	name := path.Base(key)
	const suffix = ".parquet"
	if !strings.HasSuffix(name, suffix) {
		return 0, "", false
	}
	base := strings.TrimSuffix(name, suffix)
	if len(base) < 22 || base[20] != '-' {
		return 0, "", false
	}
	version, err := strconv.ParseInt(base[:20], 10, 64)
	if err != nil {
		return 0, "", false
	}
	id := base[21:]
	if id == "" {
		return 0, "", false
	}
	return version, id, true
}

func commitSegmentIdentityFromKey(key string) (int64, int64, bool) {
	name := path.Base(key)
	const suffix = ".parquet"
	if !strings.HasSuffix(name, suffix) {
		return 0, 0, false
	}
	base := strings.TrimSuffix(name, suffix)
	if len(base) < 43 || base[20] != '-' || base[41] != '-' ||
		base[42:] == "" {
		return 0, 0, false
	}
	first, err := strconv.ParseInt(base[:20], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	last, err := strconv.ParseInt(base[21:41], 10, 64)
	if err != nil || last < first {
		return 0, 0, false
	}
	return first, last, true
}

func snapshotIdentityFromKey(key string) (int64, bool) {
	name := path.Base(key)
	const suffix = ".parquet"
	if !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	base := strings.TrimSuffix(name, suffix)
	if len(base) != 20 {
		return 0, false
	}
	version, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return version, true
}

func referencedCommits(manifest Manifest) map[string]struct{} {
	out := map[string]struct{}{}
	for _, segment := range manifest.CommitSegments {
		out[segment.Key] = struct{}{}
	}
	for _, key := range manifest.CommitKeys {
		out[key] = struct{}{}
	}
	return out
}
