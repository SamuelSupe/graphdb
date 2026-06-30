package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"graphdb/internal/graph"
)

func (s *TenantStore) CommitWithReport(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (CommitResult, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return CommitResult{}, err
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}
	request := directCommitRequest(mutations, opts)
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if record, ok, err := s.loadDirectCommitRecord(ctx, tenantID, request); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	} else if ok {
		return replayDirectCommitResult(record), nil
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}
	if err := s.CheckWriteBackpressure(ctx, tenantID); err != nil {
		return CommitResult{}, err
	}
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}
	if err := s.CheckWriteBackpressure(ctx, tenantID); err != nil {
		return CommitResult{}, err
	}
	started := time.Now().UTC()
	result, err := s.commitWithRetryLocked(ctx, tenantID, mutations, opts)
	finished := time.Now().UTC()
	if err != nil {
		return CommitResult{}, err
	}
	if err := s.saveDirectCommitRecord(ctx, tenantID, request, result, started, finished); err != nil {
		return result, fmt.Errorf("save commit idempotency record: %w", err)
	}
	return result, nil
}

func (s *TenantStore) commitWithRetryLocked(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (CommitResult, error) {
	attempts := s.MaxRetries
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		result, err := s.commitOnceLocked(ctx, tenantID, mutations, opts)
		if err == nil {
			return result, nil
		}
		last = err
		if !errors.Is(err, ErrConflict) {
			return CommitResult{}, err
		}
		s.deleteWriteCache(tenantID)
		if attempt+1 >= attempts {
			break
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return CommitResult{}, err
		}
		if err := s.acquireWriterLease(ctx, tenantID); err != nil {
			if pressure := s.objectStoreBackpressureError(err); pressure != nil {
				return CommitResult{}, pressure
			}
			return CommitResult{}, err
		}
	}
	return CommitResult{}, last
}

func (s *TenantStore) commitOnceLocked(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (CommitResult, error) {
	loaded, err := s.loadForWriteLocked(ctx, tenantID)
	if err != nil {
		return CommitResult{}, err
	}
	mutations, err = s.resolveSourcePriorities(ctx, tenantID, mutations)
	if err != nil {
		return CommitResult{}, err
	}
	manifest := loaded.Manifest
	if opts.ExpectedVersion != nil && *opts.ExpectedVersion != manifest.Version {
		return CommitResult{}, fmt.Errorf("expected version %d, current version %d", *opts.ExpectedVersion, manifest.Version)
	}
	version := manifest.Version + 1
	commitID, err := newCommitID()
	if err != nil {
		return CommitResult{}, err
	}
	commit := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		ID:            commitID,
		TenantID:      tenantID,
		Version:       version,
		CreatedAt:     time.Now().UTC(),
		Mutations:     mutations,
	}
	nextGraph, report, err := loaded.Graph.ApplyCommitCopyWithOptions(commit, graph.ApplyOptions{})
	if err != nil {
		return CommitResult{}, err
	}
	if err := s.checkQuotaAfterApply(ctx, tenantID, loaded.Graph, nextGraph); err != nil {
		return CommitResult{}, err
	}
	previousMD5, err := loaded.Graph.ContentMD5()
	if err != nil {
		return CommitResult{}, err
	}
	nextMD5, err := nextGraph.ContentMD5()
	if err != nil {
		return CommitResult{}, err
	}
	if previousMD5 == nextMD5 {
		return CommitResult{
			Manifest:          manifest,
			ReadableVersion:   manifest.Version,
			Skipped:           true,
			DataMD5:           previousMD5,
			Suppressed:        report.Suppressed,
			CanonicalEntities: report.CanonicalEntities,
			CanonicalEdges:    report.CanonicalEdges,
		}, nil
	}
	commitKey := s.commitKey(tenantID, version, commitID)
	if err := s.putCommitObjectIfAbsent(ctx, commitKey, commit); err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.Version = version
	manifest.HeadCommitID = commitID
	manifest.CommitKeys = append(append([]string(nil), manifest.CommitKeys...), commitKey)
	manifest.UpdatedAt = commit.CreatedAt
	if err := s.segmentCommitTailIfNeeded(ctx, tenantID, &manifest); err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, loaded.Meta)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}
	s.setWriteCache(tenantID, loadedGraph{Graph: nextGraph, Manifest: manifest, Meta: meta})
	result := CommitResult{
		Manifest:          manifest,
		ReadableVersion:   version,
		ReadAfterCommitID: commitID,
		DataMD5:           nextMD5,
		Suppressed:        report.Suppressed,
		CanonicalEntities: report.CanonicalEntities,
		CanonicalEdges:    report.CanonicalEdges,
	}
	if err := s.updateIndexesAfterCommit(ctx, tenantID, loaded.Graph, nextGraph, mutations, report, version); err != nil {
		result.IndexWarnings = append(result.IndexWarnings, "incremental index update failed: "+err.Error())
	}
	return result, nil
}
