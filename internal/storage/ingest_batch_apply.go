package storage

import (
	"context"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
)

func (s *TenantStore) applyIngestBatchCandidates(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) (*graph.Graph, []commitSegmentItem, bool, error) {
	groups := ingestApplyGroups(candidates)
	if len(groups) == 1 {
		return s.applyIngestBatchCandidateGroup(ctx, tenantID, loaded, candidates)
	}

	current := loaded.Graph
	currentManifest := loaded.Manifest
	items := make([]commitSegmentItem, 0, len(candidates))
	fallback := false
	for _, group := range groups {
		groupLoaded := loaded
		groupLoaded.Graph = current
		groupLoaded.Manifest = currentManifest
		next, groupItems, groupFallback, err := s.applyIngestBatchCandidateGroup(ctx, tenantID, groupLoaded, group)
		if err != nil {
			return nil, nil, fallback || groupFallback, err
		}
		current = next
		items = append(items, groupItems...)
		fallback = fallback || groupFallback
		if len(groupItems) == 0 {
			continue
		}
		last := groupItems[len(groupItems)-1].Commit
		currentManifest.Version = last.Version
		currentManifest.HeadCommitID = last.ID
		currentManifest.UpdatedAt = last.CreatedAt
	}
	return current, items, fallback, nil
}

func ingestApplyGroups(candidates []*ingestBatchCandidate) [][]*ingestBatchCandidate {
	groups := make([][]*ingestBatchCandidate, 0, len(candidates))
	for start := 0; start < len(candidates); {
		end := start + 1
		request := candidates[start].request
		switch {
		case request.ExpectedVersion != nil && !ingest.IngestRequestAtomic(request):
			expected := *request.ExpectedVersion
			for end < len(candidates) {
				next := candidates[end].request
				if next.ExpectedVersion == nil || ingest.IngestRequestAtomic(next) || *next.ExpectedVersion != expected {
					break
				}
				end++
			}
		case !ingest.IngestRequestNeedsIsolatedApply(request):
			for end < len(candidates) && !ingest.IngestRequestNeedsIsolatedApply(candidates[end].request) {
				end++
			}
		}
		groups = append(groups, candidates[start:end])
		start = end
	}
	return groups
}

func (s *TenantStore) applyIngestBatchCandidateGroup(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) (*graph.Graph, []commitSegmentItem, bool, error) {
	originalCandidates := candidates
	casCohort := false
	if merged, ok := s.prepareIngestCASCohort(loaded.Graph, candidates); ok {
		candidates = merged
		casCohort = true
		if len(candidates) == 0 {
			if err := validatePreparedIngestResults(originalCandidates); err != nil {
				return nil, nil, false, err
			}
			return loaded.Graph, nil, false, nil
		}
	} else {
		for _, candidate := range candidates {
			if ingest.IngestRequestNeedsIsolatedApply(candidate.request) {
				next, items, err := s.applyIngestBatchCandidatesIsolated(ctx, tenantID, loaded, candidates)
				return next, items, true, err
			}
		}
	}
	commits := make([]graph.Commit, len(candidates))
	nextVersion := loaded.Manifest.Version
	for index, candidate := range candidates {
		nextVersion++
		if candidate.prepared {
			if candidate.commit.TenantID != tenantID || candidate.commit.Version != nextVersion {
				return nil, nil, false, fmt.Errorf("%w: prepared commit no longer follows the base manifest", ErrIngestRepairRequired)
			}
		} else {
			commitID, err := mutationID(ctx, "ingest_batch")
			if err != nil {
				return nil, nil, false, err
			}
			candidate.commit = graph.Commit{
				LayoutVersion:    CurrentObjectLayoutVersion,
				IdentityEncoding: commitIdentityEncoding(ctx),
				ID:               commitID,
				TenantID:         tenantID,
				Version:          nextVersion,
				CreatedAt:        mutationTime(ctx),
				Mutations:        candidate.mutations,
			}
		}
		commits[index] = candidate.commit
	}
	if next, reports, err := loaded.Graph.ApplyCommitBatchStorageCopyWithOptions(commits, nil); err == nil {
		for index, candidate := range candidates {
			reports[index].Suppressed = append(candidate.policyReport.Suppressed, reports[index].Suppressed...)
		}
		if validateIngestBatchRelationSchemas(next, candidates, reports, loaded.Manifest.Version) == nil &&
			s.checkQuotaAfterApply(ctx, tenantID, loaded.Graph, next) == nil {
			items := make([]commitSegmentItem, 0, len(candidates))
			for index, candidate := range candidates {
				candidate.report = reports[index]
				candidate.changed = true
				candidate.resultManifest = Manifest{
					LayoutVersion: CurrentObjectLayoutVersion,
					TenantID:      tenantID,
					Version:       candidate.commit.Version,
					HeadCommitID:  candidate.commit.ID,
					UpdatedAt:     candidate.commit.CreatedAt,
				}
				applyCommitResultToIngest(&candidate.result, candidate.request, CommitResult{
					Manifest:          candidate.resultManifest,
					ReadableVersion:   candidate.commit.Version,
					ReadAfterCommitID: candidate.commit.ID,
					Suppressed:        reports[index].Suppressed,
					CanonicalEntities: reports[index].CanonicalEntities,
					CanonicalEdges:    reports[index].CanonicalEdges,
				})
				items = append(items, commitSegmentItem{
					Key:    s.commitKey(tenantID, candidate.commit.Version, candidate.commit.ID),
					Commit: candidate.commit,
				})
			}
			if err := validatePreparedIngestResults(originalCandidates); err != nil {
				return nil, nil, false, err
			}
			return next, items, false, nil
		}
	}
	var next *graph.Graph
	var items []commitSegmentItem
	var err error
	if casCohort {
		next, items, err = s.applyIngestBatchCandidatesIsolatedPrevalidated(ctx, tenantID, loaded, candidates)
	} else {
		next, items, err = s.applyIngestBatchCandidatesIsolated(ctx, tenantID, loaded, candidates)
	}
	if err == nil {
		err = validatePreparedIngestResults(originalCandidates)
	}
	return next, items, true, err
}

type ingestRelationSchemaVersion struct {
	tenantID     string
	revision     int64
	graphVersion int64
	etag         string
}

type ingestRelationSchemaValidation struct {
	catalog     RelationSchemaCatalog
	incremental bool
	affected    []string
	seen        map[string]struct{}
}

func validateIngestBatchRelationSchemas(
	next *graph.Graph,
	candidates []*ingestBatchCandidate,
	reports []graph.ApplyReport,
	baseVersion int64,
) error {
	validations := make([]ingestRelationSchemaValidation, 0)
	byVersion := make(map[ingestRelationSchemaVersion]int)
	for candidateIndex, candidate := range candidates {
		catalog := candidate.relationSchema
		if len(catalog.RelationSchemas) == 0 {
			continue
		}
		version := ingestRelationSchemaVersion{
			tenantID:     catalog.TenantID,
			revision:     catalog.Revision,
			graphVersion: catalog.GraphVersion,
			etag:         candidate.schemaMeta.ETag,
		}
		index, ok := byVersion[version]
		if !ok {
			index = len(validations)
			byVersion[version] = index
			validations = append(validations, ingestRelationSchemaValidation{
				catalog:     catalog,
				incremental: relationSchemaCommitCanValidateIncrementally(catalog, baseVersion),
				seen:        make(map[string]struct{}),
			})
		}
		validation := &validations[index]
		for _, edgeID := range reports[candidateIndex].AffectedEdgeIDs {
			if _, exists := validation.seen[edgeID]; exists {
				continue
			}
			validation.seen[edgeID] = struct{}{}
			validation.affected = append(validation.affected, edgeID)
		}
	}
	for _, validation := range validations {
		var err error
		if validation.incremental {
			err = validateRelationSchemaCommit(next, validation.catalog, validation.affected)
		} else {
			err = validateRelationSchemaGraph(next, validation.catalog)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) advanceIngestBatchRelationSchemaValidation(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
	graphVersion int64,
) {
	advanced := make(map[ingestRelationSchemaVersion]struct{})
	for _, candidate := range candidates {
		catalog := candidate.relationSchema
		if len(catalog.RelationSchemas) == 0 {
			continue
		}
		version := ingestRelationSchemaVersion{
			tenantID:     catalog.TenantID,
			revision:     catalog.Revision,
			graphVersion: catalog.GraphVersion,
			etag:         candidate.schemaMeta.ETag,
		}
		if _, ok := advanced[version]; ok {
			continue
		}
		if err := s.advanceRelationSchemaValidation(ctx, tenantID, catalog, candidate.schemaMeta, graphVersion); err != nil {
			return
		}
		advanced[version] = struct{}{}
	}
}

// prepareIngestCASCohort compares one writer's WAL cohort against its shared
// base snapshot. The accepted requests retain WAL order and individual logical
// versions, but can use the single-COW batch apply and one manifest publish.
func (s *TenantStore) prepareIngestCASCohort(
	base *graph.Graph,
	candidates []*ingestBatchCandidate,
) ([]*ingestBatchCandidate, bool) {
	if len(candidates) < 2 {
		return nil, false
	}
	expected := int64(0)
	for index, candidate := range candidates {
		if candidate.request.ExpectedVersion == nil || ingest.IngestRequestAtomic(candidate.request) {
			return nil, false
		}
		if index == 0 {
			expected = *candidate.request.ExpectedVersion
			continue
		}
		if *candidate.request.ExpectedVersion != expected {
			return nil, false
		}
	}

	accepted := make([]*ingestBatchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if expected != base.Version {
			markIngestCandidateFailure(candidate, fmt.Errorf(
				"%w: expected version %d, current version %d",
				ErrVersionConflict,
				expected,
				base.Version,
			))
			continue
		}
		if err := ingest.EvaluateIngestPreconditions(base, candidate.request.Preconditions, candidate.started); err != nil {
			markIngestCandidateFailure(candidate, err)
			continue
		}
		accepted = append(accepted, candidate)
	}
	if len(accepted) > 1 {
		for _, candidate := range accepted {
			candidate.casMerged = true
		}
	}
	return accepted, true
}

func (s *TenantStore) applyIngestBatchCandidatesIsolated(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) (*graph.Graph, []commitSegmentItem, error) {
	return s.applyIngestBatchCandidatesIsolatedWithGuards(ctx, tenantID, loaded, candidates, false)
}

func (s *TenantStore) applyIngestBatchCandidatesIsolatedPrevalidated(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) (*graph.Graph, []commitSegmentItem, error) {
	return s.applyIngestBatchCandidatesIsolatedWithGuards(ctx, tenantID, loaded, candidates, true)
}

func (s *TenantStore) applyIngestBatchCandidatesIsolatedWithGuards(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
	guardsPrevalidated bool,
) (*graph.Graph, []commitSegmentItem, error) {
	current := loaded.Graph
	currentHeadID := loaded.Manifest.HeadCommitID
	currentUpdatedAt := loaded.Manifest.UpdatedAt
	items := make([]commitSegmentItem, 0, len(candidates))
	for _, candidate := range candidates {
		if !guardsPrevalidated && candidate.request.ExpectedVersion != nil && *candidate.request.ExpectedVersion != current.Version {
			markIngestCandidateFailure(candidate, fmt.Errorf(
				"%w: expected version %d, current version %d",
				ErrVersionConflict,
				*candidate.request.ExpectedVersion,
				current.Version,
			))
			continue
		}
		if !guardsPrevalidated {
			if err := ingest.EvaluateIngestPreconditions(current, candidate.request.Preconditions, candidate.started); err != nil {
				markIngestCandidateFailure(candidate, err)
				continue
			}
		}
		expectedVersion := current.Version + 1
		if candidate.prepared {
			if candidate.commit.Version != expectedVersion {
				return nil, nil, fmt.Errorf("%w: prepared commit version is no longer contiguous", ErrIngestRepairRequired)
			}
		} else {
			if candidate.commit.ID == "" {
				commitID, err := mutationID(ctx, "ingest_batch")
				if err != nil {
					return nil, nil, err
				}
				candidate.commit = graph.Commit{
					LayoutVersion:    CurrentObjectLayoutVersion,
					IdentityEncoding: commitIdentityEncoding(ctx),
					ID:               commitID,
					TenantID:         tenantID,
					Mutations:        candidate.mutations,
				}
			}
			candidate.commit.Version = expectedVersion
			candidate.commit.CreatedAt = mutationTime(ctx)
		}
		identityGraph, err := current.WithCommitIdentity(candidate.commit)
		if err != nil {
			markIngestCandidateFailure(candidate, err)
			continue
		}
		report, entityNoop, err := identityGraph.PreviewStorageEntityNoop(candidate.commit)
		next := identityGraph
		if err == nil && !entityNoop {
			next, report, err = identityGraph.ApplyCommitStorageCopyWithOptions(candidate.commit, graph.ApplyOptions{})
		}
		if err == nil {
			if relationSchemaCommitCanValidateIncrementally(candidate.relationSchema, current.Version) {
				err = validateRelationSchemaCommit(next, candidate.relationSchema, report.AffectedEdgeIDs)
			} else {
				err = validateRelationSchemaGraph(next, candidate.relationSchema)
			}
		}
		if err == nil {
			err = s.checkQuotaAfterApply(ctx, tenantID, current, next)
		}
		if err != nil {
			markIngestCandidateFailure(candidate, err)
			continue
		}
		report.Suppressed = append(candidate.policyReport.Suppressed, report.Suppressed...)
		if ingest.IngestRequestAtomic(candidate.request) && len(report.Suppressed) > 0 {
			candidate.result.Suppressed = len(report.Suppressed)
			candidate.result.Conflicts = append(candidate.result.Conflicts, ingest.IngestConflicts(candidate.request, report.Suppressed)...)
			markIngestCandidateFailure(candidate, fmt.Errorf("%w: %d mutation conflicts", ErrIngestAtomicSuppressed, len(report.Suppressed)))
			continue
		}
		candidate.report = report
		if !report.Changed {
			if candidate.prepared {
				return nil, nil, fmt.Errorf("%w: prepared commit became a logical no-op", ErrIngestRepairRequired)
			}
			current = identityGraph
			candidate.result.Version = current.Version
			candidate.result.Skipped = true
			candidate.result.SkipReason = IngestSkipReasonLogicalNoop
			candidate.result.Suppressed = len(report.Suppressed)
			candidate.result.Conflicts = append(candidate.result.Conflicts, ingest.IngestConflicts(candidate.request, report.Suppressed)...)
			candidate.resultManifest = Manifest{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      tenantID,
				Version:       current.Version,
				HeadCommitID:  currentHeadID,
				UpdatedAt:     currentUpdatedAt,
				DataHash:      loaded.DataHash,
			}
			continue
		}
		candidate.changed = true
		current = next
		currentHeadID = candidate.commit.ID
		currentUpdatedAt = candidate.commit.CreatedAt
		candidate.resultManifest = Manifest{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			Version:       candidate.commit.Version,
			HeadCommitID:  candidate.commit.ID,
			UpdatedAt:     candidate.commit.CreatedAt,
		}
		applyCommitResultToIngest(&candidate.result, candidate.request, CommitResult{
			Manifest:          candidate.resultManifest,
			ReadableVersion:   candidate.commit.Version,
			ReadAfterCommitID: candidate.commit.ID,
			Suppressed:        report.Suppressed,
			CanonicalEntities: report.CanonicalEntities,
			CanonicalEdges:    report.CanonicalEdges,
		})
		items = append(items, commitSegmentItem{
			Key:    s.commitKey(tenantID, candidate.commit.Version, candidate.commit.ID),
			Commit: candidate.commit,
		})
	}
	if err := validatePreparedIngestResults(candidates); err != nil {
		return nil, nil, err
	}
	return current, items, nil
}
