package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
	"go.opentelemetry.io/otel/attribute"
)

func (s *TenantStore) prepareIngestBatchManifest(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	finalGraph *graph.Graph,
	newItems []commitSegmentItem,
) (Manifest, int64, preparedCommitSegment, error) {
	type hashResult struct {
		digest       string
		logicalBytes int64
		err          error
	}
	type segmentResult struct {
		prepared preparedCommitSegment
		err      error
	}
	hashCh := make(chan hashResult, 1)
	segmentCh := make(chan segmentResult, 1)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		digest, logicalBytes, err := finalGraph.ContentHashWithLogicalSize()
		hashCh <- hashResult{digest: digest, logicalBytes: logicalBytes, err: err}
	}()
	go func() {
		defer wait.Done()
		items, err := s.ingestBatchSegmentItems(ctx, tenantID, loaded, newItems)
		if err != nil {
			segmentCh <- segmentResult{err: err}
			return
		}
		prepared, err := s.prepareCommitSegment(ctx, tenantID, items, true)
		segmentCh <- segmentResult{prepared: prepared, err: err}
	}()
	wait.Wait()
	hashed := <-hashCh
	segmented := <-segmentCh
	if hashed.err != nil {
		return Manifest{}, 0, preparedCommitSegment{}, hashed.err
	}
	if segmented.err != nil {
		return Manifest{}, 0, preparedCommitSegment{}, segmented.err
	}
	last := newItems[len(newItems)-1].Commit
	manifest := loaded.Manifest
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.Version = last.Version
	manifest.HeadCommitID = last.ID
	manifest.CommitSegments = append(append([]CommitSegmentRef(nil), manifest.CommitSegments...), segmented.prepared.ref)
	manifest.CommitKeys = nil
	manifest.UpdatedAt = last.CreatedAt
	manifest.DataHash = hashed.digest
	return manifest, hashed.logicalBytes, segmented.prepared, nil
}

func (s *TenantStore) publishIngestBatch(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	finalGraph *graph.Graph,
	manifest Manifest,
	preparedSegment preparedCommitSegment,
	newItems []commitSegmentItem,
	candidates []*ingestBatchCandidate,
	logicalBytes int64,
) (indexWork *commitIndexUpdate, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest.publish",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.publish.logical_commits", len(newItems)),
		attribute.Int64("graphdb.ingest.publish.base_version", loaded.Manifest.Version),
		attribute.Int64("graphdb.ingest.publish.final_version", manifest.Version),
	)
	defer func() { endStorageSpan(span, err) }()
	if err := s.persistInitialTenantHead(ctx, tenantID, &loaded); err != nil {
		return nil, err
	}
	ref, err := s.putPreparedCommitSegment(ctx, tenantID, preparedSegment)
	if err != nil {
		return nil, err
	}
	expected := manifest.CommitSegments[len(manifest.CommitSegments)-1]
	if ref != expected {
		return nil, fmt.Errorf("prepared commit segment changed before publish")
	}
	err = s.publishCommittedGraph(ctx, tenantID, loaded, loadedGraph{
		Graph: finalGraph, Manifest: manifest, DataHash: manifest.DataHash,
		CommitTail: emptyCommitTailCache(),
		CacheBytes: writeCacheBytesForGraphWithCommitTail(finalGraph, logicalBytes, emptyCommitTailCache()),
	})
	if err != nil {
		return nil, err
	}
	s.advanceIngestBatchRelationSchemaValidation(ctx, tenantID, candidates, manifest.Version)
	span.SetAttributes(
		attribute.Int("graphdb.ingest.publish.segment_commits", ref.Count),
		attribute.Int64("graphdb.ingest.publish.segment_first_version", ref.FirstVersion),
		attribute.Int64("graphdb.ingest.publish.segment_last_version", ref.LastVersion),
	)

	aggregateMutations := graph.Mutations{}
	aggregateReport := graph.ApplyReport{Changed: true}
	for _, item := range newItems {
		appendGraphMutations(&aggregateMutations, item.Commit.Mutations)
	}
	for _, candidate := range candidates {
		if !candidate.changed {
			continue
		}
		aggregateReport.Suppressed = append(aggregateReport.Suppressed, candidate.report.Suppressed...)
		aggregateReport.CanonicalEntities = append(aggregateReport.CanonicalEntities, candidate.report.CanonicalEntities...)
		aggregateReport.CanonicalEdges = append(aggregateReport.CanonicalEdges, candidate.report.CanonicalEdges...)
		aggregateReport.AffectedEntityIDs = append(aggregateReport.AffectedEntityIDs, candidate.report.AffectedEntityIDs...)
		aggregateReport.AffectedEdgeIDs = append(aggregateReport.AffectedEdgeIDs, candidate.report.AffectedEdgeIDs...)
	}
	indexWork = &commitIndexUpdate{
		baseVersion: loaded.Manifest.Version,
		before:      loaded.Graph,
		after:       finalGraph,
		mutations:   aggregateMutations,
		report:      aggregateReport,
		version:     manifest.Version,
	}
	if s.localFileStore() != nil && s.enqueueLocalIngestIndexUpdate(ctx, tenantID, indexWork) {
		// The WAL and manifest are already durable. Index failures cannot undo
		// acceptance or publication; reads retain their version-checked fallback.
		return nil, nil
	}
	s.enqueueCommitIndexUpdate(ctx, tenantID, indexWork)
	return indexWork, nil
}

func (s *TenantStore) ingestBatchSegmentItems(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	newItems []commitSegmentItem,
) ([]commitSegmentItem, error) {
	items := make([]commitSegmentItem, 0, len(loaded.Manifest.CommitKeys)+len(newItems))
	if len(loaded.Manifest.CommitKeys) > 0 {
		if loaded.CommitTail.matches(loaded.Manifest.CommitKeys) {
			items = append(items, loaded.CommitTail.items...)
		} else {
			for _, key := range loaded.Manifest.CommitKeys {
				commit, err := s.getCommitObject(ctx, key)
				if err != nil {
					return nil, err
				}
				if err := validateCommitObjectIdentity(key, commit); err != nil {
					return nil, err
				}
				items = append(items, commitSegmentItem{Key: key, Commit: commit})
			}
		}
	}
	items = append(items, newItems...)
	return items, nil
}

func (s *TenantStore) preparedIngestPublished(
	ctx context.Context,
	tenantID string,
	prepared IngestPreparedRequest,
) (bool, error) {
	if prepared.FlushID == "" || prepared.FinalVersion < prepared.BaseVersion {
		return false, fmt.Errorf("%w: invalid prepared flush identity", ErrIngestRepairRequired)
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if manifest.Version == prepared.BaseVersion && manifest.HeadCommitID == prepared.BaseHeadCommitID {
		return false, nil
	}
	if manifest.Version == prepared.FinalVersion && manifest.HeadCommitID == prepared.FinalHeadCommitID {
		return true, nil
	}
	if manifest.Version <= prepared.FinalVersion {
		return false, fmt.Errorf(
			"%w: manifest is neither the prepared base nor final state",
			ErrIngestRepairRequired,
		)
	}

	targetVersion := prepared.FinalVersion
	targetCommitID := prepared.FinalHeadCommitID
	if prepared.Commit != nil {
		targetVersion = prepared.Commit.Version
		targetCommitID = prepared.Commit.ID
	}
	published, decisive, err := s.directCommitPreparedPublished(ctx, tenantID, DirectCommitRecord{
		Result: CommitResult{Manifest: Manifest{
			Version:      targetVersion,
			HeadCommitID: targetCommitID,
		}},
	})
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrIngestRepairRequired, err)
	}
	if !decisive || !published {
		return false, fmt.Errorf(
			"%w: prepared flush is not present in the current manifest history",
			ErrIngestRepairRequired,
		)
	}
	return true, nil
}

func (s *TenantStore) preparedIngestBatchPlans(
	ctx context.Context,
	tenantID string,
	entries []IngestBatchEntry,
	candidates []*ingestBatchCandidate,
	base Manifest,
	final Manifest,
) ([]*IngestPreparedRequest, error) {
	if base.TenantID == "" {
		var err error
		base, _, err = s.getManifest(ctx, tenantID)
		if err != nil {
			return nil, err
		}
	}
	if final.TenantID == "" {
		final = base
	}

	var existing *IngestPreparedRequest
	hasFresh := false
	hasPreparedCommit := false
	for _, candidate := range candidates {
		if candidate.preparedPlan == nil {
			hasFresh = true
			continue
		}
		hasPreparedCommit = hasPreparedCommit || candidate.prepared
		if existing == nil {
			existing = candidate.preparedPlan
			continue
		}
		if existing.FlushID != candidate.preparedPlan.FlushID ||
			existing.BaseVersion != candidate.preparedPlan.BaseVersion ||
			existing.BaseHeadCommitID != candidate.preparedPlan.BaseHeadCommitID ||
			existing.BaseHeadRevision != candidate.preparedPlan.BaseHeadRevision ||
			existing.BaseGeneration != candidate.preparedPlan.BaseGeneration ||
			existing.BaseWriteContextRevision != candidate.preparedPlan.BaseWriteContextRevision ||
			existing.FinalVersion != candidate.preparedPlan.FinalVersion ||
			existing.FinalHeadCommitID != candidate.preparedPlan.FinalHeadCommitID {
			return nil, fmt.Errorf("%w: mixed prepared flush plans", ErrIngestRepairRequired)
		}
	}
	if existing != nil && hasFresh {
		return nil, fmt.Errorf("%w: cannot extend a prepared flush", ErrIngestRepairRequired)
	}
	if existing != nil && !hasPreparedCommit {
		plans := make([]*IngestPreparedRequest, len(entries))
		for _, candidate := range candidates {
			plans[candidate.index] = candidate.preparedPlan
		}
		return plans, nil
	}

	flushID := ""
	if existing != nil {
		flushID = existing.FlushID
		if base.Version != existing.BaseVersion || base.HeadCommitID != existing.BaseHeadCommitID {
			return nil, fmt.Errorf("%w: prepared flush base changed", ErrIngestRepairRequired)
		}
		if final.Version != existing.FinalVersion || final.HeadCommitID != existing.FinalHeadCommitID {
			return nil, fmt.Errorf("%w: prepared flush result changed", ErrIngestRepairRequired)
		}
	} else {
		var err error
		flushID, err = mutationID(ctx, "ingest_batch")
		if err != nil {
			return nil, err
		}
	}

	plans := make([]*IngestPreparedRequest, len(entries))
	for _, candidate := range candidates {
		if candidate.preparedPlan != nil {
			if candidate.preparedPlan.DataHash != final.DataHash {
				return nil, fmt.Errorf("%w: prepared graph digest changed", ErrIngestRepairRequired)
			}
			plans[candidate.index] = candidate.preparedPlan
			continue
		}
		plan := &IngestPreparedRequest{
			FlushID:           flushID,
			BaseVersion:       base.Version,
			BaseHeadCommitID:  base.HeadCommitID,
			FinalVersion:      final.Version,
			FinalHeadCommitID: final.HeadCommitID,
			Result:            candidate.result,
			DataHash:          final.DataHash,
			StartedAt:         candidate.started,
		}
		if candidate.changed {
			commit := candidate.commit
			plan.Commit = &commit
		}
		plans[candidate.index] = plan
	}
	return plans, nil
}

func validatePreparedIngestResults(candidates []*ingestBatchCandidate) error {
	for _, candidate := range candidates {
		if candidate.preparedPlan == nil || candidate.preparedPlan.Commit == nil {
			continue
		}
		result := candidate.result
		durable := candidate.preparedPlan.Result
		// Alias decisions precede graph conflicts in the durable result. The
		// prepared commit already has canonical fields, so replay cannot emit
		// that prefix again. Restore only this missing prefix; every recomputed
		// field, graph conflict, commit identity and graph digest must still agree.
		missing := len(durable.Conflicts) - len(result.Conflicts)
		if candidate.prepared && missing > 0 && durable.Suppressed-result.Suppressed == missing {
			aliases := true
			for _, conflict := range durable.Conflicts[:missing] {
				if conflict.AliasField == "" {
					aliases = false
					break
				}
			}
			if aliases {
				result.Conflicts = append(append([]IngestConflict(nil), durable.Conflicts[:missing]...), result.Conflicts...)
				result.Suppressed += missing
			}
		}
		actual, err := json.Marshal(result)
		if err != nil {
			return err
		}
		expected, err := json.Marshal(candidate.preparedPlan.Result)
		if err != nil {
			return err
		}
		if string(actual) != string(expected) {
			return fmt.Errorf("%w: prepared ingest result changed", ErrIngestRepairRequired)
		}
		candidate.result = result
	}
	return nil
}

func applyCommitResultToIngest(result *IngestResult, request IngestRequest, commitResult CommitResult) {
	result.Version = commitResult.Version
	result.Skipped = commitResult.Skipped
	result.SkipReason = ingestSkipReasonForCommit(commitResult)
	result.Suppressed = len(commitResult.Suppressed)
	result.Conflicts = append(result.Conflicts, ingest.IngestConflicts(request, commitResult.Suppressed)...)
}

func markIngestCandidateFailure(candidate *ingestBatchCandidate, commitErr error) {
	ingest.MarkIngestResultFailure(&candidate.result, candidate.request, candidate.appliedIndices, commitErr)
}

func appendGraphMutations(target *graph.Mutations, source graph.Mutations) {
	target.UpsertCITypes = append(target.UpsertCITypes, source.UpsertCITypes...)
	target.DeleteCITypes = append(target.DeleteCITypes, source.DeleteCITypes...)
	target.UpsertRelationTypes = append(target.UpsertRelationTypes, source.UpsertRelationTypes...)
	target.DeleteRelationTypes = append(target.DeleteRelationTypes, source.DeleteRelationTypes...)
	target.UpsertEntities = append(target.UpsertEntities, source.UpsertEntities...)
	target.DeleteEntities = append(target.DeleteEntities, source.DeleteEntities...)
	target.DeleteEntityRequests = append(target.DeleteEntityRequests, source.DeleteEntityRequests...)
	target.MarkSourceStale = append(target.MarkSourceStale, source.MarkSourceStale...)
	target.UpsertEdges = append(target.UpsertEdges, source.UpsertEdges...)
	target.DeleteEdges = append(target.DeleteEdges, source.DeleteEdges...)
	target.DeleteEdgeRequests = append(target.DeleteEdgeRequests, source.DeleteEdgeRequests...)
	target.MergeEntities = append(target.MergeEntities, source.MergeEntities...)
	target.SplitEntities = append(target.SplitEntities, source.SplitEntities...)
}
