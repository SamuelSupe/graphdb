package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"go.opentelemetry.io/otel/attribute"
)

var ErrIngestRepairRequired = errors.New("ingest WAL repair required")

type IngestBatchEntry struct {
	Request    IngestRequest
	AcceptedAt time.Time
	FinishedAt time.Time
	Prepared   *IngestPreparedRequest
}

type IngestPreparedRequest struct {
	FlushID           string        `json:"flush_id"`
	BaseVersion       int64         `json:"base_version"`
	BaseHeadCommitID  string        `json:"base_head_commit_id,omitempty"`
	FinalVersion      int64         `json:"final_version"`
	FinalHeadCommitID string        `json:"final_head_commit_id,omitempty"`
	Result            IngestResult  `json:"result"`
	Commit            *graph.Commit `json:"commit,omitempty"`
	DataMD5           string        `json:"data_md5,omitempty"`
	StartedAt         time.Time     `json:"started_at"`
}

type IngestBatchHooks struct {
	Prepared      func(context.Context, []*IngestPreparedRequest) error
	Published     func(context.Context, []IngestPublishedRecord) error
	DeferMetadata bool
	Stats         func(IngestBatchStats)
}

type IngestBatchStats struct {
	LogicalCommits    int
	Segments          int
	ManifestPublishes int
	ExactDedup        int
	Fallback          bool
}

type ingestBatchCandidate struct {
	index          int
	request        IngestRequest
	started        time.Time
	result         IngestResult
	appliedIndices []int
	mutations      graph.Mutations
	policyReport   graph.ApplyReport
	relationSchema RelationSchemaCatalog
	schemaMeta     ObjectMeta
	commit         graph.Commit
	report         graph.ApplyReport
	changed        bool
	metadataOnly   bool
	resultManifest Manifest
	prepared       bool
	preparedPlan   *IngestPreparedRequest
}

// IngestDurableBatch flushes one tenant queue under one tenant lock. Each
// request keeps its own logical commit and result while changed commits share a
// commit segment and one manifest publication.
func (s *TenantStore) IngestDurableBatch(ctx context.Context, tenantID string, entries []IngestBatchEntry) ([]IngestResult, error) {
	return s.IngestDurableBatchWithHooks(ctx, tenantID, entries, IngestBatchHooks{})
}

func (s *TenantStore) IngestDurableBatchWithHooks(
	ctx context.Context,
	tenantID string,
	entries []IngestBatchEntry,
	hooks IngestBatchHooks,
) (results []IngestResult, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest.batch",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.batch.requests", len(entries)),
	)
	defer func() { endStorageSpan(span, err) }()
	if len(entries) == 0 {
		return nil, nil
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if s.coordinated() {
		return nil, fmt.Errorf("durable ingest batching requires local coordination")
	}
	if err := s.ensureIngestMetadataWriteMode(ctx, tenantID); err != nil {
		return nil, err
	}
	preparedEntries := make([]IngestBatchEntry, len(entries))
	for index, entry := range entries {
		request, err := PrepareIngestRequest(tenantID, entry.Request)
		if err != nil {
			return nil, err
		}
		entry.Request = request
		if entry.AcceptedAt.IsZero() {
			entry.AcceptedAt = time.Now().UTC()
		}
		preparedEntries[index] = entry
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return nil, err
	}
	if err := s.checkWriteBackpressure(ctx, tenantID, false); err != nil {
		return nil, err
	}
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return nil, err
	}
	ctx, err = s.bindCurrentWriterFence(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return nil, err
	}
	if err := s.checkWriteBackpressure(ctx, tenantID, true); err != nil {
		return nil, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return nil, err
	}

	results = make([]IngestResult, len(preparedEntries))
	candidates := make([]*ingestBatchCandidate, 0, len(preparedEntries))
	for index, entry := range preparedEntries {
		request := entry.Request
		if previous, ok, err := s.loadIngestRecord(ctx, tenantID, request); err != nil {
			return nil, err
		} else if ok {
			result := previous.Result
			result.Skipped = true
			result.SkipReason = IngestSkipReasonIdempotentReplay
			results[index] = result
			if err := s.repairIngestMetadataAfterSkip(ctx, tenantID, previous, true); err != nil {
				return results, err
			}
			continue
		}
		result, mutations, appliedIndices := buildIngestMutations(request)
		result.BatchID = request.BatchID
		result.Cursor = request.Cursor
		candidate := &ingestBatchCandidate{
			index:          index,
			request:        request,
			started:        entry.AcceptedAt,
			result:         result,
			appliedIndices: appliedIndices,
			mutations:      mutations,
			metadataOnly:   result.Applied == 0,
		}
		if entry.Prepared != nil {
			if entry.Prepared.Result.BatchID != request.BatchID {
				return results, fmt.Errorf("%w: prepared ingest batch identity changed", ErrIngestRepairRequired)
			}
			published, err := s.preparedIngestPublished(ctx, tenantID, *entry.Prepared)
			if err != nil {
				return results, err
			}
			if entry.Prepared.Commit == nil || published {
				candidate.result = entry.Prepared.Result
				candidate.metadataOnly = true
			} else {
				candidate.commit = *entry.Prepared.Commit
				candidate.mutations = candidate.commit.Mutations
				candidate.prepared = true
			}
			candidate.preparedPlan = entry.Prepared
		}
		candidates = append(candidates, candidate)
	}

	mutationCandidates := make([]*ingestBatchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.metadataOnly {
			mutations := candidate.mutations
			policyReport := graph.ApplyReport{}
			if !candidate.prepared {
				mutations, policyReport, err = s.resolveSourcePolicy(ctx, tenantID, mutations)
				if err != nil {
					return results, err
				}
			}
			mutations, relationSchema, schemaMeta, err := s.prepareRelationSchemaMutations(ctx, tenantID, mutations)
			if err != nil {
				return results, err
			}
			candidate.mutations = mutations
			candidate.policyReport = policyReport
			candidate.relationSchema = relationSchema
			candidate.schemaMeta = schemaMeta
			mutationCandidates = append(mutationCandidates, candidate)
		}
	}

	var (
		loaded        loadedGraph
		finalGraph    *graph.Graph
		finalManifest Manifest
		commitItems   []commitSegmentItem
		logicalBytes  int64
		fallback      bool
	)
	if len(mutationCandidates) > 0 {
		loaded, err = s.loadForWriteLocked(ctx, tenantID)
		if err != nil {
			return results, err
		}
		applyCtx, applySpan := startStorageSpan(ctx, "graphdb.storage.ingest.batch_apply",
			tenantTraceAttr(tenantID),
			attribute.Int("graphdb.ingest.batch_apply.requests", len(mutationCandidates)),
		)
		finalGraph, commitItems, fallback, err = s.applyIngestBatchCandidates(applyCtx, tenantID, loaded, mutationCandidates)
		applySpan.SetAttributes(
			attribute.Int("graphdb.ingest.batch_apply.logical_commits", len(commitItems)),
			attribute.Bool("graphdb.ingest.batch_apply.fallback", fallback),
		)
		endStorageSpan(applySpan, err)
		if err != nil {
			return results, err
		}
		if err := s.checkIngestProjectedCommitTail(ctx, tenantID, loaded.Manifest, len(commitItems)); err != nil {
			return results, err
		}
		finalManifest = loaded.Manifest
		if len(commitItems) > 0 {
			finalManifest, logicalBytes, err = s.prepareIngestBatchManifest(ctx, tenantID, loaded, finalGraph, commitItems)
			if err != nil {
				return results, err
			}
		}
	}
	stats := ingestBatchStats(candidates, commitItems, fallback)
	span.SetAttributes(
		attribute.Int("graphdb.ingest.batch.logical_commits", stats.LogicalCommits),
		attribute.Int("graphdb.ingest.batch.segments", stats.Segments),
		attribute.Int("graphdb.ingest.batch.manifest_publishes", stats.ManifestPublishes),
		attribute.Int("graphdb.ingest.batch.exact_dedup", stats.ExactDedup),
		attribute.Bool("graphdb.ingest.batch.fallback", stats.Fallback),
	)
	if hooks.Prepared != nil {
		plans, err := s.preparedIngestBatchPlans(ctx, tenantID, preparedEntries, candidates, loaded.Manifest, finalManifest)
		if err != nil {
			return results, err
		}
		if err := hooks.Prepared(ctx, plans); err != nil {
			return results, err
		}
	}
	if len(commitItems) > 0 {
		if err := s.publishIngestBatch(ctx, tenantID, loaded, finalGraph, finalManifest, commitItems, mutationCandidates, logicalBytes); err != nil {
			return results, err
		}
	}
	if hooks.Stats != nil {
		hooks.Stats(stats)
	}

	published := make([]IngestPublishedRecord, 0, len(candidates))
	for _, candidate := range candidates {
		results[candidate.index] = candidate.result
		finishedAt := preparedEntries[candidate.index].FinishedAt
		if finishedAt.IsZero() {
			finishedAt = time.Now().UTC()
		}
		published = append(published, IngestPublishedRecord{
			Index: candidate.index,
			Record: IngestBatchRecord{
				TenantID:   tenantID,
				Request:    candidate.request,
				Result:     candidate.result,
				StartedAt:  candidate.started,
				FinishedAt: finishedAt,
			},
		})
	}
	if hooks.Published != nil {
		if err := hooks.Published(ctx, published); err != nil {
			return results, err
		}
	}
	if hooks.DeferMetadata {
		return results, nil
	}

	metadataCtx, metadataSpan := startStorageSpan(ctx, "graphdb.storage.ingest.finalize_metadata",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.metadata.requests", len(candidates)),
	)
	var metadataErr error
	for _, item := range published {
		record := item.Record
		if err := s.saveIngestResultMetadata(metadataCtx, tenantID, record.Request, record.Result, record.StartedAt, record.FinishedAt, true); err != nil {
			metadataErr = errors.Join(metadataErr, err)
		}
	}
	endStorageSpan(metadataSpan, metadataErr)
	if metadataErr != nil {
		return results, metadataErr
	}
	return results, nil
}

func (s *TenantStore) checkIngestProjectedCommitTail(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	changedCommits int,
) error {
	if s.Backpressure == nil || changedCommits == 0 {
		return nil
	}
	config, err := s.effectiveBackpressureConfig(ctx, tenantID)
	if err != nil {
		return err
	}
	projected := manifestCommitTailLength(manifest) + changedCommits
	if config.MaxCommitTail <= 0 || projected <= config.MaxCommitTail {
		return nil
	}
	return newBackpressureError([]BackpressureReason{{
		Code:      "commit_tail_too_long",
		Current:   float64(projected),
		Threshold: float64(config.MaxCommitTail),
		Message:   "compact required before ingest flush",
	}}, config.RetryAfter)
}

func ingestBatchStats(
	candidates []*ingestBatchCandidate,
	items []commitSegmentItem,
	fallback bool,
) IngestBatchStats {
	stats := IngestBatchStats{
		LogicalCommits: len(items),
		Fallback:       fallback,
	}
	if len(items) > 0 {
		stats.Segments = 1
		stats.ManifestPublishes = 1
	}
	for _, candidate := range candidates {
		if candidate.preparedPlan == nil &&
			candidate.result.Skipped &&
			candidate.result.SkipReason == IngestSkipReasonLogicalNoop {
			stats.ExactDedup++
		}
	}
	return stats
}

func (s *TenantStore) applyIngestBatchCandidates(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) (*graph.Graph, []commitSegmentItem, bool, error) {
	commits := make([]graph.Commit, len(candidates))
	nextVersion := loaded.Manifest.Version
	for index, candidate := range candidates {
		nextVersion++
		if candidate.prepared {
			if candidate.commit.TenantID != tenantID || candidate.commit.Version != nextVersion {
				return nil, nil, false, fmt.Errorf("%w: prepared commit no longer follows the base manifest", ErrIngestRepairRequired)
			}
		} else {
			commitID, err := newCommitID()
			if err != nil {
				return nil, nil, false, err
			}
			candidate.commit = graph.Commit{
				LayoutVersion: CurrentObjectLayoutVersion,
				ID:            commitID,
				TenantID:      tenantID,
				Version:       nextVersion,
				CreatedAt:     time.Now().UTC(),
				Mutations:     candidate.mutations,
			}
		}
		commits[index] = candidate.commit
	}
	if next, reports, err := loaded.Graph.ApplyCommitBatchStorageCopyWithOptions(commits, nil); err == nil {
		valid := true
		for index, candidate := range candidates {
			if err := validateRelationSchemaGraph(next, candidate.relationSchema); err != nil {
				valid = false
				break
			}
			reports[index].Suppressed = append(candidate.policyReport.Suppressed, reports[index].Suppressed...)
		}
		if valid && s.checkQuotaAfterApply(ctx, tenantID, loaded.Graph, next) == nil {
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
			if err := validatePreparedIngestResults(candidates); err != nil {
				return nil, nil, false, err
			}
			return next, items, false, nil
		}
	}
	next, items, err := s.applyIngestBatchCandidatesIsolated(ctx, tenantID, loaded, candidates)
	return next, items, true, err
}

func (s *TenantStore) applyIngestBatchCandidatesIsolated(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) (*graph.Graph, []commitSegmentItem, error) {
	current := loaded.Graph
	currentHeadID := loaded.Manifest.HeadCommitID
	currentUpdatedAt := loaded.Manifest.UpdatedAt
	items := make([]commitSegmentItem, 0, len(candidates))
	for _, candidate := range candidates {
		expectedVersion := current.Version + 1
		if candidate.prepared {
			if candidate.commit.Version != expectedVersion {
				return nil, nil, fmt.Errorf("%w: prepared commit version is no longer contiguous", ErrIngestRepairRequired)
			}
		} else {
			candidate.commit.Version = expectedVersion
			candidate.commit.CreatedAt = time.Now().UTC()
		}
		report, entityNoop, err := current.PreviewStorageEntityNoop(candidate.commit)
		next := current
		if err == nil && !entityNoop {
			next, report, err = current.ApplyCommitStorageCopyWithOptions(candidate.commit, graph.ApplyOptions{})
		}
		if err == nil {
			if relationSchemaCommitCanValidateIncrementally(false, candidate.relationSchema, current.Version) {
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
		candidate.report = report
		if !report.Changed {
			if candidate.prepared {
				return nil, nil, fmt.Errorf("%w: prepared commit became a logical no-op", ErrIngestRepairRequired)
			}
			candidate.result.Version = current.Version
			candidate.result.Skipped = true
			candidate.result.SkipReason = IngestSkipReasonLogicalNoop
			candidate.result.Suppressed = len(report.Suppressed)
			candidate.result.Conflicts = append(candidate.result.Conflicts, ingestConflicts(candidate.request, report.Suppressed)...)
			candidate.resultManifest = Manifest{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      tenantID,
				Version:       current.Version,
				HeadCommitID:  currentHeadID,
				UpdatedAt:     currentUpdatedAt,
				DataMD5:       loaded.DataMD5,
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

func (s *TenantStore) prepareIngestBatchManifest(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	finalGraph *graph.Graph,
	newItems []commitSegmentItem,
) (Manifest, int64, error) {
	nextMD5, logicalBytes, err := finalGraph.ContentMD5WithLogicalSize()
	if err != nil {
		return Manifest{}, 0, err
	}
	items, err := s.ingestBatchSegmentItems(ctx, tenantID, loaded, newItems)
	if err != nil {
		return Manifest{}, 0, err
	}
	ref, err := s.commitSegmentRef(tenantID, items)
	if err != nil {
		return Manifest{}, 0, err
	}
	last := newItems[len(newItems)-1].Commit
	manifest := loaded.Manifest
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.Version = last.Version
	manifest.HeadCommitID = last.ID
	manifest.CommitSegments = append(append([]CommitSegmentRef(nil), manifest.CommitSegments...), ref)
	manifest.CommitKeys = nil
	manifest.UpdatedAt = last.CreatedAt
	manifest.DataMD5 = nextMD5
	return manifest, logicalBytes, nil
}

func (s *TenantStore) publishIngestBatch(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	finalGraph *graph.Graph,
	manifest Manifest,
	newItems []commitSegmentItem,
	candidates []*ingestBatchCandidate,
	logicalBytes int64,
) (err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest.publish",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.publish.logical_commits", len(newItems)),
		attribute.Int64("graphdb.ingest.publish.base_version", loaded.Manifest.Version),
		attribute.Int64("graphdb.ingest.publish.final_version", manifest.Version),
	)
	defer func() { endStorageSpan(span, err) }()
	items, err := s.ingestBatchSegmentItems(ctx, tenantID, loaded, newItems)
	if err != nil {
		return err
	}
	ref, err := s.putCommitSegment(ctx, tenantID, items)
	if err != nil {
		return err
	}
	expected := manifest.CommitSegments[len(manifest.CommitSegments)-1]
	if ref != expected {
		return fmt.Errorf("prepared commit segment changed before publish")
	}
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, loaded.Meta)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return err
	}
	s.setWriteCache(tenantID, loadedGraph{
		Graph:      finalGraph,
		Manifest:   manifest,
		Meta:       meta,
		DataMD5:    manifest.DataMD5,
		CommitTail: emptyCommitTailCache(),
		CacheBytes: writeCacheBytesForGraphWithCommitTail(finalGraph, logicalBytes, emptyCommitTailCache()),
	})
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
	indexErr := s.updateIndexesAfterCommit(ctx, tenantID, loaded.Graph, finalGraph, aggregateMutations, aggregateReport, manifest.Version)
	if indexErr != nil {
		// Indexes are derived state. The manifest remains authoritative and the
		// regular repair path can rebuild them.
		return nil
	}
	return nil
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

func (s *TenantStore) commitSegmentRef(tenantID string, items []commitSegmentItem) (CommitSegmentRef, error) {
	if len(items) == 0 {
		return CommitSegmentRef{}, fmt.Errorf("empty commit segment")
	}
	payload, err := marshalCommitSegmentPayload(items)
	if err != nil {
		return CommitSegmentRef{}, err
	}
	first := items[0].Commit.Version
	last := items[len(items)-1].Commit.Version
	hash := objectContentHash(payload)
	return CommitSegmentRef{
		Key:          s.commitSegmentKey(tenantID, first, last, hash),
		Codec:        commitSegmentCodecParquet,
		FirstVersion: first,
		LastVersion:  last,
		Count:        len(items),
		ContentHash:  hash,
	}, nil
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
		flushID, err = newCommitID()
		if err != nil {
			return nil, err
		}
	}

	plans := make([]*IngestPreparedRequest, len(entries))
	for _, candidate := range candidates {
		if candidate.preparedPlan != nil {
			if candidate.preparedPlan.DataMD5 != final.DataMD5 {
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
			DataMD5:           final.DataMD5,
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
		actual, err := json.Marshal(candidate.result)
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
	}
	return nil
}

func applyCommitResultToIngest(result *IngestResult, request IngestRequest, commitResult CommitResult) {
	result.Version = commitResult.Version
	result.Skipped = commitResult.Skipped
	result.SkipReason = ingestSkipReasonForCommit(commitResult)
	result.Suppressed = len(commitResult.Suppressed)
	result.Conflicts = append(result.Conflicts, ingestConflicts(request, commitResult.Suppressed)...)
}

func markIngestCandidateFailure(candidate *ingestBatchCandidate, commitErr error) {
	pendingApplied := candidate.result.Applied
	candidate.result.Failed += pendingApplied
	candidate.result.Applied = 0
	for _, index := range candidate.appliedIndices {
		candidate.result.Failures = append(candidate.result.Failures, IngestFailure{
			Index:      index,
			ExternalID: candidate.request.Items[index].ExternalID,
			Error:      commitErr.Error(),
		})
	}
	candidate.result.Conflicts = append(candidate.result.Conflicts, IngestConflict{Message: commitErr.Error()})
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
