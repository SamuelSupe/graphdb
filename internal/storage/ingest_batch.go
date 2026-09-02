package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrIngestRepairRequired   = errors.New("ingest WAL repair required")
	errIngestGenerationFenced = errors.New("ingest WAL tenant generation fenced")
)

const legacyUnboundIngestGeneration int64 = -1

type IngestBatchEntry struct {
	Request            IngestRequest
	AcceptedAt         time.Time
	AcceptedGeneration int64
	Prepared           *IngestPreparedRequest
}

type IngestPreparedRequest struct {
	FlushID                  string        `json:"flush_id"`
	BaseVersion              int64         `json:"base_version"`
	BaseHeadCommitID         string        `json:"base_head_commit_id,omitempty"`
	BaseHeadRevision         int64         `json:"base_head_revision,omitempty"`
	BaseGeneration           int64         `json:"base_generation,omitempty"`
	BaseWriteContextRevision int64         `json:"base_write_context_revision,omitempty"`
	FinalVersion             int64         `json:"final_version"`
	FinalHeadCommitID        string        `json:"final_head_commit_id,omitempty"`
	Result                   IngestResult  `json:"result"`
	Commit                   *graph.Commit `json:"commit,omitempty"`
	DataMD5                  string        `json:"data_md5,omitempty"`
	StartedAt                time.Time     `json:"started_at"`
}

type IngestBatchHooks struct {
	Prepared  func(context.Context, []*IngestPreparedRequest) error
	Published func()
	Stats     func(IngestBatchStats)
}

type IngestBatchStats struct {
	LogicalCommits    int
	Segments          int
	ManifestPublishes int
	ExactDedup        int
	CASMerged         int
	Fallback          bool
}

type ingestBatchCandidate struct {
	index            int
	request          IngestRequest
	started          time.Time
	result           IngestResult
	appliedIndices   []int
	mutations        graph.Mutations
	policyReport     graph.ApplyReport
	relationSchema   RelationSchemaCatalog
	schemaMeta       ObjectMeta
	commit           graph.Commit
	report           graph.ApplyReport
	changed          bool
	metadataOnly     bool
	skipMetadata     bool
	resultManifest   Manifest
	prepared         bool
	preparedPlan     *IngestPreparedRequest
	reservation      *directCommitReservation
	batchReservation *directCommitReservation
	casMerged        bool
}

func ingestBatchAcceptedGeneration(entries []IngestBatchEntry) (int64, error) {
	expected := normalizedIngestAcceptedGeneration(entries[0])
	for _, entry := range entries[1:] {
		generation := normalizedIngestAcceptedGeneration(entry)
		if generation != expected {
			return 0, fmt.Errorf("%w: mixed accepted tenant generations", ErrIngestRepairRequired)
		}
	}
	return expected, nil
}

func normalizedIngestAcceptedGeneration(entry IngestBatchEntry) int64 {
	if entry.AcceptedGeneration != 0 {
		return entry.AcceptedGeneration
	}
	if entry.Prepared != nil && entry.Prepared.BaseGeneration > 0 {
		return entry.Prepared.BaseGeneration
	}
	return 0
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
) ([]IngestResult, error) {
	return s.ingestDurableBatchWithHooks(ctx, tenantID, entries, hooks, true)
}

func (s *TenantStore) ingestDurableBatchWithHooks(
	ctx context.Context,
	tenantID string,
	entries []IngestBatchEntry,
	hooks IngestBatchHooks,
	saveFailures bool,
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
	if s.coordinated() && s.RequireCoordinationMarker && !s.coordinationMarkerVerified.Load() {
		if err := s.EnsurePostgresMarker(ctx); err != nil {
			return nil, err
		}
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
	acceptedGeneration, err := ingestBatchAcceptedGeneration(preparedEntries)
	if err != nil {
		return nil, err
	}
	if s.coordinated() {
		if err := s.validateCoordinatedIngestGeneration(ctx, tenantID, acceptedGeneration); err != nil {
			return nil, err
		}
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return nil, err
	}
	if err := s.checkAcceptedWALBackpressure(ctx, tenantID, false); err != nil {
		return nil, err
	}
	if !s.coordinated() {
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
	}
	if err := s.checkAcceptedWALBackpressure(ctx, tenantID, true); err != nil {
		return nil, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return nil, err
	}

	results = make([]IngestResult, len(preparedEntries))
	candidates := make([]*ingestBatchCandidate, 0, len(preparedEntries))
	for index, entry := range preparedEntries {
		request := entry.Request
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
		if ingestRequestAtomic(request) && candidate.result.Failed > 0 {
			markIngestResultFailure(
				&candidate.result,
				request,
				appliedIndices,
				fmt.Errorf("%w: one or more ingest items are invalid", ErrIngestAtomicValidation),
			)
			candidate.mutations = graph.Mutations{}
			candidate.metadataOnly = true
		}
		if !s.coordinated() {
			if previous, ok, err := s.loadIngestRecord(ctx, tenantID, request); err != nil {
				if errors.Is(err, ErrIngestIdentityConflict) {
					markIngestCandidateFailure(candidate, err)
					candidate.metadataOnly = true
					candidate.skipMetadata = true
					candidates = append(candidates, candidate)
					continue
				}
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
		}
		if entry.Prepared != nil {
			if entry.Prepared.Result.BatchID != request.BatchID {
				return results, fmt.Errorf("%w: prepared ingest batch identity changed", ErrIngestRepairRequired)
			}
			stale := false
			if s.coordinated() {
				stale, err = s.coordinatedPreparedIngestStale(ctx, tenantID, *entry.Prepared)
				if err != nil {
					return results, err
				}
			}
			published := false
			if !stale {
				published, err = s.preparedIngestPublished(ctx, tenantID, *entry.Prepared)
				if err != nil {
					return results, err
				}
			}
			if published {
				candidate.result = entry.Prepared.Result
				candidate.metadataOnly = true
			} else if entry.Prepared.Commit != nil && !stale {
				candidate.commit = *entry.Prepared.Commit
				candidate.mutations = candidate.commit.Mutations
				candidate.prepared = true
			}
			if !stale {
				candidate.preparedPlan = entry.Prepared
			}
		}
		candidates = append(candidates, candidate)
	}
	if s.coordinated() {
		if err := s.reserveCoordinatedIngestCandidates(ctx, tenantID, candidates); err != nil {
			return results, err
		}
	}

	batchCtx := ctx
	var stopPublishSlot func()
	if s.coordinated() && coordinatedBatchHasReservations(candidates) {
		publishCtx, stop, err := s.startCoordinatorIngestPublishSlot(ctx, tenantID)
		if err != nil {
			return results, err
		}
		// The slot only avoids duplicate object-store work. PostgreSQL Head CAS
		// remains authoritative so older writers can coexist during upgrades.
		ctx = publishCtx
		stopPublishSlot = stop
		defer func() {
			if stopPublishSlot != nil {
				stopPublishSlot()
			}
		}()
		if err := s.validateCoordinatedIngestGeneration(ctx, tenantID, acceptedGeneration); err != nil {
			abortErr := s.abortCoordinatedIngestReservations(candidates, err)
			return results, errors.Join(err, abortErr)
		}
		if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
			return results, err
		}
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
		loaded          loadedGraph
		finalGraph      *graph.Graph
		finalManifest   Manifest
		commitItems     []commitSegmentItem
		preparedSegment preparedCommitSegment
		logicalBytes    int64
		fallback        bool
		indexUpdateDone <-chan error
	)
	if len(mutationCandidates) > 0 || coordinatedBatchHasReservations(candidates) {
		loaded, err = s.loadForWriteLocked(ctx, tenantID)
		if err != nil {
			return results, err
		}
		applyCtx, applySpan := startStorageSpan(ctx, "graphdb.storage.ingest.batch_apply",
			tenantTraceAttr(tenantID),
			attribute.Int("graphdb.ingest.batch_apply.requests", len(mutationCandidates)),
		)
		finalGraph = loaded.Graph
		finalManifest = loaded.Manifest
		if len(mutationCandidates) > 0 {
			finalGraph, commitItems, fallback, err = s.applyIngestBatchCandidates(applyCtx, tenantID, loaded, mutationCandidates)
		}
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
		if len(commitItems) > 0 {
			finalManifest, logicalBytes, preparedSegment, err = s.prepareIngestBatchManifest(ctx, tenantID, loaded, finalGraph, commitItems)
			if err != nil {
				return results, err
			}
		}
		prepareCoordinatedMetadataOnlyResults(loaded.Manifest, candidates)
	}
	stats := ingestBatchStats(candidates, commitItems, fallback)
	span.SetAttributes(
		attribute.Int("graphdb.ingest.batch.logical_commits", stats.LogicalCommits),
		attribute.Int("graphdb.ingest.batch.segments", stats.Segments),
		attribute.Int("graphdb.ingest.batch.manifest_publishes", stats.ManifestPublishes),
		attribute.Int("graphdb.ingest.batch.exact_dedup", stats.ExactDedup),
		attribute.Int("graphdb.ingest.batch.cas_merged", stats.CASMerged),
		attribute.Bool("graphdb.ingest.batch.fallback", stats.Fallback),
	)
	if hooks.Prepared != nil {
		plans, err := s.preparedIngestBatchPlans(ctx, tenantID, preparedEntries, candidates, loaded.Manifest, loaded.Meta, finalManifest)
		if err != nil {
			if errors.Is(err, ErrTenantDisabled) || errors.Is(err, ErrTenantDeleted) {
				abortErr := s.abortCoordinatedIngestReservations(candidates, err)
				return results, errors.Join(err, abortErr)
			}
			return results, err
		}
		if err := hooks.Prepared(ctx, plans); err != nil {
			return results, err
		}
	}
	if len(commitItems) > 0 {
		indexUpdateDone, err = s.publishIngestBatch(ctx, tenantID, loaded, finalGraph, finalManifest, preparedSegment, commitItems, candidates, logicalBytes)
		if err != nil {
			return results, err
		}
		if hooks.Published != nil {
			hooks.Published()
		}
	} else if s.coordinated() && coordinatedBatchHasReservations(candidates) {
		if err := s.completeCoordinatedIngestBatch(ctx, tenantID, loaded, candidates); err != nil {
			return results, err
		}
	}
	if stopPublishSlot != nil {
		releasePublishSlot := stopPublishSlot
		stopPublishSlot = nil
		if _, releasedWithPublish := coordinatorIngestPublishStateFromContext(ctx, tenantID); releasedWithPublish {
			// PostgreSQL released the slot in the publication transaction, so this
			// only stops renewal and can stay on the current goroutine.
			releasePublishSlot()
		} else {
			// Older coordinator implementations still release through a separate
			// call; keep that compatibility work off the successful flush path.
			go releasePublishSlot()
		}
		ctx = batchCtx
	}
	if hooks.Stats != nil {
		hooks.Stats(stats)
	}

	metadataCtx, metadataSpan := startStorageSpan(ctx, "graphdb.storage.ingest.finalize_metadata",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.metadata.requests", len(candidates)),
	)
	for _, candidate := range candidates {
		results[candidate.index] = candidate.result
	}
	metadataErr := s.saveIngestBatchResultMetadataWithFailures(metadataCtx, tenantID, candidates, saveFailures)
	if indexUpdateDone != nil {
		<-indexUpdateDone
	}
	endStorageSpan(metadataSpan, metadataErr)
	if metadataErr != nil {
		return results, metadataErr
	}
	if err := s.releaseFailedIngestReservations(candidates); err != nil {
		return results, err
	}
	return results, nil
}

func (s *TenantStore) saveIngestBatchResultMetadata(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
) error {
	return s.saveIngestBatchResultMetadataWithFailures(ctx, tenantID, candidates, true)
}

func (s *TenantStore) saveIngestBatchResultMetadataWithFailures(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
	saveFailures bool,
) error {
	type collectorKey struct {
		source      string
		collectorID string
	}
	type collectorMetadataGroup struct {
		updates       []ingestCollectorStatusUpdate
		records       sync.WaitGroup
		recordsMu     sync.Mutex
		recordsFailed bool
	}
	collectorOrder := make([]collectorKey, 0)
	collectorGroups := make(map[collectorKey]*collectorMetadataGroup)
	recordJobs := make([]func() error, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.skipMetadata {
			continue
		}
		candidate := candidate
		finished := time.Now().UTC()
		key := collectorKey{source: candidate.request.Source, collectorID: candidate.request.CollectorID}
		group := collectorGroups[key]
		if group == nil {
			group = &collectorMetadataGroup{}
			collectorGroups[key] = group
			collectorOrder = append(collectorOrder, key)
		}
		group.records.Add(1)
		recordJobs = append(recordJobs, func() error {
			defer group.records.Done()
			var result error
			if err := s.saveIngestBatch(ctx, tenantID, IngestBatchRecord{
				Request: candidate.request, Result: candidate.result,
				StartedAt: candidate.started, FinishedAt: finished,
			}); err != nil {
				result = errors.Join(result, fmt.Errorf("save ingest batch: %w", err))
			}
			if saveFailures && candidate.result.Failed > 0 {
				if err := s.saveDeadLetter(ctx, tenantID, candidate.request, candidate.result); err != nil {
					result = errors.Join(result, fmt.Errorf("save dead letter: %w", err))
				}
			}
			if result != nil {
				group.recordsMu.Lock()
				group.recordsFailed = true
				group.recordsMu.Unlock()
			}
			return result
		})
		group.updates = append(group.updates, ingestCollectorStatusUpdate{
			request: candidate.request, result: candidate.result,
			started: candidate.started, finished: finished,
		})
	}
	jobs := append(make([]func() error, 0, len(recordJobs)+len(collectorOrder)), recordJobs...)
	for _, key := range collectorOrder {
		group := collectorGroups[key]
		jobs = append(jobs, func() error {
			group.records.Wait()
			group.recordsMu.Lock()
			recordsFailed := group.recordsFailed
			group.recordsMu.Unlock()
			if recordsFailed {
				return nil
			}
			if err := s.saveCollectorStatusBatch(ctx, tenantID, group.updates); err != nil {
				return fmt.Errorf("save collector status: %w", err)
			}
			return nil
		})
	}
	return runIngestMetadataJobs(jobs)
}

func runIngestMetadataJobs(jobs []func() error) error {
	if len(jobs) == 0 {
		return nil
	}
	workerCount := min(len(jobs), 8)
	jobCh := make(chan func() error)
	errCh := make(chan error, len(jobs))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for job := range jobCh {
				if err := job(); err != nil {
					errCh <- err
				}
			}
		}()
	}
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)
	workers.Wait()
	close(errCh)
	var result error
	for err := range errCh {
		result = errors.Join(result, err)
	}
	return result
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
		if candidate.casMerged && candidate.changed {
			stats.CASMerged++
		}
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
		case request.ExpectedVersion != nil && !ingestRequestAtomic(request):
			expected := *request.ExpectedVersion
			for end < len(candidates) {
				next := candidates[end].request
				if next.ExpectedVersion == nil || ingestRequestAtomic(next) || *next.ExpectedVersion != expected {
					break
				}
				end++
			}
		case !ingestRequestNeedsIsolatedApply(request):
			for end < len(candidates) && !ingestRequestNeedsIsolatedApply(candidates[end].request) {
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
			if ingestRequestNeedsIsolatedApply(candidate.request) {
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
		for index, candidate := range candidates {
			reports[index].Suppressed = append(candidate.policyReport.Suppressed, reports[index].Suppressed...)
		}
		if validateIngestBatchRelationSchemas(next, candidates, reports, s.coordinated(), loaded.Manifest.Version) == nil &&
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
	coordinated bool,
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
				incremental: relationSchemaCommitCanValidateIncrementally(coordinated, catalog, baseVersion),
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
	if s.coordinated() {
		return
	}
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
// A coordinated writer must still win the PostgreSQL head CAS before any
// candidate in the cohort becomes visible.
func (s *TenantStore) prepareIngestCASCohort(
	base *graph.Graph,
	candidates []*ingestBatchCandidate,
) ([]*ingestBatchCandidate, bool) {
	if len(candidates) < 2 {
		return nil, false
	}
	expected := int64(0)
	for index, candidate := range candidates {
		if candidate.request.ExpectedVersion == nil || ingestRequestAtomic(candidate.request) {
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
		if err := evaluateIngestPreconditions(base, candidate.request.Preconditions, candidate.started); err != nil {
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
			if err := evaluateIngestPreconditions(current, candidate.request.Preconditions, candidate.started); err != nil {
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
				commitID, err := newCommitID()
				if err != nil {
					return nil, nil, err
				}
				candidate.commit = graph.Commit{
					LayoutVersion: CurrentObjectLayoutVersion,
					ID:            commitID,
					TenantID:      tenantID,
					Mutations:     candidate.mutations,
				}
			}
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
		if ingestRequestAtomic(candidate.request) && len(report.Suppressed) > 0 {
			candidate.result.Suppressed = len(report.Suppressed)
			candidate.result.Conflicts = append(candidate.result.Conflicts, ingestConflicts(candidate.request, report.Suppressed)...)
			markIngestCandidateFailure(candidate, fmt.Errorf("%w: %d mutation conflicts", ErrIngestAtomicSuppressed, len(report.Suppressed)))
			continue
		}
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
		digest, logicalBytes, err := finalGraph.ContentMD5WithLogicalSize()
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
	manifest.DataMD5 = hashed.digest
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
) (indexUpdateDone <-chan error, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest.publish",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.publish.logical_commits", len(newItems)),
		attribute.Int64("graphdb.ingest.publish.base_version", loaded.Manifest.Version),
		attribute.Int64("graphdb.ingest.publish.final_version", manifest.Version),
	)
	defer func() { endStorageSpan(span, err) }()
	ref, err := s.putPreparedCommitSegment(ctx, tenantID, preparedSegment)
	if err != nil {
		return nil, err
	}
	expected := manifest.CommitSegments[len(manifest.CommitSegments)-1]
	if ref != expected {
		return nil, fmt.Errorf("prepared commit segment changed before publish")
	}
	var meta ObjectMeta
	if s.coordinated() {
		meta, err = s.putCoordinatedIngestBatchManifest(ctx, tenantID, manifest, loaded.Meta, candidates)
	} else {
		meta, err = s.putManifestMeta(ctx, tenantID, manifest, loaded.Meta)
	}
	if err != nil {
		s.handleManifestPublishFailureCache(tenantID, loaded, err)
		return nil, err
	}
	s.setWriteCache(tenantID, loadedGraph{
		Graph:      finalGraph,
		Manifest:   manifest,
		Meta:       meta,
		DataMD5:    manifest.DataMD5,
		CommitTail: emptyCommitTailCache(),
		CacheBytes: writeCacheBytesForGraphWithCommitTail(finalGraph, logicalBytes, emptyCommitTailCache()),
	})
	s.advanceIngestBatchRelationSchemaValidation(ctx, tenantID, candidates, manifest.Version)
	span.SetAttributes(
		attribute.Int("graphdb.ingest.publish.segment_commits", ref.Count),
		attribute.Int64("graphdb.ingest.publish.segment_first_version", ref.FirstVersion),
		attribute.Int64("graphdb.ingest.publish.segment_last_version", ref.LastVersion),
	)
	if s.coordinated() {
		return nil, nil
	}
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
	done := make(chan error, 1)
	go func() {
		done <- s.updateIndexesAfterCommit(
			ctx, tenantID, loaded.Graph, finalGraph,
			aggregateMutations, aggregateReport, manifest.Version,
		)
	}()
	return done, nil
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
	baseMeta ObjectMeta,
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
		flushID, err = newCommitID()
		if err != nil {
			return nil, err
		}
	}

	baseToken := coordinatedHeadToken{}
	if s.coordinated() {
		var err error
		baseToken, err = parseCoordinatedHeadToken(baseMeta)
		if err != nil {
			return nil, err
		}
		acceptedGeneration, err := ingestBatchAcceptedGeneration(entries)
		if err != nil {
			return nil, err
		}
		if acceptedGeneration == legacyUnboundIngestGeneration && baseToken.Generation > 1 {
			return nil, fmt.Errorf(
				"%w: %w: tenant %q legacy WAL record is not bound to current generation %d",
				ErrTenantDeleted, errIngestGenerationFenced, tenantID, baseToken.Generation,
			)
		}
		if acceptedGeneration > 0 && baseToken.Generation != acceptedGeneration {
			return nil, fmt.Errorf(
				"%w: %w: tenant %q WAL generation changed from %d to %d",
				ErrTenantDeleted, errIngestGenerationFenced, tenantID, acceptedGeneration, baseToken.Generation,
			)
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
			FlushID:                  flushID,
			BaseVersion:              base.Version,
			BaseHeadCommitID:         base.HeadCommitID,
			BaseHeadRevision:         baseToken.Revision,
			BaseGeneration:           baseToken.Generation,
			BaseWriteContextRevision: baseToken.ContextRevision,
			FinalVersion:             final.Version,
			FinalHeadCommitID:        final.HeadCommitID,
			Result:                   candidate.result,
			DataMD5:                  final.DataMD5,
			StartedAt:                candidate.started,
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
	markIngestResultFailure(&candidate.result, candidate.request, candidate.appliedIndices, commitErr)
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
