package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/admission"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
	"go.opentelemetry.io/otel/attribute"
)

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

	preparedEntries := make([]IngestBatchEntry, len(entries))
	validatePlans := hooks.Prepared != nil
	for index, entry := range entries {
		validatePlans = validatePlans || entry.Prepared != nil
		request, err := ingest.PrepareRequestWithPathValidation(tenantID, entry.Request, entry.Prepared == nil && commitIdentityEncoding(ctx) != 0)
		if err != nil {
			return nil, err
		}
		entry.Request = request
		if entry.AcceptedAt.IsZero() {
			entry.AcceptedAt = mutationTime(ctx)
		}
		preparedEntries[index] = entry
	}
	acceptedGeneration, err := ingestBatchAcceptedGeneration(preparedEntries)
	if err != nil {
		return nil, err
	}

	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return nil, err
	}
	if err := s.checkAcceptedWALBackpressure(ctx, tenantID, false); err != nil {
		return nil, err
	}
	var foregroundUnlock func()
	{
		viewCtx, releaseView, err := s.ReadViewContext(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		defer releaseView()
		ctx = viewCtx
		unlock, err := s.lockTenantForeground(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		foregroundUnlock = unlock
		defer func() {
			if foregroundUnlock != nil {
				foregroundUnlock()
			}
		}()
		if err := s.validateLocalIngestGeneration(ctx, tenantID, acceptedGeneration); err != nil {
			return nil, err
		}
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
		result, mutations, appliedIndices := ingest.BuildIngestMutations(request)
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
		if ingest.IngestRequestAtomic(request) && candidate.result.Failed > 0 {
			ingest.MarkIngestResultFailure(
				&candidate.result,
				request,
				appliedIndices,
				fmt.Errorf("%w: one or more ingest items are invalid", ErrIngestAtomicValidation),
			)
			candidate.mutations = graph.Mutations{}
			candidate.metadataOnly = true
		}

		if previous, ok, err := s.loadIngestRecord(ctx, tenantID, request); err != nil {
			if errors.Is(err, ErrIngestIdentityConflict) {
				markIngestCandidateFailure(candidate, err)
				candidate.metadataOnly = true
				candidate.skipMetadata = true
				if entry.Prepared != nil {
					if entry.Prepared.Commit != nil || entry.Prepared.Result.BatchID != request.BatchID ||
						entry.Prepared.Result.Failed == 0 || entry.Prepared.Result.Applied != 0 {
						return nil, fmt.Errorf("%w: prepared ingest outcome conflicts with saved identity", ErrIngestRepairRequired)
					}
					// The conflict is already prepared; only its attempt-failure
					// marker or terminal WAL record may still need persistence.
					candidate.preparedPlan = entry.Prepared
					candidate.result = entry.Prepared.Result
				}
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

		if entry.Prepared != nil {
			if entry.Prepared.Result.BatchID != request.BatchID {
				return results, fmt.Errorf("%w: prepared ingest batch identity changed", ErrIngestRepairRequired)
			}
			published, err := s.preparedIngestPublished(ctx, tenantID, *entry.Prepared)
			if err != nil {
				return results, err
			}
			if published {
				candidate.result = entry.Prepared.Result
				candidate.metadataOnly = true
			} else if entry.Prepared.Commit != nil {
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
		loaded          loadedGraph
		finalGraph      *graph.Graph
		finalManifest   Manifest
		commitItems     []commitSegmentItem
		preparedSegment preparedCommitSegment
		logicalBytes    int64
		fallback        bool
		indexWork       *commitIndexUpdate
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
		} else if finalGraph != loaded.Graph {
			loaded.Graph = finalGraph
			s.setWriteCache(tenantID, loaded)
		}
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
	if validatePlans {
		plans, err := s.preparedIngestBatchPlans(ctx, tenantID, preparedEntries, candidates, loaded.Manifest, finalManifest)
		if err != nil {
			return results, err
		}
		if hooks.Prepared != nil {
			if err := hooks.Prepared(ctx, plans); err != nil {
				return results, err
			}
		}
	}
	if len(commitItems) > 0 {
		indexWork, err = s.publishIngestBatch(ctx, tenantID, loaded, finalGraph, finalManifest, preparedSegment, commitItems, candidates, logicalBytes)
		if err != nil {
			return results, err
		}
		if hooks.Published != nil {
			hooks.Published()
		}
	}
	if foregroundUnlock != nil {
		foregroundUnlock()
		foregroundUnlock = nil
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
	if indexWork != nil {
		s.finishCommitIndexUpdate(ctx, tenantID, indexWork, nil)
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
	// Resource pressure defers proposals on the leader. Rejecting a committed
	// flush would stop application on every replica and prevent compaction.
	if IsReplicatedContext(ctx) || s.Backpressure == nil || changedCommits == 0 {
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
	return admission.NewError([]BackpressureReason{{
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
