package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
)

func (s *TenantStore) Ingest(ctx context.Context, tenantID string, request IngestRequest) (IngestResult, error) {
	return s.ingest(ctx, tenantID, request, true, false)
}

// IngestDurable gives a WAL-replayed request a storage-side idempotency fence.
// It closes the crash window between manifest publication and ingest metadata.
func (s *TenantStore) IngestDurable(ctx context.Context, tenantID string, request IngestRequest) (IngestResult, error) {
	return s.ingest(ctx, tenantID, request, true, true)
}

func (s *TenantStore) ingest(ctx context.Context, tenantID string, request IngestRequest, saveFailures bool, durableCommit bool) (IngestResult, error) {
	request, err := ingest.PrepareRequestWithPathValidation(tenantID, request, commitIdentityEncoding(ctx) != 0)
	if err != nil {
		return IngestResult{}, err
	}
	if err := s.validateTaskIngestGeneration(ctx, tenantID); err != nil {
		return IngestResult{}, err
	}
	resumeIngest := func() {}
	if s.localFileStore() != nil && s.ingestBarrier != nil {
		resume, err := s.pauseLocalIngest(ctx, tenantID)
		if err != nil {
			return IngestResult{}, err
		}
		resumeIngest = sync.OnceFunc(resume)
		defer resumeIngest()
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return IngestResult{}, pressure
		}
		return IngestResult{}, err
	}
	if err := s.checkWriteBackpressure(ctx, tenantID, false); err != nil {
		return IngestResult{}, err
	}

	ctx, releaseView, err := s.ReadViewContext(ctx, tenantID)
	if err != nil {
		return IngestResult{}, err
	}
	defer releaseView()
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return IngestResult{}, err
	}
	resumeIngest()
	var indexWork *commitIndexUpdate
	foregroundLockHeld := true
	defer func() {
		if foregroundLockHeld {
			unlock()
		}
		if indexWork != nil {
			s.finishCommitIndexUpdate(ctx, tenantID, indexWork, nil)
		}
	}()
	if err := s.validateTaskIngestGeneration(ctx, tenantID); err != nil {
		return IngestResult{}, err
	}
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return IngestResult{}, pressure
		}
		return IngestResult{}, err
	}
	boundCtx, err := s.bindCurrentWriterFence(ctx, tenantID)
	if err != nil {
		return IngestResult{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return IngestResult{}, err
	}
	if err := s.checkWriteBackpressure(ctx, tenantID, true); err != nil {
		return IngestResult{}, err
	}
	started := mutationTime(ctx)
	if previousRecord, ok, err := s.loadIngestRecord(ctx, tenantID, request); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return IngestResult{}, pressure
		}
		return IngestResult{}, err
	} else if ok {
		previous := previousRecord.Result
		previous.Skipped = true
		previous.SkipReason = IngestSkipReasonIdempotentReplay
		if err := s.repairIngestMetadataAfterSkip(ctx, tenantID, previousRecord, saveFailures); err != nil {
			return previous, err
		}
		return previous, nil
	}
	result, mutations, appliedIndices := ingest.BuildIngestMutations(request)
	result.BatchID = request.BatchID
	result.Cursor = request.Cursor
	if ingest.IngestRequestAtomic(request) && result.Failed > 0 {
		ingest.MarkIngestResultFailure(
			&result,
			request,
			appliedIndices,
			fmt.Errorf("%w: one or more ingest items are invalid", ErrIngestAtomicValidation),
		)
	}
	pendingApplied := result.Applied
	if pendingApplied > 0 {
		commitResult, err := s.commitIngestMutationsLocked(ctx, tenantID, request, mutations, durableCommit, started)
		indexWork = commitResult.indexUpdate
		commitResult.indexUpdate = nil
		if err != nil {
			if errors.Is(err, ErrBackpressure) {
				return IngestResult{}, err
			}
			if pressure := s.objectStoreBackpressureError(err); pressure != nil {
				return IngestResult{}, pressure
			}
			ingest.MarkIngestResultFailure(&result, request, appliedIndices, err)
		} else {
			result.Version = commitResult.Version
			result.Skipped = commitResult.Skipped
			result.SkipReason = ingestSkipReasonForCommit(commitResult)
			result.Suppressed = len(commitResult.Suppressed)
			result.Conflicts = append(result.Conflicts, ingest.IngestConflicts(request, commitResult.Suppressed)...)
		}
	}
	if indexWork != nil && s.localFileStore() != nil && s.enqueueLocalIngestIndexUpdate(ctx, tenantID, indexWork, false) {
		indexWork = nil
	} else {
		s.enqueueCommitIndexUpdate(ctx, tenantID, indexWork)
	}
	if foregroundLockHeld {
		unlock()
		foregroundLockHeld = false
	}
	finished := mutationTime(ctx)
	metadataErr := s.saveIngestResultMetadata(ctx, tenantID, request, result, started, finished, saveFailures)
	if metadataErr != nil {
		return result, metadataErr
	}
	if result.Failed > 0 {
		return result, nil
	}
	return result, nil
}

func (s *TenantStore) saveIngestResultMetadata(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	result IngestResult,
	started time.Time,
	finished time.Time,
	saveFailures bool,
) error {
	{
		var metadataErr error
		if err := s.saveIngestBatch(ctx, tenantID, IngestBatchRecord{
			Request: request, Result: result, StartedAt: started, FinishedAt: finished,
		}); err != nil {
			metadataErr = errors.Join(metadataErr, fmt.Errorf("save ingest batch: %w", err))
		}
		if err := s.saveCollectorStatus(ctx, tenantID, request, result, started, finished); err != nil {
			metadataErr = errors.Join(metadataErr, fmt.Errorf("save collector status: %w", err))
		}
		if saveFailures && result.Failed > 0 {
			if err := s.saveDeadLetter(ctx, tenantID, request, result); err != nil {
				metadataErr = errors.Join(metadataErr, fmt.Errorf("save dead letter: %w", err))
			}
		}
		return metadataErr
	}
}

func (s *TenantStore) PersistIngestFailure(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	result IngestResult,
	started time.Time,
	finished time.Time,
) error {
	return s.saveIngestBatch(ctx, tenantID, IngestBatchRecord{
		TenantID:   tenantID,
		Request:    request,
		Result:     result,
		StartedAt:  started,
		FinishedAt: finished,
	})
}

func (s *TenantStore) ResolveIngestFailure(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	result IngestResult,
	started time.Time,
	finished time.Time,
) (IngestResult, error) {
	previous, ok, err := s.loadIngestRecord(ctx, tenantID, request)
	if err != nil {
		if errors.Is(err, ErrIngestIdentityConflict) || errors.Is(err, ErrIdempotencyConflict) {
			return result, nil
		}
		return result, err
	}
	if ok {
		return replayedIngestResult(previous.Result), nil
	}
	if err := s.PersistIngestFailure(ctx, tenantID, request, result, started, finished); err != nil {
		if !errors.Is(err, ErrConflict) &&
			!errors.Is(err, ErrIngestIdentityConflict) &&
			!errors.Is(err, ErrIdempotencyConflict) {
			return result, err
		}
		previous, ok, loadErr := s.loadIngestRecord(ctx, tenantID, request)
		if loadErr != nil {
			if errors.Is(loadErr, ErrIngestIdentityConflict) || errors.Is(loadErr, ErrIdempotencyConflict) {
				return result, nil
			}
			return result, loadErr
		}
		if ok {
			return replayedIngestResult(previous.Result), nil
		}
		return result, err
	}
	return result, nil
}

func replayedIngestResult(result IngestResult) IngestResult {
	result.Skipped = true
	result.SkipReason = IngestSkipReasonIdempotentReplay
	return result
}

func (s *TenantStore) commitIngestMutationsLocked(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	mutations graph.Mutations,
	durable bool,
	acceptedAt time.Time,
) (CommitResult, error) {
	opts := CommitOptions{
		ExpectedVersion:     request.ExpectedVersion,
		ingestPreconditions: request.Preconditions,
		ingestAcceptedAt:    acceptedAt,
		rejectSuppressed:    ingest.IngestRequestAtomic(request),
	}
	if !durable {
		return s.commitWithRetryLocked(ctx, tenantID, mutations, opts)
	}
	commitRequest := DirectCommitRequest{
		ExpectedVersion: request.ExpectedVersion,
		IdempotencyKey:  durableIngestCommitKey(tenantID, request),
		Mutations:       mutations,
	}
	reservation, replay, err := s.beginDirectCommit(ctx, tenantID, commitRequest, mutationTime(ctx))
	if err != nil {
		return CommitResult{}, err
	}
	if replay != nil {
		return *replay, nil
	}
	opts.directCommit = reservation
	result, err := s.commitWithRetryLocked(ctx, tenantID, mutations, opts)
	if err != nil {
		return CommitResult{}, err
	}
	if err := s.completeDirectCommit(ctx, reservation, result, mutationTime(ctx)); err != nil {
		return result, fmt.Errorf("complete durable ingest commit: %w", err)
	}
	return result, nil
}

func durableIngestCommitKey(tenantID string, request IngestRequest) string {
	identity := tenantID + "\x00" + request.Source + "\x00" + request.CollectorID + "\x00"
	if request.IdempotencyKey != "" {
		identity += "idempotency\x00" + request.IdempotencyKey
	} else {
		identity += "batch\x00" + request.BatchID
	}
	sum := sha256.Sum256([]byte(identity))
	return "ingest-wal-" + hex.EncodeToString(sum[:])
}

func ingestSkipReasonForCommit(result CommitResult) string {
	if result.IdempotentReplay {
		return IngestSkipReasonIdempotentReplay
	}
	if result.Skipped {
		return IngestSkipReasonLogicalNoop
	}
	return ""
}
