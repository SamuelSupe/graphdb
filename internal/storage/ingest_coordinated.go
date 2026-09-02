package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *TenantStore) ingestCoordinated(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	saveFailures bool,
) (IngestResult, error) {
	result, _, _ := buildIngestMutations(request)
	if coordinatedIngestBatchAliasKey(request) == "" || result.Applied == 0 ||
		(ingestRequestAtomic(request) && result.Failed > 0) {
		return s.ingestCoordinatedLegacy(ctx, tenantID, request, saveFailures)
	}
	return s.ingestCoordinatedWithBatchAlias(ctx, tenantID, request, saveFailures)
}

func (s *TenantStore) ingestCoordinatedWithBatchAlias(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	saveFailures bool,
) (IngestResult, error) {
	entry := IngestBatchEntry{Request: request, AcceptedAt: time.Now().UTC()}
	attempts := max(1, s.CoordinatorRetryLimit+1)
	for attempt := 0; attempt < attempts; attempt++ {
		results, err := s.ingestDurableBatchWithHooks(
			ctx, tenantID, []IngestBatchEntry{entry}, IngestBatchHooks{}, saveFailures,
		)
		if err == nil {
			if len(results) != 1 {
				return IngestResult{}, fmt.Errorf("coordinated ingest result count %d, want 1", len(results))
			}
			return results[0], nil
		}
		if !errors.Is(err, ErrConflict) &&
			!errors.Is(err, ErrWriteConflict) &&
			!errors.Is(err, ErrTaskLeaseHeld) &&
			!errors.Is(err, ErrIdempotencyInProgress) {
			return IngestResult{}, err
		}
		if attempt+1 == attempts {
			return IngestResult{}, err
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return IngestResult{}, err
		}
	}
	return IngestResult{}, ErrWriteConflict
}

func (s *TenantStore) ingestCoordinatedLegacy(
	ctx context.Context,
	tenantID string,
	request IngestRequest,
	saveFailures bool,
) (IngestResult, error) {
	started := time.Now().UTC()
	result, mutations, appliedIndices := buildIngestMutations(request)
	result.BatchID = request.BatchID
	result.Cursor = request.Cursor
	if ingestRequestAtomic(request) && result.Failed > 0 {
		markIngestResultFailure(
			&result,
			request,
			appliedIndices,
			fmt.Errorf("%w: one or more ingest items are invalid", ErrIngestAtomicValidation),
		)
	}
	collector := &CollectorStateUpdate{
		Source:      request.Source,
		CollectorID: request.CollectorID,
		BatchID:     request.BatchID,
		Cursor:      request.Cursor,
	}
	var commitResult CommitResult
	var err error
	commitAttempted := !ingestRequestAtomic(request) || result.Failed == 0
	if commitAttempted {
		commitResult, err = s.CommitWithReport(ctx, tenantID, mutations, CommitOptions{
			ExpectedVersion:          request.ExpectedVersion,
			IdempotencyKey:           coordinatedIngestIdempotencyKey(request),
			WriteBackpressureChecked: true,
			collectorState:           collector,
			ingestPreconditions:      request.Preconditions,
			ingestAcceptedAt:         started,
			rejectSuppressed:         ingestRequestAtomic(request),
		})
	}
	if err != nil {
		if errors.Is(err, ErrCoordinatorUnavailable) ||
			errors.Is(err, ErrWriteConflict) ||
			errors.Is(err, ErrIdempotencyInProgress) {
			return IngestResult{}, err
		}
		markIngestResultFailure(&result, request, appliedIndices, err)
	} else if commitAttempted {
		if commitResult.IdempotentReplay {
			if previous, ok, loadErr := s.loadIngestRecord(ctx, tenantID, request); loadErr != nil {
				return IngestResult{}, loadErr
			} else if ok {
				replayed := previous.Result
				replayed.Skipped = true
				replayed.SkipReason = IngestSkipReasonIdempotentReplay
				if err := s.repairIngestMetadataAfterSkip(
					ctx, tenantID, previous, saveFailures,
				); err != nil {
					return replayed, err
				}
				return replayed, nil
			}
		}
		result.Version = commitResult.Version
		result.Skipped = commitResult.Skipped || commitResult.IdempotentReplay
		result.SkipReason = ingestSkipReasonForCommit(commitResult)
		result.Suppressed = len(commitResult.Suppressed)
		result.Conflicts = append(result.Conflicts, ingestConflicts(request, commitResult.Suppressed)...)
	}
	finished := time.Now().UTC()
	record := IngestBatchRecord{
		Request: request, Result: result, StartedAt: started, FinishedAt: finished,
	}
	var metadataErr error
	if err := s.saveIngestBatch(ctx, tenantID, record); err != nil {
		metadataErr = errors.Join(metadataErr, fmt.Errorf("save ingest batch: %w", err))
	}
	if saveFailures && result.Failed > 0 {
		if err := s.saveDeadLetter(ctx, tenantID, request, result); err != nil {
			metadataErr = errors.Join(metadataErr, fmt.Errorf("save dead letter: %w", err))
		}
	}
	if metadataErr != nil {
		return result, metadataErr
	}
	return result, nil
}

func coordinatedIngestIdempotencyKey(request IngestRequest) string {
	identity := request.IdempotencyKey
	if identity == "" {
		identity = request.BatchID
	}
	return "ingest/" + request.Source + "/" + request.CollectorID + "/" + identity
}
