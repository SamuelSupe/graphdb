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
	started := time.Now().UTC()
	result, mutations, appliedIndices := buildIngestMutations(request)
	result.BatchID = request.BatchID
	result.Cursor = request.Cursor
	pendingApplied := result.Applied
	collector := &CollectorStateUpdate{
		Source:      request.Source,
		CollectorID: request.CollectorID,
		BatchID:     request.BatchID,
		Cursor:      request.Cursor,
	}
	commitResult, err := s.CommitWithReport(ctx, tenantID, mutations, CommitOptions{
		IdempotencyKey:           coordinatedIngestIdempotencyKey(request),
		WriteBackpressureChecked: true,
		collectorState:           collector,
	})
	if err != nil {
		if errors.Is(err, ErrCoordinatorUnavailable) ||
			errors.Is(err, ErrWriteConflict) ||
			errors.Is(err, ErrIdempotencyInProgress) {
			return IngestResult{}, err
		}
		result.Failed += pendingApplied
		result.Applied = 0
		for _, index := range appliedIndices {
			result.Failures = append(result.Failures, IngestFailure{
				Index: index, ExternalID: request.Items[index].ExternalID, Error: err.Error(),
			})
		}
		result.Conflicts = append(result.Conflicts, IngestConflict{Message: err.Error()})
	} else {
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
