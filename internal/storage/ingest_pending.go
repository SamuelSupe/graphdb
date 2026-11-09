package storage

import (
	"context"
	"errors"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
)

func (s *IngestService) setPendingRetry(pending *ingestPending, err error) {
	s.mu.Lock()
	pending.state = IngestStateRetrying
	pending.err = err
	pending.retryAttempts++

	s.mu.Unlock()
}

func (s *IngestService) ingestRetryDelay(pending *ingestPending) time.Duration {
	var pressure *BackpressureError
	if errors.As(pending.err, &pressure) {
		if pressure.RetryAfter > 0 {
			return pressure.RetryAfter
		}
		return s.config.RetryInterval
	}
	delay := s.config.RetryInterval
	maximum := max(30*time.Second, delay)
	for attempt := 1; attempt < min(pending.retryAttempts, 8) && delay < maximum; attempt++ {
		delay = min(maximum, delay*2)
	}
	jitterPercent := 80 + int((pending.acceptedLSN+uint64(pending.retryAttempts*17))%41)
	return delay * time.Duration(jitterPercent) / 100
}

func (s *IngestService) completePendingBatch(items []*ingestPending, results []IngestResult) {
	if len(items) == 0 {
		return
	}
	s.mu.Lock()
	for index, pending := range items {
		state := IngestStateCommitted
		if results[index].Failed > 0 && results[index].Applied == 0 {
			state = IngestStateFailed
		}
		s.completePendingStateLocked(pending, results[index], nil, state)
	}
	shouldPrune := s.finishPendingCompletionsLocked()
	s.mu.Unlock()
	if shouldPrune {
		_ = s.prune(context.Background())
	}
}

func (s *IngestService) completePendingState(pending *ingestPending, result IngestResult, err error, state string) {
	s.mu.Lock()
	s.completePendingStateLocked(pending, result, err, state)
	shouldPrune := s.finishPendingCompletionsLocked()
	s.mu.Unlock()
	if shouldPrune {
		_ = s.prune(context.Background())
	}
}

func (s *IngestService) completePendingStateLocked(pending *ingestPending, result IngestResult, err error, state string) {
	pending.state = state
	pending.result = result
	pending.err = err
	pending.finishedAt = time.Now().UTC()
	s.pendingBytes -= pending.bytes
	delete(s.active, ingestRequestIdentity(pending.envelope.TenantID, pending.envelope.Request))
	statusKey := ingestStatusKey(
		pending.envelope.TenantID,
		pending.envelope.Request.Source,
		pending.envelope.Request.CollectorID,
		pending.envelope.Request.BatchID,
	)
	if s.activeByStatus[statusKey] == pending {
		if state == IngestStateFailed {
			s.cacheFailedStatusLocked(statusKey, pending)
		} else {
			delete(s.activeByStatus, statusKey)
			if s.config.Observer != nil {
				s.config.Observer.RecordIngestQueueCache("eviction")
			}
		}
	}
	// Keep identity and FullSync immutable for callers and the scheduler.
	// Wait keeps the result; the request body already has a durable record.
	pending.envelope.Request.Items = nil
	pending.envelope.Request.Preconditions = nil
	pending.envelope.Request.IdempotencyKey = ""
	pending.envelope.Request.Cursor = ""
	pending.envelope.Request.StaleAction = ""
	pending.envelope.Request.StaleKind = ""
	pending.envelope.Request.FailureMode = ""
	pending.envelope.Request.ExpectedVersion = nil
	pending.envelope.Prepared = nil
	pending.envelope.Result = nil
	if pending.envelope.AcceptedAt.Equal(s.oldestPending) {
		s.oldestPending = time.Time{}
	}
	s.completedBytes += pending.bytes
	pending.completedOnce.Do(func() { close(pending.done) })
}

func (s *IngestService) finishPendingCompletionsLocked() bool {
	if s.oldestPending.IsZero() {
		for _, active := range s.active {
			if s.oldestPending.IsZero() || active.envelope.AcceptedAt.Before(s.oldestPending) {
				s.oldestPending = active.envelope.AcceptedAt
			}
		}
	}
	shouldPrune := s.walFull || s.completedBytes >= max(s.pendingBytes, s.config.WAL.SegmentBytes)
	if shouldPrune {
		s.completedBytes = 0
	}
	s.observeQueueLocked()
	return shouldPrune
}

func terminalIngestFlushError(err error) bool {
	return errors.Is(err, ErrTenantDisabled) ||
		errors.Is(err, ErrTenantDeleted) ||
		errors.Is(err, ErrIngestIdentityConflict) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrIngestWALRecordTooLarge) ||
		errors.Is(err, ErrIngestWALRecordExceedsSegment)
}

func failedIngestResult(request IngestRequest, failure error) IngestResult {
	result, _, appliedIndices := ingest.BuildIngestMutations(request)
	result.BatchID = request.BatchID
	result.Cursor = request.Cursor
	result.Failed += result.Applied
	result.Applied = 0
	if result.Failed == 0 {
		result.Failed = 1
	}
	result.ErrorCode = ingest.IngestErrorCode(failure)
	for _, index := range appliedIndices {
		result.Failures = append(result.Failures, IngestFailure{
			Index:      index,
			ExternalID: request.Items[index].ExternalID,
			Error:      failure.Error(),
		})
	}
	result.Conflicts = append(result.Conflicts, IngestConflict{Message: failure.Error()})
	return result
}

func (s *IngestService) prune(ctx context.Context, generation ...uint64) error {
	err := s.wal.prune(ctx, 0, true, generation...)
	if err != nil {
		s.recordError(err)
	}
	if err == nil {
		s.mu.Lock()
		s.walFull = false
		s.mu.Unlock()
	}
	return err
}
