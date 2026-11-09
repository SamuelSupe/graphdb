package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// WaitCommitted skips the normal batching delay through this acceptance.
// Earlier batches still run first, and a failed attempt keeps its retry delay.
func (s *IngestService) WaitCommitted(ctx context.Context, acceptance IngestAcceptance) (IngestResult, error) {
	if pending := acceptance.pending; pending != nil {
		select {
		case <-acceptance.completion:
		default:
			select {
			case s.forceCh <- ingestForceRequest{tenantID: pending.envelope.TenantID, throughLSN: acceptance.acceptedLSN, pending: pending}:
			case <-acceptance.completion:
			case <-ctx.Done():
			case <-s.runCtx.Done():
			}
		}
	}
	return s.Wait(ctx, acceptance)
}

// Wait returns a completed outcome or stops when the caller or service stops.
// A service error does not revoke durable acceptance; recovery can still finish
// the batch. Already completed outcomes remain available after shutdown.
func (s *IngestService) Wait(ctx context.Context, acceptance IngestAcceptance) (IngestResult, error) {
	if acceptance.completion == nil {
		return IngestResult{}, fmt.Errorf("invalid ingest acceptance")
	}
	select {
	case <-acceptance.completion:
	case <-ctx.Done():
		return IngestResult{}, ctx.Err()
	case <-s.runCtx.Done():
		select {
		case <-acceptance.completion:
		default:
			s.mu.Lock()
			err := s.fatalErr
			s.mu.Unlock()
			if err == nil {
				err = ErrIngestWALClosed
			}
			return IngestResult{}, err
		}
	}
	pending := acceptance.pending
	if pending == nil || pending.envelope.RecordID != acceptance.recordID {
		return IngestResult{}, fmt.Errorf("invalid ingest acceptance")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return pending.result, pending.err
}

func (s *IngestService) Status(ctx context.Context, tenantID string, source string, collectorID string, batchID string) (IngestBatchStatus, error) {
	key := ingestStatusKey(tenantID, source, collectorID, batchID)
	s.mu.Lock()
	if pending := s.activeByStatus[key]; pending != nil {
		status := statusFromPending(pending, s.config.OwnerID, s.config.WAL.Durability)
		if s.config.Observer != nil {
			s.config.Observer.RecordIngestQueueCache("hit")
		}
		s.mu.Unlock()
		return status, nil
	}
	s.mu.Unlock()
	var record IngestBatchRecord
	err := ErrNotFound
	if ValidateIngestStorageIdentifiers(IngestRequest{Source: source, CollectorID: collectorID, BatchID: batchID}) == nil {
		record, err = s.store.GetIngestBatch(ctx, tenantID, source, collectorID, batchID)
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return IngestBatchStatus{}, err
	}
	failureStore, durableFailures := s.store.(ingestAttemptFailureStore)
	if durableFailures {
		failure, failureErr := failureStore.GetIngestAttemptFailure(
			ctx, s.config.OwnerID, tenantID, source, collectorID, batchID,
		)
		if failureErr != nil && !errors.Is(failureErr, ErrNotFound) {
			return IngestBatchStatus{}, failureErr
		}
		if failureErr == nil && (errors.Is(err, ErrNotFound) || !record.FinishedAt.After(failure.FinishedAt)) {
			return ingestBatchStatusFromRecord(
				s.config.OwnerID, tenantID, source, collectorID, batchID,
				failure, IngestStateFailed,
			), nil
		}
	}
	if err != nil {
		return IngestBatchStatus{}, err
	}
	state := IngestStateCommitted
	if record.Result.Failed > 0 && record.Result.Applied == 0 {
		state = IngestStateFailed
	}
	return ingestBatchStatusFromRecord(
		s.config.OwnerID, tenantID, source, collectorID, batchID,
		record, state,
	), nil
}

func ingestBatchStatusFromRecord(
	ownerID string,
	tenantID string,
	source string,
	collectorID string,
	batchID string,
	record IngestBatchRecord,
	state string,
) IngestBatchStatus {
	result := record.Result
	return IngestBatchStatus{
		WriterID:    ownerID,
		TenantID:    tenantID,
		Source:      source,
		CollectorID: collectorID,
		BatchID:     batchID,
		State:       state,
		Durability:  "durable",
		AcceptedAt:  record.StartedAt,
		FinishedAt:  record.FinishedAt,
		Result:      &result,
	}
}

func (s *IngestService) Readiness() IngestServiceReadiness {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := IngestServiceReadiness{
		WriterID:     s.config.OwnerID,
		Ready:        !s.closed && s.fatalErr == nil,
		Writable:     !s.closed && s.fatalErr == nil && !s.walFull && s.pendingBytes < s.config.QueueMemoryBytes,
		Recovered:    true,
		Pending:      len(s.active),
		PendingBytes: s.pendingBytes,
		LastError:    s.lastError,
	}
	status.Oldest = s.oldestPending
	for _, pending := range s.active {
		if status.LastError == "" && pending.state == IngestStateRetrying && pending.err != nil {
			status.LastError = pending.err.Error()
		}
	}
	return status
}

func (s *IngestService) WriterID() string {
	return s.config.OwnerID
}

func (s *IngestService) ObserveMetrics() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeQueueLocked()
}

func acceptanceFromPending(pending *ingestPending, ownerID string, durability string) IngestAcceptance {
	reportedDurability := "memory"
	if durability == IngestWALDurabilitySync {
		reportedDurability = "durable"
	}
	return IngestAcceptance{
		WriterID:       ownerID,
		BatchID:        pending.envelope.Request.BatchID,
		Source:         pending.envelope.Request.Source,
		CollectorID:    pending.envelope.Request.CollectorID,
		State:          pending.state,
		Durability:     reportedDurability,
		AcceptedAt:     pending.envelope.AcceptedAt,
		EstimatedFlush: pending.estimated,
		acceptedLSN:    pending.acceptedLSN,
		recordID:       pending.envelope.RecordID,
		completion:     pending.done,
		pending:        pending,
	}
}

func statusFromPending(pending *ingestPending, ownerID string, durability string) IngestBatchStatus {
	reportedDurability := "memory"
	if durability == IngestWALDurabilitySync {
		reportedDurability = "durable"
	}
	status := IngestBatchStatus{
		WriterID:        ownerID,
		TenantID:        pending.envelope.TenantID,
		Source:          pending.envelope.Request.Source,
		CollectorID:     pending.envelope.Request.CollectorID,
		BatchID:         pending.envelope.Request.BatchID,
		State:           pending.state,
		Durability:      reportedDurability,
		AcceptedLSN:     pending.acceptedLSN,
		AcceptedAt:      pending.envelope.AcceptedAt,
		EstimatedFlush:  pending.estimated,
		FinishedAt:      pending.finishedAt,
		RecoveryPending: pending.state != IngestStateCommitted && pending.state != IngestStateFailed,
	}
	if pending.state == IngestStateCommitted || pending.state == IngestStateFailed {
		result := pending.result
		status.Result = &result
	}
	if pending.err != nil {
		status.LastError = pending.err.Error()
	}
	return status
}

func ingestRequestIdentity(tenantID string, request IngestRequest) string {
	key := request.BatchID
	kind := "batch"
	if request.IdempotencyKey != "" {
		key = request.IdempotencyKey
		kind = "idempotency"
	}
	return tenantID + "\x00" + request.Source + "\x00" + request.CollectorID + "\x00" + kind + "\x00" + key
}

func ingestRecordID(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func ingestStatusKey(tenantID string, source string, collectorID string, batchID string) string {
	return tenantID + "\x00" + source + "\x00" + collectorID + "\x00" + batchID
}
