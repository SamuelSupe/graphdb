package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

func (s *IngestService) appendTerminalBatch(items []*ingestPending, results []IngestResult) (int, error) {
	retryItems, err := s.appendTerminalBatchPending(items, results)
	return len(items) - len(retryItems), err
}

func (s *IngestService) appendTerminalBatchPending(items []*ingestPending, results []IngestResult) ([]*ingestPending, error) {
	if len(items) != len(results) {
		return items, fmt.Errorf("terminal ingest result count mismatch")
	}
	records := make([]ingestWALBatchRecord, 0, len(items))
	// Preparation has not finalized any prefix; a failure here retries all items.
	for index, pending := range items {
		result := results[index]
		if result.Failed > 0 && result.Applied == 0 {
			if failureStore, ok := s.store.(ingestAttemptFailureStore); ok {
				if err := failureStore.PersistIngestAttemptFailure(
					s.runCtx,
					s.config.OwnerID,
					pending.envelope.TenantID,
					pending.envelope.Request,
					result,
					pending.envelope.AcceptedAt,
					time.Now().UTC(),
				); err != nil {
					return items, fmt.Errorf("persist ingest attempt failure: %w", err)
				}
			}
			payload, err := pendingStatePayload(
				pending, IngestWALFailed, IngestStateFailed, &result, "",
			)
			if err != nil {
				return items, err
			}
			records = append(records, ingestWALBatchRecord{kind: IngestWALFailed, payload: payload, id: pending.envelope.RecordID})
			continue
		}
		if result.SkipReason == IngestSkipReasonIdempotentReplay {
			if resolver, ok := s.store.(ingestAttemptFailureResolver); ok {
				if err := resolver.ResolveIngestAttemptFailure(
					s.runCtx,
					s.config.OwnerID,
					pending.envelope.TenantID,
					pending.envelope.Request,
					result,
					pending.envelope.AcceptedAt,
					time.Now().UTC(),
				); err != nil {
					return items, fmt.Errorf("resolve ingest attempt failure: %w", err)
				}
			}
		}
		finalized, err := pendingStatePayload(
			pending, IngestWALFinalized, IngestStateCommitted, &result, "",
		)
		if err != nil {
			return items, err
		}
		records = append(records, ingestWALBatchRecord{kind: IngestWALFinalized, payload: finalized, id: pending.envelope.RecordID})
	}

	responses := s.appendWALBatchWithPrune(records)
	var (
		succeededItems   []*ingestPending
		succeededResults []IngestResult
		retryItems       []*ingestPending
		firstErr         error
	)
	for index, response := range responses {
		if response.err != nil {
			if firstErr == nil {
				firstErr = response.err
			}
			retryItems = append(retryItems, items[index])
			continue
		}
		succeededItems = append(succeededItems, items[index])
		succeededResults = append(succeededResults, results[index])
	}
	if len(succeededItems) > 0 {
		s.completePendingBatch(succeededItems, succeededResults)
	}
	return retryItems, firstErr
}

func (s *IngestService) appendWALBatchWithPrune(records []ingestWALBatchRecord) []ingestWALAppendResponse {
	checkpointGeneration := s.wal.checkpointGeneration.Load()
	responses := s.wal.appendBatch(s.runCtx, records)
	retryIndexes := make([]int, 0)
	for index, response := range responses {
		if errors.Is(response.err, ErrIngestWALFull) {
			retryIndexes = append(retryIndexes, index)
		}
	}
	if len(retryIndexes) > 0 {
		if err := s.prune(s.runCtx, checkpointGeneration); err == nil {
			retryRecords := make([]ingestWALBatchRecord, len(retryIndexes))
			for index, recordIndex := range retryIndexes {
				retryRecords[index] = records[recordIndex]
			}
			retried := s.wal.appendBatch(s.runCtx, retryRecords)
			for index, recordIndex := range retryIndexes {
				responses[recordIndex] = retried[index]
			}
		}
	}
	var highest uint64
	for _, response := range responses {
		if response.err == nil {
			highest = max(highest, response.result.LSN)
		}
	}
	if highest > 0 {
		s.mu.Lock()
		s.highestLSN = max(s.highestLSN, highest)
		s.mu.Unlock()
	}
	return responses
}

func (s *IngestService) appendPreparedBatchState(
	ctx context.Context,
	items []*ingestPending,
	plans []*IngestPreparedRequest,
) error {
	if len(items) != len(plans) {
		return fmt.Errorf("prepared ingest plan count mismatch")
	}
	envelopes := make([]walPreparedEnvelope, 0, len(items))
	recordIDs := make([]string, 0, len(items))
	planBytes := make(map[string]int64, len(items))
	for index, pending := range items {
		if plans[index] == nil {
			continue
		}
		s.mu.Lock()
		previous := pending.envelope.Prepared
		s.mu.Unlock()
		if previous != nil {
			// Recovery reconstructs this same durable plan. Retrying a failed
			// publication or metadata write must not consume WAL space again.
			if !reflect.DeepEqual(previous, plans[index]) {
				return fmt.Errorf("%w: durable prepared ingest plan changed", ErrIngestRepairRequired)
			}
			continue
		}
		encoded, err := json.Marshal(plans[index])
		if err != nil {
			return err
		}
		planBytes[pending.envelope.RecordID] = int64(len(encoded))
		envelopes = append(envelopes, walPreparedEnvelope{
			RecordID: pending.envelope.RecordID,
			Prepared: plans[index],
		})
		recordIDs = append(recordIDs, pending.envelope.RecordID)
	}
	if len(envelopes) == 0 {
		return nil
	}
	payload, err := json.Marshal(walPreparedBatchEnvelope{Items: envelopes})
	if err != nil {
		return err
	}
	checkpointGeneration := s.wal.checkpointGeneration.Load()
	appendResult, err := s.wal.append(ctx, IngestWALPrepared, payload, recordIDs)
	if errors.Is(err, ErrIngestWALFull) {
		if pruneErr := s.prune(ctx, checkpointGeneration); pruneErr == nil {
			appendResult, err = s.wal.append(ctx, IngestWALPrepared, payload, recordIDs)
		}
		if len(items) == 1 && errors.Is(err, ErrIngestWALFull) {
			// A checkpoint still needs the accepted request before the plan can
			// be written. Diagnose an impossible single request separately from
			// pressure caused by other requests that may finish later.
			request := items[0].envelope
			request.AcceptedLSN = 0
			accepted, encodeErr := json.Marshal(request)
			if encodeErr != nil {
				return encodeErr
			}
			const overhead = ingestWALHeaderBytes + ingestWALChecksumBytes
			minimum := min(items[0].bytes, int64(len(accepted)+overhead)) + int64(len(payload)+overhead)
			limit := s.config.WAL.MaxBytes - s.config.WAL.terminalReserveBytes()
			if minimum > limit {
				return fmt.Errorf("%w: accepted request and prepared plan need at least %d bytes, WAL preparation limit is %d", ErrIngestWALRecordTooLarge, minimum, limit)
			}
			if !s.hasOtherActiveTenant(items[0].envelope.TenantID) {
				appendResult, err = s.wal.appendHeadOfLine(ctx, IngestWALPrepared, payload, recordIDs)
			}
		}
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.highestLSN = max(s.highestLSN, appendResult.LSN)
	for index, pending := range items {
		if plans[index] == nil {
			continue
		}
		current := s.activeByStatus[ingestStatusKey(
			pending.envelope.TenantID,
			pending.envelope.Request.Source,
			pending.envelope.Request.CollectorID,
			pending.envelope.Request.BatchID,
		)]
		if current == nil || current.envelope.RecordID != pending.envelope.RecordID {
			s.mu.Unlock()
			return fmt.Errorf("prepared ingest request is no longer active")
		}
		current.bytes += planBytes[current.envelope.RecordID]
		s.pendingBytes += planBytes[current.envelope.RecordID]
		current.envelope.State = IngestStatePrepared
		current.envelope.Prepared = plans[index]
		current.envelope.Result = &plans[index].Result
		current.envelope.Error = ""
		current.state = IngestStatePrepared
		current.err = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *IngestService) hasOtherActiveTenant(tenantID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pending := range s.active {
		if pending.envelope.TenantID != tenantID && pending.state != IngestStateFailed {
			return true
		}
	}
	return false
}

func pendingStatePayload(pending *ingestPending, kind IngestWALRecordType, state string, _ *IngestResult, errorMessage string) ([]byte, error) {
	envelope := walPendingStateEnvelope{
		RecordID: pending.envelope.RecordID,
		State:    state,
		Error:    errorMessage,
	}
	if kind == IngestWALFinalized || kind == IngestWALFailed {
		envelope.FinishedAt = time.Now().UTC()
	}
	return json.Marshal(envelope)
}
