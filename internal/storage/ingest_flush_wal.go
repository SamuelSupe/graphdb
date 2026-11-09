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
	if len(items) != len(results) {
		return 0, fmt.Errorf("terminal ingest result count mismatch")
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
					return 0, fmt.Errorf("persist ingest attempt failure: %w", err)
				}
			}
			payload, err := pendingStatePayload(
				pending, IngestWALFailed, IngestStateFailed, &result, "",
			)
			if err != nil {
				return 0, err
			}
			records = append(records, ingestWALBatchRecord{kind: IngestWALFailed, payload: payload})
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
					return 0, fmt.Errorf("resolve ingest attempt failure: %w", err)
				}
			}
		}
		finalized, err := pendingStatePayload(
			pending, IngestWALFinalized, IngestStateCommitted, &result, "",
		)
		if err != nil {
			return 0, err
		}
		records = append(records, ingestWALBatchRecord{kind: IngestWALFinalized, payload: finalized})
	}

	responses := s.appendWALBatchWithPrune(records)
	for index, response := range responses {
		if response.err != nil {
			s.completePendingBatch(items[:index], results[:index])
			return index, response.err
		}
	}
	s.completePendingBatch(items, results)
	return len(items), nil
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
	}
	if len(envelopes) == 0 {
		return nil
	}
	payload, err := json.Marshal(walPreparedBatchEnvelope{Items: envelopes})
	if err != nil {
		return err
	}
	checkpointGeneration := s.wal.checkpointGeneration.Load()
	appendResult, err := s.wal.Append(ctx, IngestWALPrepared, payload)
	if errors.Is(err, ErrIngestWALFull) {
		if pruneErr := s.prune(ctx, checkpointGeneration); pruneErr == nil {
			appendResult, err = s.wal.Append(ctx, IngestWALPrepared, payload)
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
