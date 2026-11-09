package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *TenantStore) GetIngestAttemptFailure(
	ctx context.Context,
	ownerID string,
	tenantID string,
	source string,
	collectorID string,
	batchID string,
) (IngestBatchRecord, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IngestBatchRecord{}, err
	}
	source = strings.TrimSpace(source)
	collectorID = strings.TrimSpace(collectorID)
	batchID = strings.TrimSpace(batchID)
	if source == "" || collectorID == "" || batchID == "" {
		return IngestBatchRecord{}, fmt.Errorf("source, collector_id, and batch_id are required")
	}
	record, _, err := s.loadIngestRecordWithMeta(
		ctx,
		s.ingestAttemptFailureKey(tenantID, ownerID, source, collectorID, batchID),
	)
	if err != nil {
		return IngestBatchRecord{}, err
	}
	if record.TenantID != tenantID ||
		record.Request.Source != source ||
		record.Request.CollectorID != collectorID ||
		record.Result.BatchID != batchID ||
		record.Result.Failed == 0 ||
		record.Result.Applied != 0 {
		return IngestBatchRecord{}, fmt.Errorf(
			"ingest attempt failure identity mismatch for tenant %q source %q collector %q batch %q",
			tenantID, source, collectorID, batchID,
		)
	}
	return record, nil
}

func (s *TenantStore) PersistIngestAttemptFailure(
	ctx context.Context,
	ownerID string,
	tenantID string,
	request IngestRequest,
	result IngestResult,
	started time.Time,
	finished time.Time,
) error {
	if result.Failed == 0 || result.Applied != 0 {
		return fmt.Errorf("ingest attempt failure must be terminally failed")
	}
	record := IngestBatchRecord{
		TenantID: tenantID,
		Request: IngestRequest{
			Source:         request.Source,
			CollectorID:    request.CollectorID,
			BatchID:        request.BatchID,
			IdempotencyKey: request.IdempotencyKey,
		},
		Result:     compactIngestAttemptFailure(result),
		StartedAt:  started,
		FinishedAt: finished,
	}
	data, err := marshalParquetIngestRecord(ctx, record)
	if err != nil {
		return fmt.Errorf("encode ingest attempt failure: %w", err)
	}
	key := s.ingestAttemptFailureKey(
		tenantID, ownerID, request.Source, request.CollectorID, request.BatchID,
	)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		existing, meta, loadErr := s.loadIngestRecordWithMeta(ctx, key)
		if errors.Is(loadErr, ErrNotFound) {
			meta = ObjectMeta{Key: key}
		} else if loadErr != nil {
			return loadErr
		} else if !existing.FinishedAt.Before(finished) {
			return nil
		}
		if _, err := s.putBytesWithMetaResult(ctx, key, data, meta); err == nil {
			s.markObjectKeyCached(key)
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("%w: ingest attempt failure changed while publishing", ErrConflict)
}

func (s *TenantStore) ResolveIngestAttemptFailure(
	ctx context.Context,
	ownerID string,
	tenantID string,
	request IngestRequest,
	result IngestResult,
	started time.Time,
	finished time.Time,
) error {
	if result.Failed > 0 && result.Applied == 0 {
		return fmt.Errorf("resolved ingest attempt must not be terminally failed")
	}
	key := s.ingestAttemptFailureKey(
		tenantID, ownerID, request.Source, request.CollectorID, request.BatchID,
	)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		existing, meta, loadErr := s.loadIngestRecordWithMeta(ctx, key)
		if errors.Is(loadErr, ErrNotFound) {
			return nil
		}
		if loadErr != nil {
			return loadErr
		}
		if existing.TenantID != tenantID ||
			existing.Request.Source != request.Source ||
			existing.Request.CollectorID != request.CollectorID ||
			existing.Result.BatchID != request.BatchID {
			return fmt.Errorf(
				"ingest attempt failure identity mismatch for tenant %q source %q collector %q batch %q",
				tenantID, request.Source, request.CollectorID, request.BatchID,
			)
		}
		if existing.FinishedAt.After(finished) {
			return nil
		}
		deleteErr := s.Objects.DeleteConditional(ctx, key, PutCondition{IfMatch: meta.ETag})
		if errors.Is(deleteErr, ErrConditionalDeleteUnsupported) {
			// A stable owner ID has one WAL writer and serializes attempts for an
			// identity, so stores without conditional DELETE can safely fall back.
			deleteErr = s.Objects.Delete(ctx, key)
		}
		if deleteErr == nil || errors.Is(deleteErr, ErrNotFound) {
			return nil
		}
		if !errors.Is(deleteErr, ErrConflict) {
			return deleteErr
		}
	}
	return fmt.Errorf("%w: ingest attempt failure changed while resolving", ErrConflict)
}

func compactIngestAttemptFailure(result IngestResult) IngestResult {
	compact := result
	compact.Failures = nil
	if len(result.Failures) > 0 {
		compact.Failures = []IngestFailure{{
			Index: result.Failures[0].Index,
			Error: result.Failures[0].Error,
		}}
	}
	compact.Conflicts = nil
	if len(result.Conflicts) > 0 {
		compact.Conflicts = []IngestConflict{{Message: result.Conflicts[0].Message}}
	}
	return compact
}
