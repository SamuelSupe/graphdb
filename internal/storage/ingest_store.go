package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *TenantStore) loadIngestRecord(ctx context.Context, tenantID string, request IngestRequest) (IngestBatchRecord, bool, error) {
	if request.IdempotencyKey != "" {
		keys := []string{
			s.ingestIdempotencyKey(tenantID, request.Source, request.CollectorID, request.IdempotencyKey),
			s.legacyIngestIdempotencyKey(tenantID, request.Source, request.IdempotencyKey),
		}
		if request.BatchID != request.IdempotencyKey {
			keys = append(keys,
				s.ingestBatchKey(tenantID, request.Source, request.CollectorID, request.IdempotencyKey),
				s.legacyIngestBatchKey(tenantID, request.Source, request.IdempotencyKey),
			)
		}
		for _, key := range keys {
			record, ok, err := s.loadMatchingIngestIdempotencyRecord(ctx, tenantID, key, request)
			if err != nil || ok {
				return record, ok, err
			}
		}
	}
	if request.BatchID != "" {
		for _, key := range []string{
			s.ingestBatchKey(tenantID, request.Source, request.CollectorID, request.BatchID),
			s.legacyIngestBatchKey(tenantID, request.Source, request.BatchID),
		} {
			record, ok, err := s.loadMatchingIngestRecord(ctx, tenantID, key, request)
			if err != nil || ok {
				return record, ok, err
			}
		}
	}
	return IngestBatchRecord{}, false, nil
}

func (s *TenantStore) loadMatchingIngestRecord(ctx context.Context, tenantID string, key string, request IngestRequest) (IngestBatchRecord, bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if !mayExist {
		return IngestBatchRecord{}, false, nil
	}
	record, _, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return IngestBatchRecord{}, false, nil
	}
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if !ingestRecordMatchesIdentity(record, tenantID, request) {
		return IngestBatchRecord{}, false, nil
	}
	if !ingestRecordRequestEqual(record.Request, request) {
		return IngestBatchRecord{}, false, fmt.Errorf("ingest record conflict for source %q collector %q batch %q idempotency %q: stored request differs from incoming request", request.Source, request.CollectorID, request.BatchID, request.IdempotencyKey)
	}
	return record, true, nil
}

func (s *TenantStore) loadMatchingIngestIdempotencyRecord(ctx context.Context, tenantID string, key string, request IngestRequest) (IngestBatchRecord, bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if !mayExist {
		return IngestBatchRecord{}, false, nil
	}
	record, _, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return IngestBatchRecord{}, false, nil
	}
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if !ingestRecordMatchesIdempotencyIdentity(record, tenantID, request) {
		return IngestBatchRecord{}, false, nil
	}
	if !ingestRecordRequestEqualIgnoringBatch(record.Request, request) {
		return IngestBatchRecord{}, false, fmt.Errorf("ingest record conflict for source %q collector %q batch %q idempotency %q: stored request differs from incoming request", request.Source, request.CollectorID, request.BatchID, request.IdempotencyKey)
	}
	return record, true, nil
}

func (s *TenantStore) repairIngestMetadataAfterSkip(ctx context.Context, tenantID string, record IngestBatchRecord, saveFailures bool) error {
	var metadataErr error
	if err := s.repairCollectorStatusAfterSkip(ctx, tenantID, record); err != nil {
		metadataErr = errors.Join(metadataErr, fmt.Errorf("save collector status: %w", err))
	}
	if saveFailures && record.Result.Failed > 0 {
		if err := s.ensureDeadLetterAfterSkip(ctx, tenantID, record.Request, record.Result); err != nil {
			metadataErr = errors.Join(metadataErr, fmt.Errorf("save dead letter: %w", err))
		}
	}
	return metadataErr
}

func (s *TenantStore) saveIngestBatch(ctx context.Context, tenantID string, record IngestBatchRecord) error {
	record.TenantID = tenantID
	var result error
	if record.Request.IdempotencyKey != "" && record.Request.IdempotencyKey != record.Result.BatchID {
		key := s.ingestIdempotencyKey(tenantID, record.Request.Source, record.Request.CollectorID, record.Request.IdempotencyKey)
		if err := s.saveIngestRecord(ctx, tenantID, key, record); err != nil {
			result = errors.Join(result, fmt.Errorf("save idempotency record: %w", err))
		}
	}
	key := s.ingestBatchKey(tenantID, record.Request.Source, record.Request.CollectorID, record.Result.BatchID)
	if err := s.saveIngestRecord(ctx, tenantID, key, record); err != nil {
		result = errors.Join(result, fmt.Errorf("save batch record: %w", err))
	}
	return result
}

func (s *TenantStore) saveIngestRecord(ctx context.Context, tenantID string, key string, record IngestBatchRecord) error {
	data, err := marshalParquetIngestRecord(ctx, record)
	if err != nil {
		return err
	}
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	if err == nil {
		s.markObjectKeyCached(key)
		return nil
	}
	if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, meta, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		_, retryErr := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
		if retryErr == nil {
			s.markObjectKeyCached(key)
		}
		return retryErr
	}
	if err != nil {
		return fmt.Errorf("%w: existing ingest record is unreadable; repair blocked", err)
	}
	if ingestRecordSameResult(existing, record) {
		return nil
	}
	if ingestRecordMatchesIdentity(existing, tenantID, record.Request) || ingestRecordMatchesIdempotencyIdentity(existing, tenantID, record.Request) {
		return fmt.Errorf("%w: ingest record for source %q collector %q batch %q idempotency %q changed while publishing", ErrConflict, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Request.IdempotencyKey)
	}
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	if errors.Is(err, ErrConflict) {
		return fmt.Errorf("%w: ingest record changed while repairing mismatched metadata", ErrConflict)
	}
	return err
}

func (s *TenantStore) loadIngestRecordWithMeta(ctx context.Context, key string) (IngestBatchRecord, ObjectMeta, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return IngestBatchRecord{}, meta, err
	}
	if !isParquetBytes(data) {
		return IngestBatchRecord{}, meta, fmt.Errorf("unsupported ingest record %q: only parquet ingest records are readable", key)
	}
	record, err := decodeParquetIngestRecord(ctx, data)
	if err != nil {
		return IngestBatchRecord{}, meta, err
	}
	return record, meta, nil
}

func ingestRecordSameResult(stored IngestBatchRecord, incoming IngestBatchRecord) bool {
	if !ingestRecordRequestEqual(stored.Request, incoming.Request) {
		return false
	}
	storedResult, err := json.Marshal(stored.Result)
	if err != nil {
		return false
	}
	incomingResult, err := json.Marshal(incoming.Result)
	if err != nil {
		return false
	}
	return bytes.Equal(storedResult, incomingResult)
}

func ingestRecordMatchesRequest(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return ingestRecordMatchesIdentity(record, tenantID, request) && ingestRecordRequestEqual(record.Request, request)
}

func ingestRecordMatchesIdentity(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return (record.TenantID == "" || record.TenantID == tenantID) &&
		record.Request.Source == request.Source &&
		record.Request.CollectorID == request.CollectorID &&
		record.Request.BatchID == request.BatchID &&
		record.Request.IdempotencyKey == request.IdempotencyKey &&
		record.Result.BatchID == request.BatchID
}

func ingestRecordMatchesIdempotencyIdentity(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return request.IdempotencyKey != "" &&
		(record.TenantID == "" || record.TenantID == tenantID) &&
		record.Request.Source == request.Source &&
		record.Request.CollectorID == request.CollectorID &&
		record.Request.IdempotencyKey == request.IdempotencyKey &&
		record.Result.BatchID == record.Request.BatchID
}

func ingestRecordRequestEqual(stored IngestRequest, incoming IngestRequest) bool {
	storedJSON, err := json.Marshal(stored)
	if err != nil {
		return false
	}
	incomingJSON, err := json.Marshal(incoming)
	if err != nil {
		return false
	}
	return bytes.Equal(storedJSON, incomingJSON)
}

func ingestRecordRequestEqualIgnoringBatch(stored IngestRequest, incoming IngestRequest) bool {
	stored.BatchID = ""
	incoming.BatchID = ""
	return ingestRecordRequestEqual(stored, incoming)
}

func (s *TenantStore) saveCollectorStatus(ctx context.Context, tenantID string, request IngestRequest, result IngestResult, started time.Time, finished time.Time) error {
	if !s.MaterializeCollectorStatus {
		return s.cacheCollectorStatusFromBatches(ctx, tenantID, request, result, started, finished)
	}
	key := s.collectorStatusKey(tenantID, request.Source, request.CollectorID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		status, meta, ok := s.getCachedCollectorStatus(key)
		if !ok {
			var err error
			status, meta, err = s.loadCollectorStatusWithMeta(ctx, tenantID, request.Source, request.CollectorID)
			if err != nil {
				return err
			}
		}
		applyCollectorStatusResult(&status, tenantID, request, result, started, finished)
		if nextMeta, err := s.putCollectorStatusWithMeta(ctx, key, status, meta); err == nil {
			s.setCachedCollectorStatus(key, status, nextMeta)
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		s.deleteCachedCollectorStatus(key)
		if attempt+1 >= s.retryCount() {
			break
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: collector status for source %q collector %q changed while publishing", ErrConflict, request.Source, request.CollectorID)
}

func (s *TenantStore) repairCollectorStatusAfterSkip(ctx context.Context, tenantID string, record IngestBatchRecord) error {
	if !s.MaterializeCollectorStatus {
		return s.repairCachedCollectorStatusFromBatches(ctx, tenantID, record)
	}
	request := record.Request
	result := record.Result
	key := s.collectorStatusKey(tenantID, request.Source, request.CollectorID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		status, meta, err := s.loadCollectorStatusWithMeta(ctx, tenantID, request.Source, request.CollectorID)
		if err != nil {
			return err
		}
		if collectorStatusCoversResult(status, result) || !collectorStatusCanRepairSkippedResult(status, meta, result) {
			return nil
		}
		applyCollectorStatusResult(&status, tenantID, request, result, record.StartedAt, record.FinishedAt)
		if nextMeta, err := s.putCollectorStatusWithMeta(ctx, key, status, meta); err == nil {
			s.setCachedCollectorStatus(key, status, nextMeta)
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		if attempt+1 >= s.retryCount() {
			break
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: collector status for source %q collector %q changed while repairing skipped ingest", ErrConflict, request.Source, request.CollectorID)
}

func collectorStatusCoversResult(status CollectorStatus, result IngestResult) bool {
	return status.LastBatchID == result.BatchID && status.LastVersion == result.Version
}

func collectorStatusCanRepairSkippedResult(status CollectorStatus, meta ObjectMeta, result IngestResult) bool {
	if !meta.Exists {
		return true
	}
	if result.Version <= 0 {
		return false
	}
	return status.LastVersion < result.Version
}

func (s *TenantStore) GetCollectorStatus(ctx context.Context, tenantID string, source string, collectorID string) (CollectorStatus, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return CollectorStatus{}, err
	}
	source = strings.TrimSpace(source)
	collectorID = strings.TrimSpace(collectorID)
	if source == "" || collectorID == "" {
		return CollectorStatus{}, fmt.Errorf("source and collector_id are required")
	}
	key := s.collectorStatusKey(tenantID, source, collectorID)
	if status, _, ok := s.getCachedCollectorStatus(key); ok {
		return status, nil
	}
	if !s.MaterializeCollectorStatus {
		status, err := s.deriveCollectorStatusFromBatches(ctx, tenantID, source, collectorID)
		if err == nil {
			s.setCachedCollectorStatus(key, status, ObjectMeta{Key: key})
		}
		return status, err
	}
	status, _, err := s.loadCollectorStatusWithMeta(ctx, tenantID, source, collectorID)
	return status, err
}

func (s *TenantStore) loadCollectorStatusWithMeta(ctx context.Context, tenantID string, source string, collectorID string) (CollectorStatus, ObjectMeta, error) {
	key := s.collectorStatusKey(tenantID, source, collectorID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return CollectorStatus{TenantID: tenantID, Source: source, CollectorID: collectorID}, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return CollectorStatus{}, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return CollectorStatus{}, ObjectMeta{}, fmt.Errorf("unsupported collector status %q: only parquet collector status records are readable", key)
	}
	status, err := decodeParquetCollectorStatus(ctx, data)
	if err != nil {
		return CollectorStatus{}, ObjectMeta{}, err
	}
	if status.TenantID != tenantID || status.Source != source || status.CollectorID != collectorID {
		return CollectorStatus{}, ObjectMeta{}, fmt.Errorf("collector status identity mismatch for tenant %q source %q collector %q", tenantID, source, collectorID)
	}
	return status, meta, nil
}

func (s *TenantStore) putCollectorStatusWithMeta(ctx context.Context, key string, status CollectorStatus, meta ObjectMeta) (ObjectMeta, error) {
	data, err := marshalParquetCollectorStatus(ctx, status)
	if err != nil {
		return ObjectMeta{}, err
	}
	return s.putBytesWithMetaResult(ctx, key, data, meta)
}

func applyCollectorStatusResult(status *CollectorStatus, tenantID string, request IngestRequest, result IngestResult, started time.Time, finished time.Time) {
	status.TenantID = tenantID
	status.Source = request.Source
	status.CollectorID = request.CollectorID
	status.LastBatchID = result.BatchID
	status.LastCursor = request.Cursor
	status.LastVersion = result.Version
	status.LastStartedAt = started
	status.LastFinishedAt = finished
	status.AppliedTotal += result.Applied
	status.FailedTotal += result.Failed
	status.LastError = firstFailure(result)
}

func firstFailure(result IngestResult) string {
	if result.Failed > 0 && len(result.Failures) > 0 {
		return result.Failures[0].Error
	}
	return ""
}
