package storage

import (
	"context"
	"errors"
	"time"
)

func (s *TenantStore) cacheCollectorStatusFromBatches(ctx context.Context, tenantID string, request IngestRequest, result IngestResult, started time.Time, finished time.Time) error {
	key := s.collectorStatusKey(tenantID, request.Source, request.CollectorID)
	if status, meta, ok := s.getCachedCollectorStatus(key); ok {
		applyCollectorStatusResult(&status, tenantID, request, result, started, finished)
		s.setCachedCollectorStatus(key, status, meta)
		return nil
	}
	status, err := s.deriveCollectorStatusFromBatches(ctx, tenantID, request.Source, request.CollectorID)
	if err != nil {
		return err
	}
	s.setCachedCollectorStatus(key, status, ObjectMeta{Key: key})
	return nil
}

func (s *TenantStore) repairCachedCollectorStatusFromBatches(ctx context.Context, tenantID string, record IngestBatchRecord) error {
	key := s.collectorStatusKey(tenantID, record.Request.Source, record.Request.CollectorID)
	status, err := s.deriveCollectorStatusFromBatches(ctx, tenantID, record.Request.Source, record.Request.CollectorID)
	if err != nil {
		return err
	}
	s.setCachedCollectorStatus(key, status, ObjectMeta{Key: key})
	return nil
}

func (s *TenantStore) deriveCollectorStatusFromBatches(ctx context.Context, tenantID string, source string, collectorID string) (CollectorStatus, error) {
	status := CollectorStatus{TenantID: tenantID, Source: source, CollectorID: collectorID}
	objects, err := s.Objects.List(ctx, s.ingestBatchPrefix(tenantID, source, collectorID))
	if err != nil {
		return status, err
	}
	var latest IngestBatchRecord
	hasLatest := false
	appliedTotal := 0
	failedTotal := 0
	for _, object := range objects {
		record, _, err := s.loadIngestRecordWithMeta(ctx, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return status, err
		}
		if !collectorStatusRecordMatches(record, tenantID, source, collectorID) {
			continue
		}
		appliedTotal += record.Result.Applied
		failedTotal += record.Result.Failed
		if !hasLatest || collectorStatusRecordAfter(record, latest) {
			latest = record
			hasLatest = true
		}
	}
	if hasLatest {
		applyCollectorStatusResult(&status, tenantID, latest.Request, latest.Result, latest.StartedAt, latest.FinishedAt)
		status.AppliedTotal = appliedTotal
		status.FailedTotal = failedTotal
	}
	return status, nil
}

func collectorStatusRecordAfter(left IngestBatchRecord, right IngestBatchRecord) bool {
	leftTime, rightTime := collectorStatusRecordTime(left), collectorStatusRecordTime(right)
	if !leftTime.Equal(rightTime) {
		return leftTime.After(rightTime)
	}
	if left.Result.Version != right.Result.Version {
		return left.Result.Version > right.Result.Version
	}
	return left.Result.BatchID > right.Result.BatchID
}

func collectorStatusRecordMatches(record IngestBatchRecord, tenantID string, source string, collectorID string) bool {
	return (record.TenantID == "" || record.TenantID == tenantID) &&
		record.Request.Source == source &&
		record.Request.CollectorID == collectorID &&
		record.Result.BatchID == record.Request.BatchID
}

func collectorStatusRecordTime(record IngestBatchRecord) time.Time {
	if !record.FinishedAt.IsZero() {
		return record.FinishedAt
	}
	return record.StartedAt
}
