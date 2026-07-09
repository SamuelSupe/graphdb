package storage

import (
	"context"
	"errors"
	"sort"
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
	records := make([]IngestBatchRecord, 0, len(objects))
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
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := collectorStatusRecordTime(records[i]), collectorStatusRecordTime(records[j])
		if !left.Equal(right) {
			return left.Before(right)
		}
		if records[i].Result.Version != records[j].Result.Version {
			return records[i].Result.Version < records[j].Result.Version
		}
		return records[i].Result.BatchID < records[j].Result.BatchID
	})
	for _, record := range records {
		applyCollectorStatusResult(&status, tenantID, record.Request, record.Result, record.StartedAt, record.FinishedAt)
	}
	return status, nil
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
