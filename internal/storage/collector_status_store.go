package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *TenantStore) saveCollectorStatus(ctx context.Context, tenantID string, request IngestRequest, result IngestResult, started time.Time, finished time.Time) error {
	unlock, err := s.lockCollectorStatus(ctx, tenantID, request.Source, request.CollectorID)
	if err != nil {
		return err
	}
	defer unlock()
	if !s.MaterializeCollectorStatus {
		return s.cacheCollectorStatusFromBatches(ctx, tenantID, request, result, started, finished)
	}
	key := s.collectorStatusKey(tenantID, request.Source, request.CollectorID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		status, meta, ok := s.getCachedCollectorStatus(key)
		migrated := false
		if !ok {
			var err error
			status, meta, migrated, err = s.ensureMaterializedCollectorStatus(ctx, tenantID, request.Source, request.CollectorID)
			if err != nil {
				return err
			}
		}
		if migrated || collectorStatusCoversResult(status, result) {
			return nil
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

type ingestCollectorStatusUpdate struct {
	request  IngestRequest
	result   IngestResult
	started  time.Time
	finished time.Time
}

func (s *TenantStore) saveCollectorStatusBatch(
	ctx context.Context,
	tenantID string,
	updates []ingestCollectorStatusUpdate,
) error {
	if len(updates) == 0 {
		return nil
	}
	first := updates[0]
	if !s.MaterializeCollectorStatus {
		for _, update := range updates {
			if err := s.saveCollectorStatus(ctx, tenantID, update.request, update.result, update.started, update.finished); err != nil {
				return err
			}
		}
		return nil
	}
	for _, update := range updates[1:] {
		if update.request.Source != first.request.Source || update.request.CollectorID != first.request.CollectorID {
			return fmt.Errorf("collector status batch contains multiple collectors")
		}
	}
	unlock, err := s.lockCollectorStatus(ctx, tenantID, first.request.Source, first.request.CollectorID)
	if err != nil {
		return err
	}
	defer unlock()
	key := s.collectorStatusKey(tenantID, first.request.Source, first.request.CollectorID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		status, meta, ok := s.getCachedCollectorStatus(key)
		migrated := false
		if !ok {
			var err error
			status, meta, migrated, err = s.ensureMaterializedCollectorStatus(ctx, tenantID, first.request.Source, first.request.CollectorID)
			if err != nil {
				return err
			}
		}
		if migrated {
			return nil
		}
		changed := false
		for _, update := range updates {
			if collectorStatusCoversResult(status, update.result) {
				continue
			}
			applyCollectorStatusResult(&status, tenantID, update.request, update.result, update.started, update.finished)
			changed = true
		}
		if !changed {
			return nil
		}
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
	return fmt.Errorf(
		"%w: collector status for source %q collector %q changed while publishing batch",
		ErrConflict, first.request.Source, first.request.CollectorID,
	)
}

func (s *TenantStore) repairCollectorStatusAfterSkip(ctx context.Context, tenantID string, record IngestBatchRecord) error {
	unlock, err := s.lockCollectorStatus(ctx, tenantID, record.Request.Source, record.Request.CollectorID)
	if err != nil {
		return err
	}
	defer unlock()
	if !s.MaterializeCollectorStatus {
		return s.repairCachedCollectorStatusFromBatches(ctx, tenantID, record)
	}
	request := record.Request
	result := record.Result
	key := s.collectorStatusKey(tenantID, request.Source, request.CollectorID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		status, meta, migrated, err := s.ensureMaterializedCollectorStatus(ctx, tenantID, request.Source, request.CollectorID)
		if err != nil {
			return err
		}
		if migrated || collectorStatusCoversResult(status, result) || !collectorStatusCanRepairSkippedResult(status, meta, result) {
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
	status, _, _, err := s.ensureMaterializedCollectorStatus(ctx, tenantID, source, collectorID)
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

func (s *TenantStore) lockCollectorStatus(ctx context.Context, tenantID, source, collectorID string) (func(), error) {
	// Metadata finalization runs after the graph lock is released. Use the
	// existing bounded lock queue with a namespace no valid tenant can occupy.
	return s.lockTenantContext(ctx, "\x00collector:"+s.collectorStatusKey(tenantID, source, collectorID), true)
}
