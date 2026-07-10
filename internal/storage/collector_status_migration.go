package storage

import (
	"context"
	"errors"
	"fmt"
)

// ensureMaterializedCollectorStatus performs the one-time upgrade for
// collectors written while materialization was disabled. Once persisted, all
// subsequent updates use the normal incremental CAS path.
func (s *TenantStore) ensureMaterializedCollectorStatus(ctx context.Context, tenantID string, source string, collectorID string) (CollectorStatus, ObjectMeta, bool, error) {
	key := s.collectorStatusKey(tenantID, source, collectorID)
	migrationAttempted := false
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		status, meta, err := s.loadCollectorStatusWithMeta(ctx, tenantID, source, collectorID)
		if err != nil {
			return CollectorStatus{}, ObjectMeta{}, false, err
		}
		if meta.Exists {
			s.setCachedCollectorStatus(key, status, meta)
			return status, meta, migrationAttempted, nil
		}
		migrationAttempted = true
		status, err = s.deriveCollectorStatusFromBatches(ctx, tenantID, source, collectorID)
		if err != nil {
			return CollectorStatus{}, ObjectMeta{}, false, err
		}
		nextMeta, err := s.putCollectorStatusWithMeta(ctx, key, status, meta)
		if err == nil {
			s.setCachedCollectorStatus(key, status, nextMeta)
			return status, nextMeta, true, nil
		}
		if !errors.Is(err, ErrConflict) {
			return CollectorStatus{}, ObjectMeta{}, false, err
		}
		// A competing writer may have published an independently accumulated
		// status rather than the same derived history. Reload it and let the
		// incremental CAS path merge the current batch.
		migrationAttempted = false
		if attempt+1 < s.retryCount() {
			if err := retryDelay(ctx, attempt); err != nil {
				return CollectorStatus{}, ObjectMeta{}, false, err
			}
		}
	}
	return CollectorStatus{}, ObjectMeta{}, false, fmt.Errorf("%w: collector status migration for source %q collector %q changed while publishing", ErrConflict, source, collectorID)
}
