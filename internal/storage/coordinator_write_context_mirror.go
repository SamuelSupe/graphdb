package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (s *TenantStore) mirrorLatestWriteContext(ctx context.Context, tenantID string) error {
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return err
		}
		if head.Status != TenantStatusActive {
			return ErrTenantDeleted
		}
		if err := s.mirrorWriteContextSnapshot(ctx, tenantID, snapshot, head); err != nil {
			if errors.Is(err, ErrConflict) {
				if err := coordinatorRetryDelay(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return err
		}
		current, exists, err := s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return err
		}
		if exists && sameCoordinationPoint(current, coordinatedHeadToken{
			Revision:        head.Revision,
			Generation:      head.Generation,
			ContextRevision: head.WriteContextRevision,
		}) && current.Status == TenantStatusActive {
			return nil
		}
	}
	return fmt.Errorf("%w: write-context mirror did not converge for tenant %q", ErrConflict, tenantID)
}

func (s *TenantStore) mirrorWriteContextSnapshot(
	ctx context.Context,
	tenantID string,
	snapshot WriteContextSnapshot,
	head CoordinationHead,
) error {
	if snapshot.SourcePolicyConfigured {
		data, err := marshalParquetSourcePolicy(ctx, sourcePolicyRecord{
			TenantID: tenantID, SourcePolicy: snapshot.SourcePolicy,
		})
		if err != nil {
			return err
		}
		if err := s.putLegacyMirrorObject(ctx, tenantID, s.sourcePolicyKey(tenantID), data, head); err != nil {
			return err
		}
	}
	if snapshot.TenantConfigConfigured {
		data, err := marshalParquetTenantConfig(ctx, tenantConfigRecord{
			TenantID: tenantID, Config: snapshot.TenantConfig,
		})
		if err != nil {
			return err
		}
		if err := s.putLegacyMirrorObject(ctx, tenantID, s.tenantConfigKey(tenantID), data, head); err != nil {
			return err
		}
	}
	data, err := json.Marshal(snapshot.RelationSchemas)
	if err != nil {
		return err
	}
	return s.putLegacyMirrorObject(ctx, tenantID, s.relationSchemaCatalogKey(tenantID), data, head)
}

func (s *TenantStore) putLegacyMirrorObject(
	ctx context.Context,
	tenantID string,
	key string,
	data []byte,
	expected CoordinationHead,
) error {
	for attempt := 0; attempt < 8; attempt++ {
		if err := s.ensureMirrorHeadCurrent(ctx, tenantID, expected); err != nil {
			return err
		}
		_, meta, err := s.Objects.GetWithMeta(ctx, key)
		condition := PutCondition{IfNoneMatch: true}
		if err == nil {
			condition = PutCondition{IfMatch: meta.ETag}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		next, err := s.Objects.PutConditional(ctx, key, data, condition)
		if err == nil {
			if fenceErr := s.ensureMirrorHeadCurrent(ctx, tenantID, expected); fenceErr == nil {
				return nil
			} else {
				rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				rollbackErr := s.Objects.DeleteConditional(
					rollbackCtx, key, PutCondition{IfMatch: next.ETag},
				)
				cancel()
				if rollbackErr != nil &&
					!errors.Is(rollbackErr, ErrConflict) &&
					!errors.Is(rollbackErr, ErrNotFound) {
					return errors.Join(fenceErr, fmt.Errorf("rollback stale mirror %q: %w", key, rollbackErr))
				}
				return fenceErr
			}
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: legacy mirror object %q changed while publishing", ErrConflict, key)
}

func (s *TenantStore) ensureMirrorHeadCurrent(
	ctx context.Context,
	tenantID string,
	expected CoordinationHead,
) error {
	current, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return err
	}
	if !exists ||
		current.Revision != expected.Revision ||
		current.Generation != expected.Generation ||
		current.WriteContextRevision != expected.WriteContextRevision ||
		current.Status != expected.Status {
		return fmt.Errorf("%w: tenant %q changed while mirroring compatibility metadata", ErrConflict, tenantID)
	}
	return nil
}
