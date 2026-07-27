package storage

import (
	"context"
	"errors"
	"fmt"
)

const coordinatorPurgeTaskType = coordinatorLifecycleTaskType

func (s *TenantStore) startCoordinatedPurge(
	ctx context.Context,
	tenantID string,
	force bool,
) (context.Context, int64, bool, func(), error) {
	operationCtx, stop, err := s.startCoordinatorOperationLease(
		ctx, tenantID, coordinatorPurgeTaskType,
	)
	if err != nil {
		return nil, 0, false, nil, err
	}
	fail := func(err error) (context.Context, int64, bool, func(), error) {
		stop()
		return nil, 0, false, nil, err
	}
	head, exists, err := s.Coordinator.Head(operationCtx, tenantID)
	if err != nil {
		return fail(err)
	}
	if !exists {
		if !force {
			return fail(ErrCoordinatorHeadMissing)
		}
		_, candidateExists, _, candidateErr :=
			s.getCoordinatedTenantCandidate(operationCtx, tenantID)
		if candidateErr != nil {
			return fail(candidateErr)
		}
		if !candidateExists {
			return fail(ErrCoordinatorHeadMissing)
		}
		return operationCtx, 0, true, stop, nil
	}
	if !force && head.Status != TenantStatusDeleted {
		return fail(fmt.Errorf("tenant must be soft deleted before purge"))
	}
	head, err = s.Coordinator.TransitionTenant(
		operationCtx, tenantID, TenantStatusDeleted, true,
	)
	if err != nil {
		return fail(err)
	}
	return operationCtx, head.Generation, false, stop, nil
}

func (s *TenantStore) ensureCoordinatedPurgeCurrent(
	ctx context.Context,
	tenantID string,
	generation int64,
) error {
	if generation <= 0 {
		return objectContextErr(ctx)
	}
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return err
	}
	if !exists ||
		head.Generation != generation ||
		head.Status != TenantStatusDeleted {
		return fmt.Errorf(
			"%w: tenant %q purge generation changed",
			ErrConflict, tenantID,
		)
	}
	return nil
}

func (s *TenantStore) deleteTenantPurgeObject(
	ctx context.Context,
	tenantID string,
	object ObjectInfo,
	generation int64,
) error {
	if generation <= 0 && !s.coordinated() {
		return s.Objects.Delete(ctx, object.Key)
	}
	etag := object.ETag
	if etag == "" {
		_, meta, err := s.Objects.GetWithMeta(ctx, object.Key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		etag = meta.ETag
	}
	if etag == "" {
		return fmt.Errorf(
			"%w: purge object %q has no ETag",
			ErrObjectStoreUnavailable, object.Key,
		)
	}
	err := s.Objects.DeleteConditional(
		ctx, object.Key, PutCondition{IfMatch: etag},
	)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if errors.Is(err, ErrConflict) {
		return fmt.Errorf(
			"%w: purge object %q changed",
			ErrConflict, object.Key,
		)
	}
	return err
}
