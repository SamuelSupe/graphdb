package storage

import (
	"context"
	"errors"
)

func (s *TenantStore) claimCoordinatorLease(
	ctx context.Context,
	tenantID string,
	taskType string,
	owner string,
	cancel context.CancelFunc,
	findActive func(context.Context) bool,
) (func(), bool, error) {
	attempts := max(s.CoordinatorRetryLimit+1, 1)
	for attempt := 0; attempt < attempts; attempt++ {
		stop, err := s.startCoordinatorLease(
			ctx,
			tenantID,
			taskType,
			owner,
			cancel,
		)
		if err == nil {
			return stop, false, nil
		}
		if !errors.Is(err, ErrTaskLeaseHeld) {
			return nil, false, err
		}
		if findActive != nil && findActive(ctx) {
			return nil, true, nil
		}
		if attempt+1 >= attempts {
			return nil, false, err
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return nil, false, err
		}
	}
	return nil, false, ErrTaskLeaseHeld
}
