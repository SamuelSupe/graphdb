package storage

import (
	"context"
	"errors"
	"fmt"
)

func (s *TenantStore) startCoordinatedHeadPublication(
	ctx context.Context,
	tenantID string,
) (context.Context, func(), error) {
	attempts := max(s.CoordinatorRetryLimit+1, 1)
	for attempt := 0; attempt < attempts; attempt++ {
		if _, exists, err := s.Coordinator.Head(ctx, tenantID); err != nil {
			return nil, nil, err
		} else if exists {
			return ctx, func() {}, nil
		}
		operationCtx, stop, err := s.startCoordinatorOperationLease(
			ctx, tenantID, coordinatorLifecycleTaskType,
		)
		if err != nil {
			if errors.Is(err, ErrTaskLeaseHeld) && attempt+1 < attempts {
				if err := coordinatorRetryDelay(ctx, attempt); err != nil {
					return nil, nil, err
				}
				continue
			}
			return nil, nil, err
		}
		if _, exists, err := s.Coordinator.Head(
			operationCtx, tenantID,
		); err != nil {
			stop()
			return nil, nil, err
		} else if exists {
			return operationCtx, stop, nil
		}
		_, candidateExists, _, err :=
			s.getCoordinatedTenantCandidate(operationCtx, tenantID)
		if err != nil {
			stop()
			return nil, nil, err
		}
		if candidateExists {
			stop()
			return nil, nil, fmt.Errorf(
				"%w: tenant %q has an unfinished lifecycle candidate",
				ErrConflict, tenantID,
			)
		}
		return operationCtx, stop, nil
	}
	return nil, nil, fmt.Errorf(
		"%w: tenant %q lifecycle lease remained busy",
		ErrTaskLeaseHeld, tenantID,
	)
}
