package storage

import (
	"context"
	"fmt"
)

func (s *TenantStore) prepareCoordinatedCloneTarget(
	ctx context.Context,
	targetTenantID string,
	candidate coordinatedTenantCandidate,
) (bool, error) {
	head, exists, err := s.Coordinator.Head(ctx, targetTenantID)
	if err != nil {
		return false, err
	}
	if exists && head.Status != TenantStatusDeleted {
		if head.Status != TenantStatusActive {
			return false, ErrTenantDisabled
		}
		current, candidateExists, _, err := s.getCoordinatedTenantCandidate(
			ctx, targetTenantID,
		)
		if err != nil {
			return false, err
		}
		if !candidateExists || current != candidate {
			return false, fmt.Errorf(
				"%w: target tenant %q already exists",
				ErrConflict, targetTenantID,
			)
		}
		if _, _, err := s.getCoordinatedManifest(ctx, targetTenantID); err != nil {
			return false, err
		}
		return true, nil
	}
	_, err = s.prepareCoordinatedTenantCandidate(
		ctx, targetTenantID, candidate,
	)
	return false, err
}
