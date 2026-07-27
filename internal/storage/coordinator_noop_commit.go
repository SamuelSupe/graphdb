package storage

import (
	"context"
	"fmt"
)

func (s *TenantStore) prepareCoordinatedNoop(
	ctx context.Context,
	tenantID string,
	meta ObjectMeta,
) (coordinatedHeadToken, error) {
	if !s.coordinated() {
		return coordinatedHeadToken{}, nil
	}
	token, err := parseCoordinatedHeadToken(meta)
	if err != nil {
		return coordinatedHeadToken{}, err
	}
	if token.Revision > 0 {
		err = s.ensureCoordinationPointCurrent(ctx, tenantID, meta)
	}
	return token, err
}

func (s *TenantStore) completeCoordinatedNoop(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	meta ObjectMeta,
	token coordinatedHeadToken,
	reservation *directCommitReservation,
	result CommitResult,
) (ObjectMeta, error) {
	if token.Revision == 0 {
		return s.putCoordinatedManifest(
			ctx, tenantID, manifest, meta, reservation, nil,
		)
	}
	if reservation == nil {
		return meta, nil
	}
	request := HeadPublishRequest{
		TenantID:                     tenantID,
		ExpectedRevision:             token.Revision,
		ExpectedGeneration:           token.Generation,
		ExpectedWriteContextRevision: token.ContextRevision,
		CommitID:                     manifest.HeadCommitID,
	}
	if err := attachCoordinatorCommitMetadata(
		&request, reservation, result, manifest.Version,
	); err != nil {
		return ObjectMeta{}, err
	}
	committed, err := s.Coordinator.CompleteNoop(ctx, request)
	if err != nil {
		s.observeCoordinatorCAS(tenantID, "error", 0)
		return ObjectMeta{}, err
	}
	if !committed {
		s.observeCoordinatorCAS(tenantID, "conflict", 0)
		return ObjectMeta{}, fmt.Errorf(
			"%w: tenant %q changed while completing no-op commit",
			ErrConflict, tenantID,
		)
	}
	s.observeCoordinatorCAS(tenantID, "committed", token.Revision)
	return meta, nil
}
