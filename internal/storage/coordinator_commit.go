package storage

import "context"

func (s *TenantStore) putManifestForCommit(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	meta ObjectMeta,
	reservation *directCommitReservation,
) (ObjectMeta, error) {
	if s.coordinated() {
		return s.putCoordinatedManifest(
			ctx, tenantID, manifest, meta, reservation, nil,
		)
	}
	return s.putManifestMeta(ctx, tenantID, manifest, meta)
}

func attachCoordinatorCommitMetadata(
	request *HeadPublishRequest,
	reservation *directCommitReservation,
	result CommitResult,
	version int64,
) error {
	if reservation == nil || !reservation.coordinated {
		return nil
	}
	resultJSON, err := marshalCommitResult(result)
	if err != nil {
		return err
	}
	request.IdempotencyKey = reservation.record.Request.IdempotencyKey
	request.RequestHash = reservation.requestHash
	request.OwnerToken = reservation.ownerToken
	request.Result = resultJSON
	if reservation.record.Request.CollectorState != nil {
		update := *reservation.record.Request.CollectorState
		update.Version = version
		request.CollectorState = &update
	}
	return nil
}
