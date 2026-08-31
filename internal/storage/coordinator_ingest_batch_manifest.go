package storage

import (
	"context"
	"fmt"
)

func (s *TenantStore) putCoordinatedIngestBatchManifest(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	meta ObjectMeta,
	candidates []*ingestBatchCandidate,
) (ObjectMeta, error) {
	expected, err := parseCoordinatedHeadToken(meta)
	if err != nil {
		return ObjectMeta{}, err
	}
	if expected.Revision == 0 {
		publicationCtx, stop, publicationErr := s.startCoordinatedHeadPublication(ctx, tenantID)
		if publicationErr != nil {
			return ObjectMeta{}, publicationErr
		}
		defer stop()
		ctx = publicationCtx
		_, exists, headErr := s.Coordinator.Head(ctx, tenantID)
		if headErr != nil {
			return ObjectMeta{}, headErr
		}
		if exists {
			return ObjectMeta{}, fmt.Errorf("%w: manifest for tenant %q changed while publishing", ErrConflict, tenantID)
		}
	}

	manifest.TenantID = tenantID
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		return ObjectMeta{}, err
	}
	hash := objectContentHash(data)
	nextRevision := expected.Revision + 1
	key := s.coordinatorManifestKey(tenantID, manifest.Version, nextRevision, hash)
	if err := s.putImmutableCoordinatorObject(ctx, key, data); err != nil {
		return ObjectMeta{}, err
	}
	items, err := s.coordinatedIngestBatchCompletions(candidates)
	if err != nil {
		return ObjectMeta{}, err
	}
	request := IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     tenantID,
			ExpectedRevision:             expected.Revision,
			ExpectedGeneration:           expected.Generation,
			ExpectedWriteContextRevision: expected.ContextRevision,
			GraphVersion:                 manifest.Version,
			ManifestKey:                  key,
			ManifestHash:                 hash,
			CommitID:                     manifest.HeadCommitID,
		},
		Items: items,
	}
	next, published, err := s.Coordinator.PublishIngestBatch(ctx, request)
	if err != nil {
		s.observeCoordinatorCAS(tenantID, "error", 0)
		return ObjectMeta{}, err
	}
	if !published {
		s.observeCoordinatorCAS(tenantID, "conflict", 0)
		s.recordManifestCASConflict(tenantID)
		switch next.Status {
		case TenantStatusDisabled:
			return ObjectMeta{}, ErrTenantDisabled
		case TenantStatusDeleted:
			return ObjectMeta{}, ErrTenantDeleted
		}
		return ObjectMeta{}, fmt.Errorf("%w: manifest for tenant %q changed while publishing", ErrConflict, tenantID)
	}
	s.observeCoordinatorCAS(tenantID, "committed", next.Revision)
	return coordinatedManifestMeta(next.ManifestKey, next), nil
}
