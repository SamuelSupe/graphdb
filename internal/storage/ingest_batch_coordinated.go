package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) reserveCoordinatedIngestCandidate(
	ctx context.Context,
	tenantID string,
	candidate *ingestBatchCandidate,
) (*directCommitReservation, *CommitResult, error) {
	collector := &CollectorStateUpdate{
		Source:      candidate.request.Source,
		CollectorID: candidate.request.CollectorID,
		BatchID:     candidate.request.BatchID,
		Cursor:      candidate.request.Cursor,
	}
	request := DirectCommitRequest{
		IdempotencyKey: coordinatedIngestIdempotencyKey(candidate.request),
		CollectorState: collector,
		Mutations:      candidate.mutations,
	}
	data, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	requestHash := objectContentHash(data)
	ownerToken := coordinatedIngestOwnerToken(s.InstanceID, tenantID, request.IdempotencyKey)
	reserved, err := s.Coordinator.ReserveCommit(
		ctx,
		tenantID,
		request.IdempotencyKey,
		requestHash,
		ownerToken,
		s.coordinatorPendingReservationTTL(),
	)
	if err != nil {
		return nil, nil, err
	}
	if reserved.Committed {
		var result CommitResult
		if err := json.Unmarshal(reserved.Result, &result); err != nil {
			return nil, nil, fmt.Errorf("decode coordinated ingest result: %w", err)
		}
		result.IdempotentReplay = true
		return nil, &result, nil
	}
	return &directCommitReservation{
		key: request.IdempotencyKey,
		record: DirectCommitRecord{
			TenantID:  tenantID,
			Status:    directCommitStatusPending,
			Request:   request,
			StartedAt: candidate.started,
		},
		coordinated: true,
		requestHash: requestHash,
		ownerToken:  ownerToken,
	}, nil, nil
}

func coordinatedIngestOwnerToken(instanceID string, tenantID string, key string) string {
	sum := sha256.Sum256([]byte(instanceID + "\x00" + tenantID + "\x00" + key))
	return "ingest-wal/" + instanceID + "/" + hex.EncodeToString(sum[:16])
}

func coordinatedBatchHasReservations(candidates []*ingestBatchCandidate) bool {
	for _, candidate := range candidates {
		if candidate.reservation != nil && candidate.result.Failed == 0 {
			return true
		}
	}
	return false
}

func prepareCoordinatedMetadataOnlyResults(manifest Manifest, candidates []*ingestBatchCandidate) {
	for _, candidate := range candidates {
		if candidate.reservation == nil || !candidate.metadataOnly || candidate.resultManifest.TenantID != "" {
			continue
		}
		candidate.result.Version = manifest.Version
		candidate.resultManifest = manifest
		if candidate.result.Failed == 0 {
			candidate.result.Skipped = true
			candidate.result.SkipReason = IngestSkipReasonLogicalNoop
		}
	}
}

func (s *TenantStore) coordinatedPreparedIngestStale(
	ctx context.Context,
	tenantID string,
	prepared IngestPreparedRequest,
) (bool, error) {
	manifest, meta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if prepared.BaseGeneration > 0 {
		token, tokenErr := parseCoordinatedHeadToken(meta)
		if tokenErr != nil {
			return false, tokenErr
		}
		if token.Generation != prepared.BaseGeneration {
			return false, ErrTenantDeleted
		}
		if token.Revision != prepared.BaseHeadRevision ||
			token.ContextRevision != prepared.BaseWriteContextRevision {
			return true, nil
		}
	}
	return manifest.Version != prepared.BaseVersion ||
		manifest.HeadCommitID != prepared.BaseHeadCommitID, nil
}

func (s *TenantStore) coordinatedIngestBatchCompletions(
	candidates []*ingestBatchCandidate,
) ([]IngestBatchCompletion, error) {
	items := make([]IngestBatchCompletion, 0, len(candidates))
	for _, candidate := range candidates {
		reservation := candidate.reservation
		if reservation == nil || candidate.result.Failed > 0 {
			continue
		}
		result := coordinatedCandidateCommitResult(candidate)
		resultJSON, err := marshalCommitResult(result)
		if err != nil {
			return nil, err
		}
		collector := reservation.record.Request.CollectorState
		if collector != nil {
			copy := *collector
			copy.Version = result.Version
			collector = &copy
		}
		items = append(items, IngestBatchCompletion{
			IdempotencyKey: reservation.key,
			RequestHash:    reservation.requestHash,
			OwnerToken:     reservation.ownerToken,
			CommitID:       candidate.resultManifest.HeadCommitID,
			Result:         resultJSON,
			CollectorState: collector,
		})
	}
	return items, nil
}

func coordinatedCandidateCommitResult(candidate *ingestBatchCandidate) CommitResult {
	return CommitResult{
		Manifest:          candidate.resultManifest,
		ReadableVersion:   candidate.result.Version,
		ReadAfterCommitID: candidate.resultManifest.HeadCommitID,
		Skipped:           candidate.result.Skipped,
		Suppressed:        append([]graph.FieldConflict(nil), candidate.report.Suppressed...),
		CanonicalEntities: append([]graph.EntityCanonicalization(nil), candidate.report.CanonicalEntities...),
		CanonicalEdges:    append([]graph.EdgeCanonicalization(nil), candidate.report.CanonicalEdges...),
	}
}

func (s *TenantStore) completeCoordinatedIngestBatch(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
	candidates []*ingestBatchCandidate,
) error {
	token, err := parseCoordinatedHeadToken(loaded.Meta)
	if err != nil {
		return err
	}
	items, err := s.coordinatedIngestBatchCompletions(candidates)
	if err != nil || len(items) == 0 {
		return err
	}
	if token.Revision == 0 {
		manifest := loaded.Manifest
		manifest.TenantID = tenantID
		manifest.LayoutVersion = CurrentObjectLayoutVersion
		if manifest.UpdatedAt.IsZero() {
			manifest.UpdatedAt = time.Now().UTC()
		}
		if manifest.DataMD5 == "" {
			_, dataMD5, _, emptyErr := newEmptyTenantGraph()
			if emptyErr != nil {
				return emptyErr
			}
			manifest.DataMD5 = dataMD5
		}
		for _, candidate := range candidates {
			if candidate.reservation != nil && candidate.result.Failed == 0 {
				candidate.resultManifest = manifest
			}
		}
		meta, publishErr := s.putCoordinatedIngestBatchManifest(ctx, tenantID, manifest, loaded.Meta, candidates)
		if publishErr != nil {
			return publishErr
		}
		loaded.Manifest = manifest
		loaded.Meta = meta
		s.setWriteCache(tenantID, loaded)
		return nil
	}
	committed, err := s.Coordinator.CompleteIngestBatch(ctx, IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     tenantID,
			ExpectedRevision:             token.Revision,
			ExpectedGeneration:           token.Generation,
			ExpectedWriteContextRevision: token.ContextRevision,
			CommitID:                     loaded.Manifest.HeadCommitID,
		},
		Items: items,
	})
	if err != nil {
		s.observeCoordinatorCAS(tenantID, "error", 0)
		return err
	}
	if !committed {
		s.observeCoordinatorCAS(tenantID, "conflict", 0)
		return fmt.Errorf("%w: tenant %q changed while completing ingest batch", ErrConflict, tenantID)
	}
	s.observeCoordinatorCAS(tenantID, "committed", token.Revision)
	return nil
}

func (s *TenantStore) releaseFailedIngestReservations(candidates []*ingestBatchCandidate) error {
	var result error
	for _, candidate := range candidates {
		if candidate.reservation == nil || candidate.result.Failed == 0 {
			continue
		}
		if err := s.abortDirectCommit(candidate.reservation, errors.New("ingest request failed permanently")); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}
