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
	request, reservationRequest, err := s.coordinatedIngestReservationRequest(tenantID, candidate)
	if err != nil {
		return nil, nil, err
	}
	reserved, err := s.Coordinator.ReserveCommit(
		ctx,
		tenantID,
		reservationRequest.Key,
		reservationRequest.RequestHash,
		reservationRequest.OwnerToken,
		s.coordinatorPendingReservationTTL(),
	)
	if err != nil {
		return nil, nil, err
	}
	return coordinatedIngestReservationResult(tenantID, candidate, request, reservationRequest, reserved)
}

func (s *TenantStore) reserveCoordinatedIngestCandidates(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
) error {
	if len(candidates) == 0 {
		return nil
	}
	batchStore, batchCapable := s.Coordinator.(CoordinatorCommitBatchStore)
	if !batchCapable || len(candidates) == 1 {
		for _, candidate := range candidates {
			reservation, replay, err := s.reserveCoordinatedIngestCandidate(ctx, tenantID, candidate)
			if err := applyCoordinatedIngestReservation(candidate, reservation, replay, err); err != nil {
				return err
			}
		}
		return s.reserveCoordinatedIngestBatchAliases(ctx, tenantID, candidates)
	}
	requests := make([]DirectCommitRequest, len(candidates))
	reservationRequests := make([]CommitReservationRequest, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for index, candidate := range candidates {
		request, reservationRequest, err := s.coordinatedIngestReservationRequest(tenantID, candidate)
		if err != nil {
			return err
		}
		if _, duplicate := seen[reservationRequest.Key]; duplicate {
			for _, fallback := range candidates {
				reservation, replay, reserveErr := s.reserveCoordinatedIngestCandidate(ctx, tenantID, fallback)
				if applyErr := applyCoordinatedIngestReservation(fallback, reservation, replay, reserveErr); applyErr != nil {
					return applyErr
				}
			}
			return s.reserveCoordinatedIngestBatchAliases(ctx, tenantID, candidates)
		}
		seen[reservationRequest.Key] = struct{}{}
		requests[index] = request
		reservationRequests[index] = reservationRequest
	}
	outcomes, err := batchStore.ReserveCommitBatch(
		ctx, tenantID, reservationRequests, s.coordinatorPendingReservationTTL(),
	)
	if err != nil {
		return err
	}
	if len(outcomes) != len(candidates) {
		return fmt.Errorf("coordinator batch reservation count %d does not match candidates %d", len(outcomes), len(candidates))
	}
	for index, outcome := range outcomes {
		reservation, replay, resultErr := coordinatedIngestReservationResult(
			tenantID, candidates[index], requests[index], reservationRequests[index], outcome.Reservation,
		)
		if outcome.Err != nil {
			resultErr = outcome.Err
		}
		if err := applyCoordinatedIngestReservation(candidates[index], reservation, replay, resultErr); err != nil {
			return err
		}
	}
	return s.reserveCoordinatedIngestBatchAliases(ctx, tenantID, candidates)
}

func (s *TenantStore) reserveCoordinatedIngestBatchAliases(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
) error {
	type aliasCandidate struct {
		candidate          *ingestBatchCandidate
		request            DirectCommitRequest
		reservationRequest CommitReservationRequest
	}
	aliases := make([]aliasCandidate, 0, len(candidates))
	seen := make(map[string]*ingestBatchCandidate, len(candidates))
	for _, candidate := range candidates {
		if candidate.reservation == nil || candidate.result.Failed > 0 {
			continue
		}
		key := coordinatedIngestBatchAliasKey(candidate.request)
		if key == "" {
			continue
		}
		if previous, duplicate := seen[key]; duplicate {
			if previous.reservation != nil && previous.reservation.key == candidate.reservation.key {
				return fmt.Errorf(
					"%w: duplicate ingest identity %q in one flush",
					ErrIdempotencyInProgress,
					candidate.reservation.key,
				)
			}
			if err := s.abortDirectCommit(candidate.reservation, ErrIngestIdentityConflict); err != nil {
				return err
			}
			candidate.reservation = nil
			markIngestCandidateFailure(candidate, fmt.Errorf(
				"%w for source %q collector %q batch %q",
				ErrIngestIdentityConflict,
				candidate.request.Source,
				candidate.request.CollectorID,
				candidate.request.BatchID,
			))
			candidate.metadataOnly = true
			candidate.skipMetadata = true
			continue
		}
		seen[key] = candidate
		request, reservationRequest, err := s.coordinatedIngestReservationRequestForKey(
			tenantID, candidate, key,
		)
		if err != nil {
			return err
		}
		identity, err := marshalCoordinatedIngestBatchAliasIdentity(request, candidate.request)
		if err != nil {
			return err
		}
		reservationRequest.RequestHash = objectContentHash(identity)
		aliases = append(aliases, aliasCandidate{
			candidate: candidate, request: request, reservationRequest: reservationRequest,
		})
	}
	if len(aliases) == 0 {
		return nil
	}
	apply := func(alias aliasCandidate, reserved CommitReservation, reserveErr error) error {
		reservation, replay, err := coordinatedIngestReservationResult(
			tenantID, alias.candidate, alias.request, alias.reservationRequest, reserved,
		)
		if reserveErr != nil {
			err = reserveErr
		}
		return s.applyCoordinatedIngestBatchAliasReservation(alias.candidate, reservation, replay, err)
	}
	batchStore, batchCapable := s.Coordinator.(CoordinatorCommitBatchStore)
	if !batchCapable || len(aliases) == 1 {
		for _, alias := range aliases {
			reserved, err := s.Coordinator.ReserveCommit(
				ctx,
				tenantID,
				alias.reservationRequest.Key,
				alias.reservationRequest.RequestHash,
				alias.reservationRequest.OwnerToken,
				s.coordinatorPendingReservationTTL(),
			)
			if err := apply(alias, reserved, err); err != nil {
				return err
			}
		}
		return nil
	}
	requests := make([]CommitReservationRequest, len(aliases))
	for index, alias := range aliases {
		requests[index] = alias.reservationRequest
	}
	outcomes, err := batchStore.ReserveCommitBatch(
		ctx, tenantID, requests, s.coordinatorPendingReservationTTL(),
	)
	if err != nil {
		return err
	}
	if len(outcomes) != len(aliases) {
		return fmt.Errorf("coordinator batch alias reservation count %d does not match candidates %d", len(outcomes), len(aliases))
	}
	for index, outcome := range outcomes {
		if err := apply(aliases[index], outcome.Reservation, outcome.Err); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) coordinatedIngestReservationRequest(
	tenantID string,
	candidate *ingestBatchCandidate,
) (DirectCommitRequest, CommitReservationRequest, error) {
	return s.coordinatedIngestReservationRequestForKey(
		tenantID, candidate, coordinatedIngestIdempotencyKey(candidate.request),
	)
}

func (s *TenantStore) coordinatedIngestReservationRequestForKey(
	tenantID string,
	candidate *ingestBatchCandidate,
	key string,
) (DirectCommitRequest, CommitReservationRequest, error) {
	collector := &CollectorStateUpdate{
		Source:      candidate.request.Source,
		CollectorID: candidate.request.CollectorID,
		BatchID:     candidate.request.BatchID,
		Cursor:      candidate.request.Cursor,
	}
	request := DirectCommitRequest{
		ExpectedVersion: candidate.request.ExpectedVersion,
		IdempotencyKey:  key,
		CollectorState:  collector,
		Mutations:       candidate.mutations,
	}
	data, err := marshalCoordinatedIngestReservationIdentity(request, candidate.request)
	if err != nil {
		return DirectCommitRequest{}, CommitReservationRequest{}, err
	}
	requestHash := objectContentHash(data)
	ownerToken := coordinatedIngestOwnerToken(s.InstanceID, tenantID, request.IdempotencyKey)
	return request, CommitReservationRequest{
		Key: request.IdempotencyKey, RequestHash: requestHash, OwnerToken: ownerToken,
	}, nil
}

func coordinatedIngestBatchAliasKey(request IngestRequest) string {
	if request.IdempotencyKey == "" || request.IdempotencyKey == request.BatchID {
		return ""
	}
	request.IdempotencyKey = ""
	return coordinatedIngestIdempotencyKey(request)
}

func coordinatedIngestReservationResult(
	tenantID string,
	candidate *ingestBatchCandidate,
	request DirectCommitRequest,
	reservationRequest CommitReservationRequest,
	reserved CommitReservation,
) (*directCommitReservation, *CommitResult, error) {
	if reserved.Committed {
		var result CommitResult
		if err := json.Unmarshal(reserved.Result, &result); err != nil {
			return nil, nil, fmt.Errorf("decode coordinated ingest result: %w", err)
		}
		result.IdempotentReplay = true
		return nil, &result, nil
	}
	return &directCommitReservation{
		key: reservationRequest.Key,
		record: DirectCommitRecord{
			TenantID:  tenantID,
			Status:    directCommitStatusPending,
			Request:   request,
			StartedAt: candidate.started,
		},
		coordinated: true,
		requestHash: reservationRequest.RequestHash,
		ownerToken:  reservationRequest.OwnerToken,
	}, nil, nil
}

func applyCoordinatedIngestReservation(
	candidate *ingestBatchCandidate,
	reservation *directCommitReservation,
	replay *CommitResult,
	err error,
) error {
	if err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			markIngestCandidateFailure(candidate, err)
			candidate.metadataOnly = true
			candidate.skipMetadata = true
			return nil
		}
		return err
	}
	candidate.reservation = reservation
	if replay == nil {
		return nil
	}
	applyCoordinatedIngestReplay(candidate, replay)
	return nil
}

func (s *TenantStore) applyCoordinatedIngestBatchAliasReservation(
	candidate *ingestBatchCandidate,
	reservation *directCommitReservation,
	replay *CommitResult,
	err error,
) error {
	if err != nil {
		if !errors.Is(err, ErrIdempotencyConflict) {
			return err
		}
		if abortErr := s.abortDirectCommit(candidate.reservation, ErrIngestIdentityConflict); abortErr != nil {
			return abortErr
		}
		candidate.reservation = nil
		identityErr := fmt.Errorf(
			"%w for source %q collector %q batch %q: stored batch differs from incoming request",
			ErrIngestIdentityConflict,
			candidate.request.Source,
			candidate.request.CollectorID,
			candidate.request.BatchID,
		)
		markIngestCandidateFailure(candidate, identityErr)
		candidate.metadataOnly = true
		candidate.skipMetadata = true
		return nil
	}
	if replay == nil {
		candidate.batchReservation = reservation
		return nil
	}
	if abortErr := s.abortDirectCommit(candidate.reservation, ErrIngestIdentityConflict); abortErr != nil {
		return abortErr
	}
	candidate.reservation = nil
	applyCoordinatedIngestReplay(candidate, replay)
	return nil
}

func applyCoordinatedIngestReplay(candidate *ingestBatchCandidate, replay *CommitResult) {
	applyCommitResultToIngest(&candidate.result, candidate.request, *replay)
	candidate.result.Skipped = true
	candidate.result.SkipReason = IngestSkipReasonIdempotentReplay
	candidate.metadataOnly = true
	candidate.prepared = false
	candidate.preparedPlan = nil
	candidate.resultManifest = replay.Manifest
}

func marshalCoordinatedIngestReservationIdentity(
	request DirectCommitRequest,
	ingest IngestRequest,
) ([]byte, error) {
	if len(ingest.Preconditions) == 0 && !ingestRequestAtomic(ingest) {
		return json.Marshal(request)
	}
	return json.Marshal(struct {
		Commit        DirectCommitRequest  `json:"commit"`
		FailureMode   string               `json:"failure_mode"`
		Preconditions []IngestPrecondition `json:"preconditions,omitempty"`
	}{
		Commit:        request,
		FailureMode:   ingest.FailureMode,
		Preconditions: ingest.Preconditions,
	})
}

func marshalCoordinatedIngestBatchAliasIdentity(
	request DirectCommitRequest,
	ingest IngestRequest,
) ([]byte, error) {
	return json.Marshal(struct {
		Commit         DirectCommitRequest  `json:"commit"`
		IdempotencyKey string               `json:"idempotency_key"`
		FailureMode    string               `json:"failure_mode"`
		Preconditions  []IngestPrecondition `json:"preconditions,omitempty"`
	}{
		Commit:         request,
		IdempotencyKey: ingest.IdempotencyKey,
		FailureMode:    ingest.FailureMode,
		Preconditions:  ingest.Preconditions,
	})
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

func (s *TenantStore) validateCoordinatedIngestGeneration(
	ctx context.Context,
	tenantID string,
	expected int64,
) error {
	if expected == 0 {
		return nil
	}
	publishState, cached := coordinatorIngestPublishStateFromContext(ctx, tenantID)
	head := publishState.head
	exists := publishState.headExists
	if !cached {
		var err error
		head, exists, err = s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return err
		}
	}
	if !exists {
		if expected == legacyUnboundIngestGeneration {
			return nil
		}
		return fmt.Errorf(
			"%w: %w: tenant %q coordinator head disappeared after WAL acceptance",
			ErrTenantDeleted, errIngestGenerationFenced, tenantID,
		)
	}
	if expected == legacyUnboundIngestGeneration {
		if head.Generation > 1 {
			return fmt.Errorf(
				"%w: %w: tenant %q legacy WAL record is not bound to current generation %d",
				ErrTenantDeleted, errIngestGenerationFenced, tenantID, head.Generation,
			)
		}
	} else if head.Generation != expected {
		return fmt.Errorf(
			"%w: %w: tenant %q WAL generation changed from %d to %d",
			ErrTenantDeleted, errIngestGenerationFenced, tenantID, expected, head.Generation,
		)
	}
	switch head.Status {
	case TenantStatusDisabled:
		return ErrTenantDisabled
	case TenantStatusDeleted:
		return ErrTenantDeleted
	}
	return nil
}

func (s *TenantStore) abortCoordinatedIngestReservations(
	candidates []*ingestBatchCandidate,
	cause error,
) error {
	if !definitiveCommitFailure(cause) {
		return nil
	}
	if batchStore, ok := s.Coordinator.(CoordinatorCommitBatchStore); ok {
		requests := make([]CommitReservationRequest, 0, len(candidates)*2)
		tenantID := ""
		for _, candidate := range candidates {
			for _, reservation := range []*directCommitReservation{candidate.reservation, candidate.batchReservation} {
				if reservation == nil {
					continue
				}
				requests = append(requests, CommitReservationRequest{
					Key: reservation.key, RequestHash: reservation.requestHash,
					OwnerToken: reservation.ownerToken,
				})
				if tenantID == "" {
					tenantID = reservation.record.TenantID
				}
			}
		}
		if len(requests) > 1 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := batchStore.AbortCommitBatch(ctx, tenantID, requests); err != nil {
				return err
			}
			for _, candidate := range candidates {
				candidate.reservation = nil
				candidate.batchReservation = nil
			}
			return nil
		}
	}
	var result error
	for _, candidate := range candidates {
		for _, reservation := range []*directCommitReservation{candidate.reservation, candidate.batchReservation} {
			if reservation == nil {
				continue
			}
			if err := s.abortDirectCommit(reservation, cause); err != nil {
				result = errors.Join(result, err)
			}
		}
		candidate.reservation = nil
		candidate.batchReservation = nil
	}
	return result
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
	items := make([]IngestBatchCompletion, 0, len(candidates)*2)
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
		if alias := candidate.batchReservation; alias != nil {
			items = append(items, IngestBatchCompletion{
				IdempotencyKey: alias.key,
				RequestHash:    alias.requestHash,
				OwnerToken:     alias.ownerToken,
				CommitID:       candidate.resultManifest.HeadCommitID,
				Result:         resultJSON,
			})
		}
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
		Items:        items,
		PublishLease: coordinatorIngestPublishLeaseFromContext(ctx, tenantID),
	})
	if err != nil {
		s.observeCoordinatorCAS(tenantID, "error", 0)
		return err
	}
	if !committed {
		s.observeCoordinatorCAS(tenantID, "conflict", 0)
		return fmt.Errorf("%w: tenant %q changed while completing ingest batch", ErrConflict, tenantID)
	}
	markCoordinatorIngestPublishLeaseReleased(ctx, tenantID)
	s.observeCoordinatorCAS(tenantID, "committed", token.Revision)
	return nil
}

func (s *TenantStore) releaseFailedIngestReservations(candidates []*ingestBatchCandidate) error {
	failed := make([]*ingestBatchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if (candidate.reservation != nil || candidate.batchReservation != nil) && candidate.result.Failed > 0 {
			failed = append(failed, candidate)
		}
	}
	if len(failed) == 0 {
		return nil
	}
	return s.abortCoordinatedIngestReservations(failed, errors.New("ingest request failed permanently"))
}
