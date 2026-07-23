package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const (
	directCommitStatusPending   = "pending"
	directCommitStatusPrepared  = "prepared"
	directCommitStatusCommitted = "committed"
)

type DirectCommitRequest struct {
	ExpectedVersion *int64                `json:"expected_version,omitempty"`
	IdempotencyKey  string                `json:"idempotency_key,omitempty"`
	CollectorState  *CollectorStateUpdate `json:"collector_state,omitempty"`
	Mutations       graph.Mutations       `json:"mutations"`
}

type DirectCommitRecord struct {
	TenantID   string              `json:"tenant_id,omitempty"`
	Status     string              `json:"status,omitempty"`
	Request    DirectCommitRequest `json:"request"`
	Result     CommitResult        `json:"result"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt time.Time           `json:"finished_at"`
}

type directCommitReservation struct {
	key         string
	record      DirectCommitRecord
	meta        ObjectMeta
	coordinated bool
	requestHash string
	ownerToken  string
	renewal     *commitReservationRenewal
}

func directCommitRequest(mutations graph.Mutations, opts CommitOptions) DirectCommitRequest {
	return DirectCommitRequest{
		ExpectedVersion: opts.ExpectedVersion,
		IdempotencyKey:  strings.TrimSpace(opts.IdempotencyKey),
		CollectorState:  opts.collectorState,
		Mutations:       mutations,
	}
}

func (s *TenantStore) beginDirectCommit(ctx context.Context, tenantID string, request DirectCommitRequest, started time.Time) (*directCommitReservation, *CommitResult, error) {
	if request.IdempotencyKey == "" {
		return nil, nil, nil
	}
	if s.coordinated() {
		return s.beginCoordinatedDirectCommit(ctx, tenantID, request, started)
	}
	key := s.commitIdempotencyKey(tenantID, request.IdempotencyKey)
	pending := DirectCommitRecord{
		TenantID:  tenantID,
		Status:    directCommitStatusPending,
		Request:   request,
		StartedAt: started,
	}
	data, err := marshalParquetDirectCommitRecord(ctx, pending)
	if err != nil {
		return nil, nil, err
	}
	meta, err := s.putTenantConditionalIfBound(ctx, tenantID, key, data, PutCondition{IfNoneMatch: true})
	if err == nil {
		s.markObjectKeyCached(key)
		return &directCommitReservation{key: key, record: pending, meta: meta}, nil, nil
	}
	if !errors.Is(err, ErrConflict) {
		return nil, nil, err
	}

	existing, meta, err := s.loadDirectCommitRecordWithMeta(ctx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("load existing commit idempotency record: %w", err)
	}
	if err := validateDirectCommitRecord(tenantID, request, existing); err != nil {
		return nil, nil, err
	}
	switch directCommitRecordStatus(existing) {
	case directCommitStatusCommitted:
		result := replayDirectCommitResult(existing)
		return nil, &result, nil
	case directCommitStatusPending:
		return &directCommitReservation{key: key, record: existing, meta: meta}, nil, nil
	case directCommitStatusPrepared:
		published, decisive, err := s.directCommitPreparedPublished(ctx, tenantID, existing)
		if err != nil {
			return nil, nil, err
		}
		if published {
			result := replayDirectCommitResult(existing)
			return nil, &result, nil
		}
		if !decisive {
			return nil, nil, fmt.Errorf("%w: commit idempotency key %q has an ambiguous prepared outcome", ErrConflict, request.IdempotencyKey)
		}
		return &directCommitReservation{key: key, record: existing, meta: meta}, nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported commit idempotency status %q", existing.Status)
	}
}

func (s *TenantStore) prepareDirectCommit(ctx context.Context, reservation *directCommitReservation, result CommitResult, finished time.Time) error {
	if reservation == nil {
		return nil
	}
	record := reservation.record
	record.Status = directCommitStatusPrepared
	record.Result = result
	record.FinishedAt = finished
	if reservation.coordinated {
		reservation.record = record
		return nil
	}
	return s.updateDirectCommitReservation(ctx, reservation, record)
}

func (s *TenantStore) completeDirectCommit(ctx context.Context, reservation *directCommitReservation, result CommitResult, finished time.Time) error {
	if reservation == nil {
		return nil
	}
	if reservation.coordinated {
		return nil
	}
	record := reservation.record
	record.Status = directCommitStatusCommitted
	record.Result = result
	record.FinishedAt = finished
	return s.updateDirectCommitReservation(ctx, reservation, record)
}

func (s *TenantStore) abortDirectCommit(
	reservation *directCommitReservation,
	commitErr error,
) error {
	if reservation == nil || !reservation.coordinated || !definitiveCommitFailure(commitErr) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Coordinator.AbortCommit(
		ctx,
		reservation.record.TenantID,
		reservation.key,
		reservation.requestHash,
		reservation.ownerToken,
	)
}

func definitiveCommitFailure(err error) bool {
	return err != nil &&
		!errors.Is(err, ErrCoordinatorUnavailable) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}

func (s *TenantStore) beginCoordinatedDirectCommit(
	ctx context.Context,
	tenantID string,
	request DirectCommitRequest,
	started time.Time,
) (*directCommitReservation, *CommitResult, error) {
	data, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	requestHash := objectContentHash(data)
	ownerToken, err := newCommitID()
	if err != nil {
		return nil, nil, err
	}
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
			return nil, nil, fmt.Errorf("decode coordinated commit result: %w", err)
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
			StartedAt: started,
		},
		coordinated: true,
		requestHash: requestHash,
		ownerToken:  ownerToken,
	}, nil, nil
}

func (s *TenantStore) updateDirectCommitReservation(ctx context.Context, reservation *directCommitReservation, record DirectCommitRecord) error {
	data, err := marshalParquetDirectCommitRecord(ctx, record)
	if err != nil {
		return err
	}
	condition := PutCondition{IfNoneMatch: !reservation.meta.Exists}
	if reservation.meta.Exists {
		condition.IfMatch = reservation.meta.ETag
	}
	meta, err := s.putTenantConditionalIfBound(ctx, record.TenantID, reservation.key, data, condition)
	if err != nil {
		return err
	}
	reservation.record = record
	reservation.meta = meta
	s.markObjectKeyCached(reservation.key)
	return nil
}

func (s *TenantStore) loadDirectCommitRecordWithMeta(ctx context.Context, key string) (DirectCommitRecord, ObjectMeta, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return DirectCommitRecord{}, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return DirectCommitRecord{}, ObjectMeta{}, fmt.Errorf("unsupported commit idempotency record: only parquet records are readable")
	}
	record, err := decodeParquetDirectCommitRecord(ctx, data)
	return record, meta, err
}

func validateDirectCommitRecord(tenantID string, request DirectCommitRequest, record DirectCommitRecord) error {
	if record.TenantID != "" && record.TenantID != tenantID {
		return fmt.Errorf("commit idempotency tenant mismatch: path tenant %q contains tenant %q", tenantID, record.TenantID)
	}
	if record.Request.IdempotencyKey != request.IdempotencyKey || !directCommitRequestEqual(record.Request, request) {
		return fmt.Errorf("commit idempotency conflict for key %q: stored request differs from incoming request", request.IdempotencyKey)
	}
	return nil
}

func directCommitRecordStatus(record DirectCommitRecord) string {
	if record.Status == "" {
		return directCommitStatusCommitted
	}
	return record.Status
}

func replayDirectCommitResult(record DirectCommitRecord) CommitResult {
	result := record.Result
	result.Skipped = true
	result.IdempotentReplay = true
	return result
}

func directCommitRecordSameResult(stored DirectCommitRecord, incoming DirectCommitRecord) bool {
	if !directCommitRequestEqual(stored.Request, incoming.Request) {
		return false
	}
	storedResult, err := json.Marshal(stored.Result)
	if err != nil {
		return false
	}
	incomingResult, err := json.Marshal(incoming.Result)
	if err != nil {
		return false
	}
	return bytes.Equal(storedResult, incomingResult)
}

func directCommitRequestEqual(stored DirectCommitRequest, incoming DirectCommitRequest) bool {
	stored.IdempotencyKey = strings.TrimSpace(stored.IdempotencyKey)
	incoming.IdempotencyKey = strings.TrimSpace(incoming.IdempotencyKey)
	storedJSON, err := json.Marshal(stored)
	if err != nil {
		return false
	}
	incomingJSON, err := json.Marshal(incoming)
	if err != nil {
		return false
	}
	return bytes.Equal(storedJSON, incomingJSON)
}
