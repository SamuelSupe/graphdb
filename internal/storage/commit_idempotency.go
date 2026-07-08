package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"graphdb/internal/graph"
)

type DirectCommitRequest struct {
	ExpectedVersion *int64          `json:"expected_version,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Mutations       graph.Mutations `json:"mutations"`
}

type DirectCommitRecord struct {
	TenantID   string              `json:"tenant_id,omitempty"`
	Request    DirectCommitRequest `json:"request"`
	Result     CommitResult        `json:"result"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt time.Time           `json:"finished_at"`
}

func directCommitRequest(mutations graph.Mutations, opts CommitOptions) DirectCommitRequest {
	return DirectCommitRequest{
		ExpectedVersion: opts.ExpectedVersion,
		IdempotencyKey:  strings.TrimSpace(opts.IdempotencyKey),
		Mutations:       mutations,
	}
}

func (s *TenantStore) loadDirectCommitRecord(ctx context.Context, tenantID string, request DirectCommitRequest) (DirectCommitRecord, bool, error) {
	if request.IdempotencyKey == "" {
		return DirectCommitRecord{}, false, nil
	}
	key := s.commitIdempotencyKey(tenantID, request.IdempotencyKey)
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil {
		return DirectCommitRecord{}, false, err
	}
	if !mayExist {
		return DirectCommitRecord{}, false, nil
	}
	record, err := s.loadDirectCommitRecordByKey(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return DirectCommitRecord{}, false, nil
	}
	if err != nil {
		return DirectCommitRecord{}, false, err
	}
	if record.TenantID != "" && record.TenantID != tenantID {
		return DirectCommitRecord{}, false, fmt.Errorf("commit idempotency tenant mismatch: path tenant %q contains tenant %q", tenantID, record.TenantID)
	}
	if record.Request.IdempotencyKey != request.IdempotencyKey {
		return DirectCommitRecord{}, false, nil
	}
	if !directCommitRequestEqual(record.Request, request) {
		return DirectCommitRecord{}, false, fmt.Errorf("commit idempotency conflict for key %q: stored request differs from incoming request", request.IdempotencyKey)
	}
	return record, true, nil
}

func (s *TenantStore) saveDirectCommitRecord(ctx context.Context, tenantID string, request DirectCommitRequest, result CommitResult, started time.Time, finished time.Time) error {
	if request.IdempotencyKey == "" {
		return nil
	}
	record := DirectCommitRecord{TenantID: tenantID, Request: request, Result: result, StartedAt: started, FinishedAt: finished}
	data, err := marshalParquetDirectCommitRecord(ctx, record)
	if err != nil {
		return err
	}
	key := s.commitIdempotencyKey(tenantID, request.IdempotencyKey)
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	if err == nil {
		s.markObjectKeyCached(key)
		return nil
	}
	if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.loadDirectCommitRecordByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: existing commit idempotency record is unreadable; repair blocked", err)
	}
	if directCommitRecordSameResult(existing, record) {
		return nil
	}
	if existing.TenantID == "" || existing.TenantID == tenantID {
		return fmt.Errorf("%w: commit idempotency key %q changed while publishing", ErrConflict, request.IdempotencyKey)
	}
	return fmt.Errorf("%w: commit idempotency key %q belongs to another tenant", ErrConflict, request.IdempotencyKey)
}

func (s *TenantStore) loadDirectCommitRecordByKey(ctx context.Context, key string) (DirectCommitRecord, error) {
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return DirectCommitRecord{}, err
	}
	if !isParquetBytes(data) {
		return DirectCommitRecord{}, fmt.Errorf("unsupported commit idempotency record: only parquet records are readable")
	}
	return decodeParquetDirectCommitRecord(ctx, data)
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
