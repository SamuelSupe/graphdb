package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const ingestRecordProbeConcurrency = 4

type ingestRecordLookupCandidate struct {
	key         string
	idempotency bool
}

type ingestRecordKeyProbe struct {
	mayExist bool
	err      error
}

// CaptureIngestWALGeneration returns the current tenant generation that a
// local WAL acceptance must remain bound to until terminalization.
func (s *TenantStore) CaptureIngestWALGeneration(ctx context.Context, tenantID string) (int64, error) {
	return s.localIngestGeneration(ctx, tenantID)
}

func (s *TenantStore) GetIngestBatch(ctx context.Context, tenantID string, source string, collectorID string, batchID string) (IngestBatchRecord, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IngestBatchRecord{}, err
	}
	source = strings.TrimSpace(source)
	collectorID = strings.TrimSpace(collectorID)
	batchID = strings.TrimSpace(batchID)
	if source == "" || collectorID == "" || batchID == "" {
		return IngestBatchRecord{}, fmt.Errorf("source, collector_id, and batch_id are required")
	}
	keys := []string{
		s.ingestBatchKey(tenantID, source, collectorID, batchID),
		s.legacyIngestBatchKey(tenantID, source, batchID),
	}
	for _, key := range keys {
		record, _, err := s.loadIngestRecordWithMeta(ctx, key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return IngestBatchRecord{}, err
		}
		if (record.TenantID == "" || record.TenantID == tenantID) &&
			record.Request.Source == source &&
			record.Request.CollectorID == collectorID &&
			record.Result.BatchID == batchID {
			return record, nil
		}
	}
	return IngestBatchRecord{}, ErrNotFound
}

// LookupIngestReplay finds a terminal receipt for a prepared request and checks
// that any matching batch or idempotency identity still refers to its content.
func (s *TenantStore) LookupIngestReplay(ctx context.Context, tenantID string, request IngestRequest) (IngestBatchRecord, bool, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IngestBatchRecord{}, false, err
	}
	return s.loadIngestRecord(ctx, tenantID, request)
}

func (s *TenantStore) loadIngestRecord(ctx context.Context, tenantID string, request IngestRequest) (IngestBatchRecord, bool, error) {
	candidates := make([]ingestRecordLookupCandidate, 0, 6)
	if request.IdempotencyKey != "" {
		candidates = append(candidates,
			ingestRecordLookupCandidate{
				key:         s.ingestIdempotencyKey(tenantID, request.Source, request.CollectorID, request.IdempotencyKey),
				idempotency: true,
			},
			ingestRecordLookupCandidate{
				key:         s.legacyIngestIdempotencyKey(tenantID, request.Source, request.IdempotencyKey),
				idempotency: true,
			},
		)
		if request.BatchID != request.IdempotencyKey {
			candidates = append(candidates,
				ingestRecordLookupCandidate{
					key:         s.ingestBatchKey(tenantID, request.Source, request.CollectorID, request.IdempotencyKey),
					idempotency: true,
				},
				ingestRecordLookupCandidate{
					key:         s.legacyIngestBatchKey(tenantID, request.Source, request.IdempotencyKey),
					idempotency: true,
				},
			)
		}
	}
	if request.BatchID != "" {
		candidates = append(candidates,
			ingestRecordLookupCandidate{
				key: s.ingestBatchKey(tenantID, request.Source, request.CollectorID, request.BatchID),
			},
			ingestRecordLookupCandidate{
				key: s.legacyIngestBatchKey(tenantID, request.Source, request.BatchID),
			},
		)
	}
	if len(candidates) == 0 {
		return IngestBatchRecord{}, false, nil
	}

	record, ok, err := s.loadIngestRecordCandidate(
		ctx, tenantID, candidates[0], request, false,
	)
	if err != nil || ok || len(candidates) == 1 {
		return record, ok, err
	}

	// Keep the current-format primary lookup latency unchanged for replays.
	// Only the independent compatibility fallbacks are probed concurrently.
	probes := s.probeIngestRecordKeys(ctx, candidates[1:])
	for i, candidate := range candidates[1:] {
		probe := probes[i]
		if probe.err != nil {
			return IngestBatchRecord{}, false, probe.err
		}
		if !probe.mayExist {
			continue
		}
		record, ok, err := s.loadIngestRecordCandidate(
			ctx, tenantID, candidate, request, true,
		)
		if err != nil || ok {
			return record, ok, err
		}
	}
	return IngestBatchRecord{}, false, nil
}

func (s *TenantStore) probeIngestRecordKeys(ctx context.Context, candidates []ingestRecordLookupCandidate) []ingestRecordKeyProbe {
	probes := make([]ingestRecordKeyProbe, len(candidates))
	jobs := make(chan int, len(candidates))
	for i := range candidates {
		jobs <- i
	}
	close(jobs)

	workers := min(ingestRecordProbeConcurrency, len(candidates))
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for i := range jobs {
				probes[i].mayExist, probes[i].err =
					s.objectKeyMayExist(ctx, candidates[i].key)
			}
		}()
	}
	wait.Wait()
	return probes
}

func (s *TenantStore) loadIngestRecordCandidate(
	ctx context.Context,
	tenantID string,
	candidate ingestRecordLookupCandidate,
	request IngestRequest,
	knownToExist bool,
) (IngestBatchRecord, bool, error) {
	if candidate.idempotency {
		return s.loadMatchingIngestIdempotencyRecord(
			ctx, tenantID, candidate.key, request, knownToExist,
		)
	}
	return s.loadMatchingIngestRecord(
		ctx, tenantID, candidate.key, request, knownToExist,
	)
}

func (s *TenantStore) loadMatchingIngestRecord(ctx context.Context, tenantID string, key string, request IngestRequest, knownToExist bool) (IngestBatchRecord, bool, error) {
	if !knownToExist {
		mayExist, err := s.objectKeyMayExist(ctx, key)
		if err != nil {
			return IngestBatchRecord{}, false, err
		}
		if !mayExist {
			return IngestBatchRecord{}, false, nil
		}
	}
	record, _, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return IngestBatchRecord{}, false, nil
	}
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if !ingestRecordMatchesBatchIdentity(record, tenantID, request) {
		return IngestBatchRecord{}, false, nil
	}
	if !ingestRecordRequestEqual(record.Request, request) {
		return IngestBatchRecord{}, false, fmt.Errorf(
			"%w: ingest record conflict for source %q collector %q batch %q idempotency %q: stored request differs from incoming request",
			ErrIngestIdentityConflict,
			request.Source,
			request.CollectorID,
			request.BatchID,
			request.IdempotencyKey,
		)
	}
	return record, true, nil
}

func (s *TenantStore) loadMatchingIngestIdempotencyRecord(ctx context.Context, tenantID string, key string, request IngestRequest, knownToExist bool) (IngestBatchRecord, bool, error) {
	if !knownToExist {
		mayExist, err := s.objectKeyMayExist(ctx, key)
		if err != nil {
			return IngestBatchRecord{}, false, err
		}
		if !mayExist {
			return IngestBatchRecord{}, false, nil
		}
	}
	record, _, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return IngestBatchRecord{}, false, nil
	}
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if !ingestRecordMatchesIdempotencyIdentity(record, tenantID, request) {
		return IngestBatchRecord{}, false, nil
	}
	if !ingestRecordRequestEqualIgnoringBatch(record.Request, request) {
		return IngestBatchRecord{}, false, fmt.Errorf(
			"%w: ingest record conflict for source %q collector %q batch %q idempotency %q: stored request differs from incoming request",
			ErrIngestIdentityConflict,
			request.Source,
			request.CollectorID,
			request.BatchID,
			request.IdempotencyKey,
		)
	}
	return record, true, nil
}

func (s *TenantStore) repairIngestMetadataAfterSkip(ctx context.Context, tenantID string, record IngestBatchRecord, saveFailures bool) error {
	var metadataErr error

	if err := s.repairCollectorStatusAfterSkip(ctx, tenantID, record); err != nil {
		metadataErr = errors.Join(metadataErr, fmt.Errorf("save collector status: %w", err))
	}

	if saveFailures && record.Result.Failed > 0 {
		if err := s.ensureDeadLetterAfterSkip(ctx, tenantID, record.Request, record.Result); err != nil {
			metadataErr = errors.Join(metadataErr, fmt.Errorf("save dead letter: %w", err))
		}
	}
	return metadataErr
}

func (s *TenantStore) saveIngestBatch(ctx context.Context, tenantID string, record IngestBatchRecord) error {
	record.TenantID = tenantID
	record.Result = canonicalStoredIngestResult(record.Result)
	data, err := marshalParquetIngestRecord(ctx, record)
	if err != nil {
		if record.Request.IdempotencyKey != "" &&
			record.Request.IdempotencyKey != record.Result.BatchID {
			return fmt.Errorf("save idempotency record: %w", err)
		}
		return fmt.Errorf("save batch record: %w", err)
	}

	batchKey := s.ingestBatchKey(tenantID, record.Request.Source, record.Request.CollectorID, record.Result.BatchID)
	if record.Request.IdempotencyKey == "" || record.Request.IdempotencyKey == record.Result.BatchID {
		if err := s.saveEncodedIngestRecord(ctx, tenantID, batchKey, data, record); err != nil {
			return fmt.Errorf("save batch record: %w", err)
		}
		return nil
	}

	idempotencyKey := s.ingestIdempotencyKey(tenantID, record.Request.Source, record.Request.CollectorID, record.Request.IdempotencyKey)
	if s.LocalReceiptPackingEnabled() {
		packCtx := ctx
		done := func(error) {}
		if metered := FindMeteredObjectStore(s.Objects); metered != nil {
			packCtx, done = metered.start(ctx, "put_ingest_receipt_pair", batchKey, len(data)*2)
		}
		meta, stored, err := s.localFileStore().putNewIngestReceiptPair(packCtx, batchKey, idempotencyKey, data)
		done(err)
		if err != nil {
			return fmt.Errorf("save packed ingest receipts: %w", err)
		}
		if stored {
			cache := FindWriterObjectCache(s.Objects)
			for _, key := range []string{batchKey, idempotencyKey} {
				s.markObjectKeyCached(key)
				if cache != nil {
					meta.Key = key
					cache.cachePositive(key, data, meta, true)
					cache.updateCachedLists(key, ObjectInfo{Key: key, Size: int64(len(data)), ETag: meta.ETag}, false)
				}
			}
			return nil
		}
	}
	var (
		idempotencyErr error
		batchErr       error
		wait           sync.WaitGroup
	)
	wait.Add(2)
	go func() {
		defer wait.Done()
		idempotencyErr = s.saveEncodedIngestRecord(ctx, tenantID, idempotencyKey, data, record)
	}()
	go func() {
		defer wait.Done()
		batchErr = s.saveEncodedIngestRecord(ctx, tenantID, batchKey, data, record)
	}()
	wait.Wait()

	var result error
	if idempotencyErr != nil {
		result = errors.Join(result, fmt.Errorf("save idempotency record: %w", idempotencyErr))
	}
	if batchErr != nil {
		result = errors.Join(result, fmt.Errorf("save batch record: %w", batchErr))
	}
	return result
}

func (s *TenantStore) saveEncodedIngestRecord(ctx context.Context, tenantID string, key string, data []byte, record IngestBatchRecord) error {
	_, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	if err == nil {
		s.markObjectKeyCached(key)
		return nil
	}
	if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, meta, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		_, retryErr := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
		if retryErr == nil {
			s.markObjectKeyCached(key)
		}
		return retryErr
	}
	if err != nil {
		return fmt.Errorf("%w: existing ingest record is unreadable; repair blocked", err)
	}
	if ingestRecordSameResult(existing, record) {
		return nil
	}

	if ingestRecordMatchesBatchIdentity(existing, tenantID, record.Request) {
		if !ingestRecordRequestEqual(existing.Request, record.Request) {
			return fmt.Errorf(
				"%w: ingest record for source %q collector %q batch %q changed payload",
				ErrIngestIdentityConflict,
				record.Request.Source,
				record.Request.CollectorID,
				record.Request.BatchID,
			)
		}
		return fmt.Errorf("%w: ingest record for source %q collector %q batch %q idempotency %q changed while publishing", ErrConflict, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Request.IdempotencyKey)
	}
	if ingestRecordMatchesIdempotencyIdentity(existing, tenantID, record.Request) {
		if !ingestRecordRequestEqualIgnoringBatch(existing.Request, record.Request) {
			return fmt.Errorf(
				"%w: ingest record for source %q collector %q idempotency %q changed payload",
				ErrIngestIdentityConflict,
				record.Request.Source,
				record.Request.CollectorID,
				record.Request.IdempotencyKey,
			)
		}
		return fmt.Errorf("%w: ingest record for source %q collector %q batch %q idempotency %q changed while publishing", ErrConflict, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Request.IdempotencyKey)
	}
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	if errors.Is(err, ErrConflict) {
		return fmt.Errorf("%w: ingest record changed while repairing mismatched metadata", ErrConflict)
	}
	return err
}

func (s *TenantStore) loadIngestRecordWithMeta(ctx context.Context, key string) (IngestBatchRecord, ObjectMeta, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return IngestBatchRecord{}, meta, err
	}
	if !isParquetBytes(data) {
		return IngestBatchRecord{}, meta, fmt.Errorf("unsupported ingest record %q: only parquet ingest records are readable", key)
	}
	record, err := decodeParquetIngestRecord(ctx, data)
	if err != nil {
		return IngestBatchRecord{}, meta, err
	}
	return record, meta, nil
}

func ingestRecordSameResult(stored IngestBatchRecord, incoming IngestBatchRecord) bool {
	if !ingestRecordRequestEqual(stored.Request, incoming.Request) {
		return false
	}
	stored.Result = canonicalStoredIngestResult(stored.Result)
	incoming.Result = canonicalStoredIngestResult(incoming.Result)
	stored.Result.SkipReason = ""
	incoming.Result.SkipReason = ""
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

func canonicalStoredIngestResult(result IngestResult) IngestResult {
	if result.SkipReason == IngestSkipReasonIdempotentReplay {
		result.Skipped = false
		result.SkipReason = ""
	}
	return result
}

func ingestRecordMatchesRequest(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return ingestRecordMatchesIdentity(record, tenantID, request) && ingestRecordRequestEqual(record.Request, request)
}

func ingestRecordMatchesIdentity(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return ingestRecordMatchesBatchIdentity(record, tenantID, request) &&
		record.Request.IdempotencyKey == request.IdempotencyKey
}

func ingestRecordMatchesBatchIdentity(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return (record.TenantID == "" || record.TenantID == tenantID) &&
		record.Request.Source == request.Source &&
		record.Request.CollectorID == request.CollectorID &&
		record.Request.BatchID == request.BatchID &&
		record.Result.BatchID == request.BatchID
}

func ingestRecordMatchesIdempotencyIdentity(record IngestBatchRecord, tenantID string, request IngestRequest) bool {
	return request.IdempotencyKey != "" &&
		(record.TenantID == "" || record.TenantID == tenantID) &&
		record.Request.Source == request.Source &&
		record.Request.CollectorID == request.CollectorID &&
		record.Request.IdempotencyKey == request.IdempotencyKey &&
		record.Result.BatchID == record.Request.BatchID
}

func ingestRecordRequestEqual(stored IngestRequest, incoming IngestRequest) bool {
	if stored.FailureMode == "" {
		stored.FailureMode = IngestFailureModeBestEffort
	}
	if incoming.FailureMode == "" {
		incoming.FailureMode = IngestFailureModeBestEffort
	}
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

func ingestRecordRequestEqualIgnoringBatch(stored IngestRequest, incoming IngestRequest) bool {
	stored.BatchID = ""
	incoming.BatchID = ""
	return ingestRecordRequestEqual(stored, incoming)
}
