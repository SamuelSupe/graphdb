package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func (s *TenantStore) GetReplicatedIngestReceipt(ctx context.Context, tenant string, generation int64, source, collector, batch string) (IngestBatchRecord, error) {
	request := IngestRequest{Source: source, CollectorID: collector, BatchID: batch}
	if ValidateTenantID(tenant) != nil || ValidateIngestStorageIdentifiers(request) != nil {
		return IngestBatchRecord{}, ErrNotFound
	}
	record, _, err := s.loadIngestRecordWithMeta(ctx, s.ingestBatchKey(tenant, source, collector, batch))
	if err != nil {
		return IngestBatchRecord{}, err
	}
	if record.ReplicatedReceipt == nil || record.ReplicatedReceipt.Generation != generation ||
		!ingestRecordMatchesBatchIdentity(record, tenant, request) {
		return IngestBatchRecord{}, ErrNotFound
	}
	return record, nil
}

// ReclaimReplicatedIngestReceipts removes only tenant copies bound to the
// supplied HA acceptances, as part of the caller's Raft apply.
func (s *TenantStore) ReclaimReplicatedIngestReceipts(ctx context.Context, receipts []IngestBatchRecord) error {
	if len(receipts) == 0 {
		return nil
	}
	if !IsReplicatedContext(ctx) {
		return ErrInvalidArgument
	}
	var keys []string
	collectors := make(map[string]struct{})
	for _, record := range receipts {
		tenant, request, receipt := record.TenantID, record.Request, record.ReplicatedReceipt
		if receipt == nil || ValidateTenantID(tenant) != nil || ValidateIngestStorageIdentifiers(request) != nil {
			return ErrInvalidArgument
		}
		candidates := []string{s.ingestBatchKey(tenant, request.Source, request.CollectorID, request.BatchID)}
		if request.IdempotencyKey != "" && request.IdempotencyKey != request.BatchID {
			candidates = append(candidates, s.ingestIdempotencyKey(tenant, request.Source, request.CollectorID, request.IdempotencyKey))
		}
		for _, key := range candidates {
			stored, _, err := s.loadIngestRecordWithMeta(ctx, key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			binding := stored.ReplicatedReceipt
			if binding != nil && binding.Generation == receipt.Generation && binding.Index == receipt.Index && binding.Epoch == receipt.Epoch && binding.Digest == receipt.Digest &&
				ingestRecordMatchesBatchIdentity(stored, tenant, request) && stored.Request.IdempotencyKey == request.IdempotencyKey {
				keys = append(keys, key)
				if !s.MaterializeCollectorStatus {
					collectors[s.collectorStatusKey(tenant, request.Source, request.CollectorID)] = struct{}{}
				}
			}
		}
	}
	if files := s.localFileStore(); files != nil {
		if err := files.JournalReplicatedObjects(ctx, keys); err != nil {
			return err
		}
	}
	for _, key := range keys {
		if err := s.Objects.Delete(ctx, key); err != nil {
			return err
		}
	}
	for key := range collectors {
		s.deleteCachedCollectorStatus(key)
	}
	return nil
}

// BindReplicatedIngestReceipt checks that a legacy Parquet record can replace
// the original terminal control record without changing the receipt.
func (s *TenantStore) BindReplicatedIngestReceipt(ctx context.Context, desired IngestBatchRecord, reclaimedBytes int64, publish bool) (bool, error) {
	if desired.ReplicatedReceipt == nil || desired.Result.SkipReason == IngestSkipReasonIdempotentReplay {
		return false, nil
	}
	key := s.ingestBatchKey(desired.TenantID, desired.Request.Source, desired.Request.CollectorID, desired.Request.BatchID)
	raw, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	stored, err := decodeParquetIngestRecord(ctx, raw)
	if err != nil {
		return false, err
	}
	if !ingestRecordMatchesBatchIdentity(stored, desired.TenantID, desired.Request) ||
		!stored.StartedAt.Equal(desired.StartedAt) || !stored.FinishedAt.Equal(desired.FinishedAt) {
		return false, nil
	}
	request, err := json.Marshal(stored.Request)
	if err != nil {
		return false, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(request)) != desired.ReplicatedReceipt.Digest {
		return false, nil
	}
	desired.Request = stored.Request
	if !ingestRecordSameResult(stored, desired) {
		return false, nil
	}
	if stored.ReplicatedReceipt != nil {
		return *stored.ReplicatedReceipt == *desired.ReplicatedReceipt, nil
	}
	stored.ReplicatedReceipt = desired.ReplicatedReceipt
	data, err := marshalParquetIngestRecord(ctx, stored)
	if err != nil {
		return false, err
	}
	if int64(len(data)-len(raw)) > reclaimedBytes {
		return false, nil
	}
	if publish {
		if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag}); err != nil {
			return false, err
		}
		s.markObjectKeyCached(key)
	}
	return true, nil
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
	_, err := s.saveIngestBatchRecord(ctx, tenantID, record)
	return err
}

func (s *TenantStore) saveIngestBatchRecord(ctx context.Context, tenantID string, record IngestBatchRecord) (bool, error) {
	record.TenantID = tenantID
	record.Result = canonicalStoredIngestResult(record.Result)
	data, err := marshalParquetIngestRecord(ctx, record)
	if err != nil {
		if record.Request.IdempotencyKey != "" &&
			record.Request.IdempotencyKey != record.Result.BatchID {
			return false, fmt.Errorf("save idempotency record: %w", err)
		}
		return false, fmt.Errorf("save batch record: %w", err)
	}

	batchKey := s.ingestBatchKey(tenantID, record.Request.Source, record.Request.CollectorID, record.Result.BatchID)
	if record.Request.IdempotencyKey == "" || record.Request.IdempotencyKey == record.Result.BatchID {
		stored, err := s.saveEncodedIngestRecord(ctx, tenantID, batchKey, data, record)
		if err != nil {
			return false, fmt.Errorf("save batch record: %w", err)
		}
		return stored, nil
	}

	idempotencyKey := s.ingestIdempotencyKey(tenantID, record.Request.Source, record.Request.CollectorID, record.Request.IdempotencyKey)
	if s.LocalReceiptPackingEnabled() {
		packCtx := ctx
		done := func(error) {}
		if metered := FindMeteredObjectStore(s.Objects); metered != nil {
			packCtx, done = metered.start(ctx, "put_ingest_receipt_pair", batchKey, len(data)*2)
		}
		cache := FindWriterObjectCache(s.Objects)
		keys := [...]string{batchKey, idempotencyKey}
		var generations [len(keys)]uint64
		if cache != nil {
			for i, key := range keys {
				generations[i] = cache.generation(key)
			}
		}
		meta, stored, err := s.localFileStore().putNewIngestReceiptPair(packCtx, batchKey, idempotencyKey, data)
		done(err)
		if err != nil {
			return false, fmt.Errorf("save packed ingest receipts: %w", err)
		}
		if stored {
			for i, key := range keys {
				s.markObjectKeyCached(key)
				if cache != nil {
					meta.Key = key
					cache.finishMutation(key, generations[i], data, meta, true, false)
				}
			}
			return true, nil
		}
	}
	var (
		idempotencyErr error
		batchErr       error
		batchStored    bool
		wait           sync.WaitGroup
	)
	wait.Add(2)
	go func() {
		defer wait.Done()
		_, idempotencyErr = s.saveEncodedIngestRecord(ctx, tenantID, idempotencyKey, data, record)
	}()
	go func() {
		defer wait.Done()
		batchStored, batchErr = s.saveEncodedIngestRecord(ctx, tenantID, batchKey, data, record)
	}()
	wait.Wait()

	var result error
	if idempotencyErr != nil {
		result = errors.Join(result, fmt.Errorf("save idempotency record: %w", idempotencyErr))
	}
	if batchErr != nil {
		result = errors.Join(result, fmt.Errorf("save batch record: %w", batchErr))
	}
	return batchStored, result
}

// The bool proves that this write, or an equivalent prior write, stores the
// receipt of the same HA acceptance. A storage-only replay can still succeed.
func (s *TenantStore) saveEncodedIngestRecord(ctx context.Context, tenantID string, key string, data []byte, record IngestBatchRecord) (bool, error) {
	_, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	if err == nil {
		s.markObjectKeyCached(key)
		return true, nil
	}
	if !errors.Is(err, ErrConflict) {
		return false, err
	}
	existing, meta, err := s.loadIngestRecordWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		_, retryErr := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
		if retryErr == nil {
			s.markObjectKeyCached(key)
		}
		return retryErr == nil, retryErr
	}
	if err != nil {
		return false, fmt.Errorf("%w: existing ingest record is unreadable; repair blocked", err)
	}
	if ingestRecordSameResult(existing, record) {
		exact := record.ReplicatedReceipt != nil && existing.ReplicatedReceipt != nil &&
			*existing.ReplicatedReceipt == *record.ReplicatedReceipt &&
			existing.StartedAt.Equal(record.StartedAt) && existing.FinishedAt.Equal(record.FinishedAt)
		return exact, nil
	}

	if ingestRecordMatchesBatchIdentity(existing, tenantID, record.Request) {
		if !ingestRecordRequestEqual(existing.Request, record.Request) {
			return false, fmt.Errorf(
				"%w: ingest record for source %q collector %q batch %q changed payload",
				ErrIngestIdentityConflict,
				record.Request.Source,
				record.Request.CollectorID,
				record.Request.BatchID,
			)
		}
		return false, fmt.Errorf("%w: ingest record for source %q collector %q batch %q idempotency %q changed while publishing", ErrConflict, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Request.IdempotencyKey)
	}
	if ingestRecordMatchesIdempotencyIdentity(existing, tenantID, record.Request) {
		if !ingestRecordRequestEqualIgnoringBatch(existing.Request, record.Request) {
			return false, fmt.Errorf(
				"%w: ingest record for source %q collector %q idempotency %q changed payload",
				ErrIngestIdentityConflict,
				record.Request.Source,
				record.Request.CollectorID,
				record.Request.IdempotencyKey,
			)
		}
		return false, fmt.Errorf("%w: ingest record for source %q collector %q batch %q idempotency %q changed while publishing", ErrConflict, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Request.IdempotencyKey)
	}
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	if errors.Is(err, ErrConflict) {
		return false, fmt.Errorf("%w: ingest record changed while repairing mismatched metadata", ErrConflict)
	}
	return err == nil, err
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
