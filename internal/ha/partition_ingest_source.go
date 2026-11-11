package ha

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Conversion retains source receipts on the home group. Reuse their lookup
// paths so converting a large graph never needs to scan its ingest history.
func (a *Application) replayPartitionSourceIngest(ctx context.Context, candidate acceptedRequest, sourceGeneration int64) (acceptedRequest, bool, error) {
	request := candidate.Request
	oldKey := a.ingestKey(candidate.Tenant, request.Source, request.CollectorID, request.BatchID, sourceGeneration)
	previous, err := a.accepted(ctx, oldKey)
	if err == nil {
		if previous.Digest != candidate.Digest || previous.Result == nil || previous.State == "accepted" {
			return acceptedRequest{}, false, fmt.Errorf("%w: source WAL identity already has a different or unfinished receipt", storage.ErrIngestIdentityConflict)
		}
	} else {
		if !errors.Is(err, storage.ErrNotFound) {
			return acceptedRequest{}, false, err
		}
		stored, found, err := a.Store.LookupIngestReplay(ctx, candidate.Tenant, request)
		if err != nil || !found {
			return acceptedRequest{}, false, err
		}
		request.BatchID = stored.Request.BatchID
		previous, err = a.accepted(ctx, a.ingestKey(candidate.Tenant, request.Source, request.CollectorID, request.BatchID, sourceGeneration))
		if errors.Is(err, storage.ErrNotFound) {
			previous = sourceIngestReceipt(candidate.Tenant, sourceGeneration, stored)
		} else if err != nil {
			return acceptedRequest{}, false, err
		} else if previous.Result == nil || previous.State == "accepted" {
			return acceptedRequest{}, false, fmt.Errorf("%w: source WAL receipt is unfinished", storage.ErrIngestIdentityConflict)
		}
	}
	data, err := json.Marshal(request)
	if err != nil {
		return acceptedRequest{}, false, err
	}
	previous.Digest = fmt.Sprintf("%x", sha256.Sum256(data))
	previous.Generation, previous.Partitioned = candidate.Generation, true
	if previous.Index == 0 {
		previous.Index = candidate.Index
	}
	key := a.ingestKey(candidate.Tenant, request.Source, request.CollectorID, request.BatchID, candidate.Generation)
	if request.BatchID != candidate.Request.BatchID {
		current, err := a.accepted(ctx, key)
		if err == nil {
			if current.Digest != previous.Digest {
				return acceptedRequest{}, false, fmt.Errorf("%w: canonical batch already exists with different content", storage.ErrIngestIdentityConflict)
			}
			return current, true, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return acceptedRequest{}, false, err
		}
	}
	previous.Request = storage.IngestRequest{Source: request.Source, CollectorID: request.CollectorID, BatchID: request.BatchID}
	if err := a.saveAccepted(ctx, key, previous); err != nil {
		return acceptedRequest{}, false, err
	}
	if request.IdempotencyKey != "" && previous.Result.BatchID == request.BatchID {
		// The alias digest needs the full normalized request, while the
		// terminal acceptance only keeps its identity and digest.
		alias := previous
		alias.Request = request
		if err := a.savePartitionIngestAlias(ctx, alias, key); err != nil {
			return acceptedRequest{}, false, err
		}
	}
	return previous, true, nil
}

func (a *Application) partitionSourceIngestStatus(ctx context.Context, tenant string, generation int64, source, collector, batch string) (acceptedRequest, error) {
	record, err := a.accepted(ctx, a.ingestKey(tenant, source, collector, batch, generation))
	if !errors.Is(err, storage.ErrNotFound) {
		return record, err
	}
	stored, err := a.Store.GetIngestBatch(ctx, tenant, source, collector, batch)
	if err != nil {
		return acceptedRequest{}, err
	}
	return sourceIngestReceipt(tenant, generation, stored), nil
}

func sourceIngestReceipt(tenant string, generation int64, stored storage.IngestBatchRecord) acceptedRequest {
	state := "committed"
	if stored.Result.Failed > 0 {
		state = "failed"
	}
	return acceptedRequest{Tenant: tenant, Generation: generation, Request: stored.Request, State: state,
		AcceptedAt: stored.StartedAt, FinishedAt: stored.FinishedAt, Result: &stored.Result}
}
