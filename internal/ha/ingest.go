package ha

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type acceptedRequest struct {
	ReceiptRetention time.Duration         `json:"receipt_retention_ns,omitempty"`
	AcceptedEpoch    uint64                `json:"accepted_epoch,omitempty"`
	AliasDigest      string                `json:"alias_digest,omitempty"`
	Partitioned      bool                  `json:"partitioned,omitempty"`
	Tenant           string                `json:"tenant_id"`
	Request          storage.IngestRequest `json:"request"`
	Digest           string                `json:"request_digest,omitempty"`
	Generation       int64                 `json:"generation"`
	Index            uint64                `json:"accepted_lsn"`
	AcceptedAt       time.Time             `json:"accepted_at"`
	FinishedAt       time.Time             `json:"finished_at,omitempty"`
	State            string                `json:"state"`
	Result           *storage.IngestResult `json:"result,omitempty"`
	Error            string                `json:"last_error,omitempty"`
}

func (a *Application) ingestPrefix() string {
	return path.Join(a.Store.Prefix, "control", "replication-ingest") + "/"
}
func (a *Application) ingestKey(tenant, source, collector, batch string, generation int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", tenant, source, collector, batch, generation)))
	return a.ingestPrefix() + fmt.Sprintf("%x.json", sum)
}
func (a *Application) accepted(ctx context.Context, key string) (acceptedRequest, error) {
	record, err := loadAcceptance(ctx, a.Store.Objects, key)
	if errors.Is(err, storage.ErrNotFound) {
		if managedKey := a.managedReceiptKey(key); managedKey != "" {
			return loadAcceptance(ctx, a.Store.Objects, managedKey)
		}
	}
	return record, err
}

func (a *Application) loadAcceptedUnderRLock(ctx context.Context, key string) (acceptedRequest, error) {
	a.mu.RLock()
	data, err := a.Store.Objects.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		if managedKey := a.managedReceiptKey(key); managedKey != "" {
			data, err = a.Store.Objects.Get(ctx, managedKey)
		}
	}
	a.mu.RUnlock()
	if err != nil {
		return acceptedRequest{}, err
	}
	var record acceptedRequest
	if err := json.Unmarshal(data, &record); err != nil {
		return acceptedRequest{}, err
	}
	return record, nil
}

func loadAcceptance(ctx context.Context, objects storage.ObjectStore, key string) (acceptedRequest, error) {
	record, _, err := loadAcceptanceWithSize(ctx, objects, key)
	return record, err
}

func loadAcceptanceWithSize(ctx context.Context, objects storage.ObjectStore, key string) (acceptedRequest, int64, error) {
	var record acceptedRequest
	data, err := objects.Get(ctx, key)
	if err != nil {
		return record, 0, err
	}
	err = json.Unmarshal(data, &record)
	return record, int64(len(data)), err
}
func (a *Application) saveAccepted(ctx context.Context, key string, record acceptedRequest) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if a.ShardID != "" && a.Files != nil && a.pending == nil {
		if err := a.ensurePending(ctx); err != nil {
			return err
		}
	}
	var pending pendingAcceptance
	if a.pending != nil && record.State == "accepted" {
		pending, err = a.pendingInfo(record, int64(len(data)))
		if err != nil {
			return err
		}
	}
	objectKey := key
	if !record.receiptExpiresAt().IsZero() {
		objectKey = a.managedReceiptKey(key)
		if objectKey == "" {
			return fmt.Errorf("invalid managed partition receipt key")
		}
	}
	if err := a.Store.Objects.Put(ctx, objectKey, data); err != nil {
		return err
	}
	if objectKey != key {
		if err := a.Store.Objects.Delete(ctx, key); err != nil {
			return err
		}
		a.untrackTenantReceipt(key)
	}
	if objectKey == key || (record.AcceptedEpoch != 0 && !record.Partitioned) {
		a.trackTenantReceipt(record.Tenant, objectKey)
	} else {
		a.untrackTenantReceipt(objectKey)
	}
	a.updatePendingAcceptance(key, record.State == "accepted", pending)
	return nil
}

func (a *Application) updatePendingAcceptance(key string, isPending bool, pending pendingAcceptance) {
	if a.pending != nil {
		previous, wasPending := a.pending[key]
		if wasPending != isPending {
			a.pendingIndex.update(key, isPending)
		}
		if wasPending {
			a.pendingBytes -= previous.objectBytes
			if previous.alias != "" {
				keys := slices.DeleteFunc(a.pendingIngestAliases[previous.alias], func(candidate string) bool { return candidate == key })
				if len(keys) == 0 {
					delete(a.pendingIngestAliases, previous.alias)
				} else {
					a.pendingIngestAliases[previous.alias] = keys
				}
			}
		}
		delete(a.pending, key)
		if isPending {
			a.pending[key] = pending
			a.pendingBytes += pending.objectBytes
			if pending.alias != "" {
				a.pendingIngestAliases[pending.alias] = append(a.pendingIngestAliases[pending.alias], key)
			}
		}
	}
	if !isPending {
		a.notifyIngestChanged()
	}
}

func resultJSON(status int, value any) ([]byte, error) {
	if detail, ok := value.(map[string]any); ok && status >= 400 {
		if message, ok := detail["error"].(string); ok {
			detail["message"] = message
		}
		if _, ok := detail["code"]; !ok {
			detail["code"] = "bad_request"
		}
		if _, ok := detail["retryable"]; !ok {
			detail["retryable"] = false
		}
	}
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(httpResult{Status: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body})
}

func (a *Application) accept(ctx context.Context, index uint64, cmd command) ([]byte, error) {
	var request storage.IngestRequest
	if err := json.Unmarshal(cmd.Body, &request); err != nil {
		return nil, err
	}
	generation, err := a.Store.ReplicationTenantGeneration(ctx, cmd.Tenant)
	if err != nil {
		return nil, err
	}
	sourceGeneration := generation
	replaysSource := false
	partitioned := cmd.Kind == "partition_accept"
	if partitioned {
		g, err := a.partitionStore().GetGraph(ctx, cmd.Tenant)
		if errors.Is(err, storage.ErrNotFound) {
			return resultJSON(http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "partitioned graph generation is not active"})
		}
		if err != nil {
			return nil, err
		}
		if g.State != "active" || cmd.ExpectedGeneration != g.Generation {
			return resultJSON(http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "partitioned graph generation is not active"})
		}
		generation = g.Generation
		replaysSource = g.ConvertsSource(sourceGeneration)
	}
	key := a.ingestKey(cmd.Tenant, request.Source, request.CollectorID, request.BatchID, generation)
	after, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(after))
	candidate := acceptedRequest{Partitioned: partitioned, Tenant: cmd.Tenant, Request: request, Digest: digest, Generation: generation, Index: index, AcceptedAt: cmd.At, State: "accepted"}
	if partitioned {
		candidate.ReceiptRetention = cmd.PartitionReceiptRetention
	} else {
		candidate.ReceiptRetention = cmd.OrdinaryReceiptRetention
		if candidate.ReceiptRetention > 0 && a.ShardID != "" {
			candidate.AcceptedEpoch = cmd.RouteEpoch
		}
	}
	record, err := a.accepted(ctx, key)
	if !partitioned && cmd.CompactOrdinaryReceipts && errors.Is(err, storage.ErrNotFound) {
		record, err = a.ordinaryIngestReceipt(ctx, cmd.Tenant, generation, request.Source, request.CollectorID, request.BatchID)
	}
	if (partitioned || cmd.IndexIngestQueue || cmd.CompactOrdinaryReceipts) && errors.Is(err, storage.ErrNotFound) && request.IdempotencyKey != "" {
		previous, found, aliasErr := a.ingestAlias(ctx, cmd.Tenant, generation, request)
		if aliasErr == nil && !found && !partitioned {
			previous, found, aliasErr = a.pendingIngestAlias(ctx, cmd.Tenant, generation, request)
		}
		if aliasErr != nil {
			return nil, aliasErr
		}
		if found {
			if previous.Digest != ingestAliasDigest(request) {
				return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": "ingest idempotency key was used for a different request"})
			}
			record, err := a.accepted(ctx, previous.Key)
			if errors.Is(err, storage.ErrNotFound) {
				return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": "ingest alias target acceptance was reclaimed"})
			}
			if err != nil {
				return nil, err
			}
			if previous.Index != 0 && previous.Index != record.Index || previous.Epoch != record.AcceptedEpoch {
				return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": "ingest alias no longer matches its acceptance"})
			}
			return a.acceptanceResponse(record)
		}
	}
	if err == nil {
		previous := record.Digest
		if previous == "" {
			before, err := json.Marshal(record.Request)
			if err != nil {
				return nil, err
			}
			previous = fmt.Sprintf("%x", sha256.Sum256(before))
		}
		if previous != digest {
			return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": "ingest identity was used for a different request"})
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	} else {
		if partitioned && cmd.ReplaySourceIngest {
			if replaysSource {
				previous, found, replayErr := a.replayPartitionSourceIngest(ctx, candidate, sourceGeneration)
				if errors.Is(replayErr, storage.ErrIngestIdentityConflict) {
					return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": replayErr.Error()})
				}
				if replayErr != nil {
					return nil, replayErr
				}
				if found {
					resp, err := a.acceptanceResponse(previous)
					if err != nil || previous.Generation == candidate.Generation {
						return resp, err
					}
					var result httpResult
					if json.Unmarshal(resp, &result) == nil {
						if result.Header == nil {
							result.Header = make(http.Header)
						}
						result.Header.Set("X-GraphDB-Tenant-Generation", fmt.Sprint(candidate.Generation))
						return json.Marshal(result)
					}
					return resp, nil
				}
			}
			if request.FullSync {
				return resultJSON(http.StatusNotImplemented, map[string]any{"code": "operation_disabled", "error": "partitioned full_sync requires a reconciliation task"})
			}
			if _, err := (partition.IngestInput{Request: request, AcceptedAt: cmd.At}).Encode(); err != nil {
				return resultJSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
		}
		budget := a.MaxPendingBytes
		if cmd.QueueBudget != nil {
			budget = *cmd.QueueBudget
		}
		if budget > 0 {
			if err := a.ensurePending(ctx); err != nil {
				return nil, err
			}
			if a.pendingBytes+int64(len(cmd.Body)) > budget {
				return resultJSON(http.StatusServiceUnavailable, map[string]any{"code": "ingest_queue_full", "error": "replicated accepted queue exceeds its byte budget", "retryable": true})
			}
		}
		if err := a.Store.EnsureTenantWritable(ctx, cmd.Tenant); err != nil {
			if errors.Is(err, storage.ErrTenantDisabled) || errors.Is(err, storage.ErrTenantDeleted) || errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrConflict) {
				return resultJSON(http.StatusConflict, map[string]any{"error": err.Error()})
			}
			return nil, err
		}
		record = candidate
		claimAlias := (partitioned || candidate.ReceiptRetention > 0) && request.IdempotencyKey != ""
		if claimAlias && !partitioned {
			// An older tenant receipt may already own this alias.
			_, found, err := a.Store.LookupIngestReplay(ctx, cmd.Tenant, request)
			if errors.Is(err, storage.ErrIngestIdentityConflict) {
				return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": err.Error()})
			}
			if err != nil {
				return nil, err
			}
			claimAlias = !found
		}
		if claimAlias {
			if record.AcceptedEpoch != 0 {
				record.AliasDigest = ingestAliasDigest(request)
			}
			aliasKey := a.ingestAliasKey(record.Tenant, record.Generation, record.Request)
			if err := a.Files.JournalReplicatedObjects(ctx, []string{key, aliasKey}); err != nil {
				return nil, err
			}
		}
		if err := a.saveAccepted(ctx, key, record); err != nil {
			return nil, err
		}
		if claimAlias {
			if err := a.saveIngestAlias(ctx, record, key); err != nil {
				return nil, err
			}
		}
	}
	return a.acceptanceResponse(record)
}

func (a *Application) acceptanceResponse(record acceptedRequest) ([]byte, error) {
	statusURL := "/v1/ingest/batches/" + url.PathEscape(record.Request.Source) + "/" + url.PathEscape(record.Request.CollectorID) + "/" + url.PathEscape(record.Request.BatchID)
	if record.ReceiptRetention > 0 {
		query := url.Values{"accepted_lsn": {strconv.FormatUint(record.Index, 10)}}
		if record.AcceptedEpoch != 0 {
			query.Set("accepted_epoch", strconv.FormatUint(record.AcceptedEpoch, 10))
		}
		statusURL += "?" + query.Encode()
	}
	body, _ := json.Marshal(map[string]any{"tenant_id": record.Tenant, "writer_id": a.Store.InstanceID, "batch_id": record.Request.BatchID, "source": record.Request.Source, "collector_id": record.Request.CollectorID, "generation": record.Generation, "state": record.State, "durability": "raft_majority", "accepted_lsn": record.Index, "accepted_at": record.AcceptedAt, "estimated_flush_at": a.estimatedFlushAt(record), "status_url": statusURL})
	return json.Marshal(httpResult{Status: http.StatusAccepted, Header: http.Header{"Content-Type": []string{"application/json"}, "Location": []string{statusURL}, "X-GraphDB-Tenant-Generation": []string{fmt.Sprint(record.Generation)}}, Body: body})
}

func (a *Application) estimatedFlushAt(record acceptedRequest) time.Time {
	if record.Partitioned {
		return record.AcceptedAt
	}
	return record.AcceptedAt.Add(a.FlushInterval)
}

func (a *Application) flush(ctx context.Context, cmd command) ([]byte, error) {
	if cmd.CompactOrdinaryReceipts {
		if err := a.ensurePending(ctx); err != nil {
			return nil, err
		}
	}
	if a.ShardID != "" {
		owner, err := a.ownership(ctx, cmd.Tenant)
		if err != nil {
			return nil, err
		}
		if owner.State != "active" || (cmd.RouteEpoch > 0 && owner.Epoch != cmd.RouteEpoch) {
			return resultJSON(http.StatusConflict, map[string]any{"code": "shard_epoch_changed", "error": "tenant ownership changed before WAL publication", "retryable": true})
		}
	}
	var records []acceptedRequest
	var keys []string
	var entries []storage.IngestBatchEntry
	var entryIndexes []int
	var results []storage.IngestResult
	for _, key := range cmd.IDs {
		if !strings.HasPrefix(key, a.ingestPrefix()) {
			return nil, fmt.Errorf("invalid replicated acceptance key")
		}
		record, err := a.accepted(ctx, key)
		if _, pending := a.pending[key]; (cmd.CompactOrdinaryReceipts || cmd.IndexIngestQueue) && !pending && errors.Is(err, storage.ErrNotFound) {
			// The queue can drain or reclaim an old generation before this flush applies.
			continue
		}
		if err != nil {
			return nil, err
		}
		if record.Partitioned {
			return resultJSON(http.StatusConflict, map[string]any{"code": "graph_mode_changed", "error": "partitioned ingest requires a distributed transaction"})
		}
		if record.Tenant != cmd.Tenant {
			return nil, fmt.Errorf("replicated flush contains another tenant")
		}
		if record.State != "accepted" {
			continue
		}
		records = append(records, record)
		keys = append(keys, key)
		results = append(results, storage.IngestResult{})
		if cmd.TupleIdentity {
			if err := storage.ValidateIngestStorageIdentifiers(record.Request); err != nil {
				results[len(results)-1] = storage.IngestResult{BatchID: record.Request.BatchID, Failed: max(1, len(record.Request.Items)), Failures: []storage.IngestFailure{{Error: err.Error()}}}
				continue
			}
		}
		entryIndexes = append(entryIndexes, len(records)-1)
		entry := storage.IngestBatchEntry{Request: record.Request, AcceptedAt: record.AcceptedAt, AcceptedGeneration: record.Generation}
		if (cmd.CompactOrdinaryReceipts || record.AcceptedEpoch != 0) && record.Digest != "" {
			entry.ReplicatedReceipt = &storage.ReplicatedIngestReceipt{
				Generation: record.Generation, Index: record.Index, Epoch: record.AcceptedEpoch, Digest: record.Digest,
			}
		}
		entries = append(entries, entry)
	}
	if len(records) == 0 {
		return json.Marshal([]storage.IngestResult{})
	}
	var retire []bool
	if len(entries) > 0 {
		var hooks storage.IngestBatchHooks
		if cmd.CompactOrdinaryReceipts {
			retire = make([]bool, len(records))
			hooks.ReceiptStored = func(index int) { retire[entryIndexes[index]] = true }
		}
		applied, err := a.Store.IngestDurableBatchWithHooks(ctx, cmd.Tenant, entries, hooks)
		if err != nil {
			if !errors.Is(err, storage.ErrTenantDeleted) && !errors.Is(err, storage.ErrTenantDisabled) {
				return nil, err
			}
			applied = make([]storage.IngestResult, len(entries))
			for i, entry := range entries {
				applied[i] = storage.IngestResult{BatchID: entry.Request.BatchID, Failed: len(entry.Request.Items), Failures: []storage.IngestFailure{{Error: err.Error()}}}
			}
		}
		if len(applied) != len(entries) {
			return nil, fmt.Errorf("replicated flush result count mismatch")
		}
		for i, result := range applied {
			results[entryIndexes[i]] = result
		}
	}
	for i, record := range records {
		if retire != nil && retire[i] && record.ReceiptRetention <= 0 {
			if err := a.Store.Objects.Delete(ctx, keys[i]); err != nil {
				return nil, err
			}
			a.untrackTenantReceipt(keys[i])
			a.updatePendingAcceptance(keys[i], false, pendingAcceptance{})
			continue
		}
		record.State = "committed"
		if results[i].Failed > 0 {
			record.State = "failed"
		}
		record.Result = &results[i]
		record.FinishedAt = cmd.At
		if record.Digest == "" {
			request, err := json.Marshal(record.Request)
			if err != nil {
				return nil, err
			}
			record.Digest = fmt.Sprintf("%x", sha256.Sum256(request))
		}
		request := storage.IngestRequest{Source: record.Request.Source, CollectorID: record.Request.CollectorID, BatchID: record.Request.BatchID}
		if record.ReceiptRetention > 0 {
			request.IdempotencyKey = record.Request.IdempotencyKey
		}
		record.Request = request
		if err := a.saveAccepted(ctx, keys[i], record); err != nil {
			return nil, err
		}
	}
	return json.Marshal(results)
}

func (c *Cluster) batchStatus(w http.ResponseWriter, r *http.Request) {
	statusPath := r.URL.EscapedPath()
	if strings.HasPrefix(statusPath, "/v1/ingest/writers/") {
		parts := strings.Split(strings.TrimPrefix(statusPath, "/v1/ingest/writers/"), "/")
		if len(parts) != 5 || parts[1] != "batches" || parts[0] != url.PathEscape(c.App.Store.InstanceID) {
			http.Error(w, "unknown ingest writer", http.StatusNotFound)
			return
		}
		statusPath = "/v1/ingest/batches/" + strings.Join(parts[2:], "/")
	}
	segments := strings.Split(strings.TrimPrefix(statusPath, "/v1/ingest/batches/"), "/")
	if len(segments) != 3 {
		http.Error(w, "invalid ingest status path", http.StatusBadRequest)
		return
	}
	for i := range segments {
		decoded, err := url.PathUnescape(segments[i])
		if err != nil {
			http.Error(w, "invalid ingest status path", http.StatusBadRequest)
			return
		}
		segments[i] = decoded
	}
	generation, err := c.App.Store.ReplicationTenantGeneration(r.Context(), r.Header.Get("X-Tenant-ID"))
	sourceGeneration := generation
	replaysSource := false
	if err == nil && c.App.ShardID != "" {
		g, graphErr := c.App.graphDescriptor(r.Context(), r.Header.Get("X-Tenant-ID"))
		if graphErr == nil {
			generation = g.Generation
			replaysSource = g.ConvertsSource(sourceGeneration)
		} else if !errors.Is(graphErr, storage.ErrNotFound) {
			err = graphErr
		}
	}
	if err != nil {
		c.writeError(w, err)
		return
	}
	key := c.App.ingestKey(r.Header.Get("X-Tenant-ID"), segments[0], segments[1], segments[2], generation)
	c.App.mu.RLock()
	record, err := c.App.accepted(r.Context(), key)
	if errors.Is(err, storage.ErrNotFound) {
		switch {
		case replaysSource:
			record, err = c.App.partitionSourceIngestStatus(r.Context(), r.Header.Get("X-Tenant-ID"), sourceGeneration, segments[0], segments[1], segments[2])
		case c.App.ShardID == "" && c.config.Protocol >= ingestQueueCheckpointProtocol:
			record, err = c.App.ordinaryIngestReceipt(r.Context(), r.Header.Get("X-Tenant-ID"), generation, segments[0], segments[1], segments[2])
		}
	}
	c.App.mu.RUnlock()
	if err == nil {
		query := r.URL.Query()
		lsn, epoch := query.Get("accepted_lsn"), query.Get("accepted_epoch")
		if (lsn != "" && lsn != strconv.FormatUint(record.Index, 10)) ||
			(epoch != "" && (lsn == "" || epoch != strconv.FormatUint(record.AcceptedEpoch, 10))) ||
			(lsn != "" && record.AcceptedEpoch != 0 && epoch == "") {
			err = storage.ErrNotFound
		}
	}
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			c.writeError(w, err)
			return
		}
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	status := storage.IngestBatchStatus{TenantID: record.Tenant, WriterID: c.App.Store.InstanceID, Source: record.Request.Source, CollectorID: record.Request.CollectorID, BatchID: record.Request.BatchID, State: record.State, Durability: "raft_majority", AcceptedLSN: record.Index, AcceptedAt: record.AcceptedAt, EstimatedFlush: c.App.estimatedFlushAt(record), FinishedAt: record.FinishedAt, Result: record.Result, LastError: record.Error}
	if expiresAt := record.receiptExpiresAt(); !expiresAt.IsZero() {
		status.ReceiptExpiresAt = &expiresAt
	}
	json.NewEncoder(w).Encode(status)
}

func (c *Cluster) waitCommitted(ctx context.Context, w http.ResponseWriter, data []byte) error {
	var response httpResult
	if err := json.Unmarshal(data, &response); err != nil {
		return err
	}
	if response.Status != http.StatusAccepted {
		writeResult(w, data)
		return nil
	}
	var acceptance struct {
		Tenant     string `json:"tenant_id"`
		Source     string `json:"source"`
		Collector  string `json:"collector_id"`
		Batch      string `json:"batch_id"`
		Generation int64  `json:"generation"`
		Index      uint64 `json:"accepted_lsn"`
		StatusURL  string `json:"status_url"`
	}
	if err := json.Unmarshal(response.Body, &acceptance); err != nil {
		return err
	}
	key := c.App.ingestKey(acceptance.Tenant, acceptance.Source, acceptance.Collector, acceptance.Batch, acceptance.Generation)
	statusURL, err := url.Parse(acceptance.StatusURL)
	if err != nil {
		return err
	}
	query := statusURL.Query()
	managed := query.Has("accepted_lsn")
	acceptedEpoch := query.Get("accepted_epoch")
	stillPendingInMemory := func() bool {
		c.App.mu.RLock()
		defer c.App.mu.RUnlock()
		if c.App.pending == nil {
			return false
		}
		p, ok := c.App.pending[key]
		return ok && (!managed || p.index == acceptance.Index)
	}
	load := func() (acceptedRequest, error) {
		record, err := c.App.loadAcceptedUnderRLock(ctx, key)
		if err != nil {
			if c.App.ShardID == "" && c.config.Protocol >= ingestQueueCheckpointProtocol && errors.Is(err, storage.ErrNotFound) {
				c.App.mu.RLock()
				fallbackRecord, fallbackErr := c.App.ordinaryIngestReceipt(ctx, acceptance.Tenant, acceptance.Generation, acceptance.Source, acceptance.Collector, acceptance.Batch)
				c.App.mu.RUnlock()
				if fallbackErr != nil {
					return acceptedRequest{}, fallbackErr
				}
				if fallbackRecord.Index != acceptance.Index {
					return acceptedRequest{}, storage.ErrNotFound
				}
				return fallbackRecord, nil
			}
			return acceptedRequest{}, err
		}
		return record, nil
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	checkBarrier := false
	validatedInitial := false
	for {
		if checkBarrier {
			if err := c.Node.ReadBarrier(ctx); err != nil {
				return err
			}
			checkBarrier = false
		}
		waitCh := c.App.ingestWaitCh()
		// Avoid O(N^2) full-payload disk reads and JSON unmarshals across all
		// concurrent waiters when an unrelated batch completes: if our key is
		// still in the in-memory pending index, wait for the next signal.
		if validatedInitial && stillPendingInMemory() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-waitCh:
				continue
			case <-ticker.C:
				checkBarrier = true
				continue
			}
		}
		record, err := load()
		if managed && (errors.Is(err, storage.ErrNotFound) || err == nil &&
			(acceptance.Index != record.Index || acceptedEpoch != "" && acceptedEpoch != strconv.FormatUint(record.AcceptedEpoch, 10))) {
			http.Error(w, "ingest receipt expired", http.StatusNotFound)
			return nil
		}
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				if barrierErr := c.Node.ReadBarrier(ctx); barrierErr != nil {
					return barrierErr
				}
				record, err = load()
			}
			if err != nil {
				return err
			}
		}
		validatedInitial = true
		if record.Result != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Preference-Applied", "wait=committed")
			httpapi.WriteIngestResult(w, *record.Result)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCh:
		case <-ticker.C:
			checkBarrier = true
		}
	}
}

func (c *Cluster) flushPending(ctx context.Context) (err error) {
	return c.flushPendingTenant(ctx, "")
}

// An explicit drain publishes one bounded batch per selected tenant.
func (c *Cluster) flushPendingTenant(ctx context.Context, tenant string) error {
	return c.flushQueues(ctx, tenant, true, true)
}

type pendingFlushRequest struct {
	key    string
	record pendingAcceptance
}

func (c *Cluster) flushPendingBatch(ctx context.Context, requests []pendingFlushRequest) error {
	cmd, err := newCommand("flush")
	if err != nil {
		return err
	}
	cmd.Tenant = requests[0].record.tenant
	c.App.mu.RLock()
	if c.App.ShardID != "" {
		owner, ownerErr := c.App.ownership(ctx, cmd.Tenant)
		if ownerErr != nil || owner.State != "active" {
			c.App.mu.RUnlock()
			if ownerErr == nil {
				return storage.ErrConflict
			}
			return ownerErr
		}
		cmd.RouteEpoch = owner.Epoch
	}
	err = c.App.Store.CheckAcceptedWALBackpressure(ctx, cmd.Tenant)
	c.App.mu.RUnlock()
	if err != nil {
		return err
	}
	for _, request := range requests {
		cmd.IDs = append(cmd.IDs, request.key)
	}
	_, err = c.propose(ctx, cmd)
	return err
}

// A bounded queue snapshot drives each pass. Failed tenants retain their WAL
// and cannot prevent other tenants from publishing; new arrivals wake another pass.
func (c *Cluster) flushReady(ctx context.Context, all bool) error {
	return c.flushQueues(ctx, "", all, false)
}

func (c *Cluster) flushQueues(ctx context.Context, tenant string, all, oneBatch bool) (err error) {
	finish := c.metrics.Start("ingest_flush_poll")
	defer func() { finish(err) }()
	groups, err := c.App.pendingFlushGroups(ctx, tenant)
	if err != nil || len(groups) == 0 {
		return err
	}
	var failures error
	for _, tenant := range slices.Sorted(maps.Keys(groups)) {
		requests := groups[tenant]
		sort.Slice(requests, func(i, j int) bool { return requests[i].record.index < requests[j].record.index })
		var bytes int64
		for _, r := range requests {
			bytes += r.record.requestBytes
		}
		limit := max(c.FlushMaxRequests, 1)
		for len(requests) > 0 && (all || len(requests) >= limit || bytes >= max(c.FlushMaxBytes, 1)) {
			count, size := 1, requests[0].record.requestBytes
			aliases := map[string]bool{requests[0].record.alias: true}
			for count < len(requests) && count < limit {
				next := requests[count].record
				// Legacy acceptances can share an alias. Publish its first
				// result before resolving another acceptance against it.
				if next.generation != requests[0].record.generation || size+next.requestBytes > c.FlushMaxBytes ||
					next.alias != "" && aliases[next.alias] {
					break
				}
				aliases[next.alias] = true
				count++
				size += next.requestBytes
			}
			if err := c.flushPendingBatch(ctx, requests[:count]); err != nil {
				failures = errors.Join(failures, err)
				break
			}
			if oneBatch || len(requests) == count {
				break
			}
			// A fenced proposal can return a business response without removing a receipt.
			// Do not bypass its batch until the application has published it.
			c.App.mu.RLock()
			_, stillPending := c.App.pending[requests[0].key]
			ready := c.App.pending != nil && !stillPending
			c.App.mu.RUnlock()
			if !ready {
				break
			}
			requests = requests[count:]
			bytes -= size
		}
		if ctx.Err() != nil {
			return errors.Join(failures, ctx.Err())
		}
	}
	return failures
}
