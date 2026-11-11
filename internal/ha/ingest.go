package ha

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
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
	if errors.Is(err, storage.ErrNotFound) && a.ShardID != "" {
		if managedKey := a.managedReceiptKey(key); managedKey != "" {
			return loadAcceptance(ctx, a.Store.Objects, managedKey)
		}
	}
	return record, err
}

func loadAcceptance(ctx context.Context, objects storage.ObjectStore, key string) (acceptedRequest, error) {
	var record acceptedRequest
	data, err := objects.Get(ctx, key)
	if err != nil {
		return record, err
	}
	err = json.Unmarshal(data, &record)
	return record, err
}
func (a *Application) saveAccepted(ctx context.Context, key string, record acceptedRequest) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	var pending pendingAcceptance
	if a.pending != nil && record.State == "accepted" {
		pending, err = pendingInfo(record, int64(len(data)))
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
	}
	if a.pending != nil {
		if previous, ok := a.pending[key]; ok {
			a.pendingBytes -= previous.objectBytes
		}
		delete(a.pending, key)
		if record.State == "accepted" {
			a.pending[key] = pending
			a.pendingBytes += pending.objectBytes
		}
	}
	return nil
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
	partitioned := cmd.Kind == "partition_accept"
	if partitioned {
		g, err := a.partitionStore().GetGraph(ctx, cmd.Tenant)
		if err != nil {
			return nil, err
		}
		if g.State != "active" || cmd.ExpectedGeneration != g.Generation {
			return resultJSON(http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "partitioned graph generation is not active"})
		}
		generation = g.Generation
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
	}
	record, err := a.accepted(ctx, key)
	if partitioned && errors.Is(err, storage.ErrNotFound) && request.IdempotencyKey != "" {
		previous, found, aliasErr := a.partitionIngestAlias(ctx, cmd.Tenant, generation, request)
		if aliasErr != nil {
			return nil, aliasErr
		}
		if found {
			if previous.Digest != partitionIngestDigest(request) {
				return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": "ingest idempotency key was used for a different request"})
			}
			record, err := a.accepted(ctx, previous.Key)
			if err != nil {
				return nil, err
			}
			if previous.Index != 0 && previous.Index != record.Index {
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
			if generation == sourceGeneration+1 {
				previous, found, replayErr := a.replayPartitionSourceIngest(ctx, candidate, sourceGeneration)
				if errors.Is(replayErr, storage.ErrIngestIdentityConflict) {
					return resultJSON(http.StatusConflict, map[string]any{"code": "idempotency_conflict", "error": replayErr.Error()})
				}
				if replayErr != nil {
					return nil, replayErr
				}
				if found {
					return a.acceptanceResponse(previous)
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
			return resultJSON(http.StatusConflict, map[string]any{"error": err.Error()})
		}
		record = candidate
		if err := a.saveAccepted(ctx, key, record); err != nil {
			return nil, err
		}
		if partitioned && request.IdempotencyKey != "" {
			if err := a.savePartitionIngestAlias(ctx, record, key); err != nil {
				return nil, err
			}
		}
	}
	return a.acceptanceResponse(record)
}

func (a *Application) acceptanceResponse(record acceptedRequest) ([]byte, error) {
	statusURL := "/v1/ingest/batches/" + url.PathEscape(record.Request.Source) + "/" + url.PathEscape(record.Request.CollectorID) + "/" + url.PathEscape(record.Request.BatchID)
	if record.ReceiptRetention > 0 {
		statusURL += "?accepted_lsn=" + strconv.FormatUint(record.Index, 10)
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
		entries = append(entries, storage.IngestBatchEntry{Request: record.Request, AcceptedAt: record.AcceptedAt, AcceptedGeneration: record.Generation})
	}
	if len(records) == 0 {
		return json.Marshal([]storage.IngestResult{})
	}
	if len(entries) > 0 {
		applied, err := a.Store.IngestDurableBatch(ctx, cmd.Tenant, entries)
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
		record.Request = storage.IngestRequest{Source: record.Request.Source, CollectorID: record.Request.CollectorID, BatchID: record.Request.BatchID}
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
	if err == nil && c.App.ShardID != "" {
		g, graphErr := c.App.graphDescriptor(r.Context(), r.Header.Get("X-Tenant-ID"))
		if graphErr == nil {
			generation = g.Generation
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
	if errors.Is(err, storage.ErrNotFound) && generation == sourceGeneration+1 {
		record, err = c.App.partitionSourceIngestStatus(r.Context(), r.Header.Get("X-Tenant-ID"), sourceGeneration, segments[0], segments[1], segments[2])
	}
	c.App.mu.RUnlock()
	if lsn := r.URL.Query().Get("accepted_lsn"); err == nil && lsn != "" && lsn != strconv.FormatUint(record.Index, 10) {
		err = storage.ErrNotFound
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
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := c.Node.ReadBarrier(ctx); err != nil {
			return err
		}
		c.App.mu.RLock()
		record, err := c.App.accepted(ctx, key)
		c.App.mu.RUnlock()
		if strings.Contains(acceptance.StatusURL, "?accepted_lsn=") && (errors.Is(err, storage.ErrNotFound) || err == nil && acceptance.Index != record.Index) {
			http.Error(w, "ingest receipt expired", http.StatusNotFound)
			return nil
		}
		if err != nil {
			return err
		}
		if record.Result != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Preference-Applied", "wait=committed")
			httpapi.WriteIngestResult(w, *record.Result)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Cluster) flushPending(ctx context.Context) (err error) {
	return c.flushPendingTenant(ctx, "")
}

func (c *Cluster) flushPendingTenant(ctx context.Context, tenant string) (err error) {
	finish := c.metrics.Start("ingest_flush_poll")
	defer func() { finish(err) }()
	queue, err := c.App.pendingSnapshot(ctx)
	if err != nil {
		return err
	}
	var requests []pendingFlushRequest
	for key, record := range queue {
		if record.partitioned {
			continue
		}
		if tenant != "" && record.tenant != tenant {
			continue
		}
		requests = append(requests, pendingFlushRequest{key, record})
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].record.index < requests[j].record.index })
	if len(requests) == 0 {
		return nil
	}

	var failures error
	for len(requests) > 0 {
		selected := requests[0].record.tenant
		var group, remaining []pendingFlushRequest
		for _, request := range requests {
			if request.record.tenant == selected {
				group = append(group, request)
			} else {
				remaining = append(remaining, request)
			}
		}
		failures = errors.Join(failures, c.flushPendingBatch(ctx, group))
		if ctx.Err() != nil {
			return errors.Join(failures, ctx.Err())
		}
		requests = remaining
	}
	return failures
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
	var size int64
	generation := requests[0].record.generation
	for _, pending := range requests {
		if pending.record.tenant != cmd.Tenant || pending.record.generation != generation {
			continue
		}
		if len(cmd.IDs) > 0 && (len(cmd.IDs) >= max(c.FlushMaxRequests, 1) || size+pending.record.requestBytes > c.FlushMaxBytes) {
			break
		}
		cmd.IDs = append(cmd.IDs, pending.key)
		size += pending.record.requestBytes
	}
	_, err = c.propose(ctx, cmd)
	return err
}

// A bounded queue snapshot drives each pass. Failed tenants retain their WAL
// and cannot prevent other tenants from publishing; new arrivals wake another pass.
func (c *Cluster) flushReady(ctx context.Context, all bool) error {
	queue, err := c.App.pendingSnapshot(ctx)
	if err != nil {
		return err
	}
	counts := make(map[string]int)
	bytes := make(map[string]int64)
	for _, r := range queue {
		counts[r.tenant]++
		bytes[r.tenant] += r.requestBytes
	}
	tenants := make([]string, 0, len(counts))
	for tenant := range counts {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	var failures error
	for _, tenant := range tenants {
		for counts[tenant] > 0 && (all || counts[tenant] >= max(c.FlushMaxRequests, 1) || bytes[tenant] >= max(c.FlushMaxBytes, 1)) {
			if err := c.flushPendingTenant(ctx, tenant); err != nil {
				failures = errors.Join(failures, err)
				break
			}
			next, err := c.App.pendingSnapshot(ctx)
			if err != nil {
				return errors.Join(failures, err)
			}
			count := 0
			var size int64
			for _, r := range next {
				if r.tenant == tenant {
					count++
					size += r.requestBytes
				}
			}
			if count >= counts[tenant] {
				break
			}
			counts[tenant], bytes[tenant] = count, size
		}
	}
	return failures
}
