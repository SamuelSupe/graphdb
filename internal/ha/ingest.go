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
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type acceptedRequest struct {
	Tenant     string                `json:"tenant_id"`
	Request    storage.IngestRequest `json:"request"`
	Digest     string                `json:"request_digest,omitempty"`
	Generation int64                 `json:"generation"`
	Index      uint64                `json:"accepted_lsn"`
	AcceptedAt time.Time             `json:"accepted_at"`
	FinishedAt time.Time             `json:"finished_at,omitempty"`
	State      string                `json:"state"`
	Result     *storage.IngestResult `json:"result,omitempty"`
	Error      string                `json:"last_error,omitempty"`
}

func (a *Application) ingestPrefix() string {
	return path.Join(a.Store.Prefix, "control", "replication-ingest") + "/"
}
func (a *Application) ingestKey(tenant, source, collector, batch string, generation int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", tenant, source, collector, batch, generation)))
	return a.ingestPrefix() + fmt.Sprintf("%x.json", sum)
}
func (a *Application) accepted(ctx context.Context, key string) (acceptedRequest, error) {
	var record acceptedRequest
	data, err := a.Store.Objects.Get(ctx, key)
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
	if err := a.Store.Objects.Put(ctx, key, data); err != nil {
		return err
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
	key := a.ingestKey(cmd.Tenant, request.Source, request.CollectorID, request.BatchID, generation)
	after, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(after))
	record, err := a.accepted(ctx, key)
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
		record = acceptedRequest{Tenant: cmd.Tenant, Request: request, Digest: digest, Generation: generation, Index: index, AcceptedAt: cmd.At, State: "accepted"}
		if err := a.saveAccepted(ctx, key, record); err != nil {
			return nil, err
		}
	}
	statusURL := "/v1/ingest/batches/" + url.PathEscape(record.Request.Source) + "/" + url.PathEscape(record.Request.CollectorID) + "/" + url.PathEscape(record.Request.BatchID)
	body, _ := json.Marshal(map[string]any{"tenant_id": record.Tenant, "writer_id": a.Store.InstanceID, "batch_id": record.Request.BatchID, "source": record.Request.Source, "collector_id": record.Request.CollectorID, "generation": record.Generation, "state": record.State, "durability": "raft_majority", "accepted_lsn": record.Index, "accepted_at": record.AcceptedAt, "estimated_flush_at": record.AcceptedAt.Add(a.FlushInterval), "status_url": statusURL})
	return json.Marshal(httpResult{Status: http.StatusAccepted, Header: http.Header{"Content-Type": []string{"application/json"}, "Location": []string{statusURL}, "X-GraphDB-Tenant-Generation": []string{fmt.Sprint(generation)}}, Body: body})
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
	for _, key := range cmd.IDs {
		if !strings.HasPrefix(key, a.ingestPrefix()) {
			return nil, fmt.Errorf("invalid replicated acceptance key")
		}
		record, err := a.accepted(ctx, key)
		if err != nil {
			return nil, err
		}
		if record.Tenant != cmd.Tenant {
			return nil, fmt.Errorf("replicated flush contains another tenant")
		}
		if record.State != "accepted" {
			continue
		}
		records = append(records, record)
		keys = append(keys, key)
		entries = append(entries, storage.IngestBatchEntry{Request: record.Request, AcceptedAt: record.AcceptedAt, AcceptedGeneration: record.Generation})
	}
	if len(entries) == 0 {
		return json.Marshal([]storage.IngestResult{})
	}
	results, err := a.Store.IngestDurableBatch(ctx, cmd.Tenant, entries)
	if err != nil {
		if !errors.Is(err, storage.ErrTenantDeleted) && !errors.Is(err, storage.ErrTenantDisabled) {
			return nil, err
		}
		results = make([]storage.IngestResult, len(records))
		for i, record := range records {
			results[i] = storage.IngestResult{BatchID: record.Request.BatchID, Failed: len(record.Request.Items), Failures: []storage.IngestFailure{{Error: err.Error()}}}
		}
	}
	if len(results) != len(records) {
		return nil, fmt.Errorf("replicated flush result count mismatch")
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
	if err != nil {
		c.writeError(w, err)
		return
	}
	key := c.App.ingestKey(r.Header.Get("X-Tenant-ID"), segments[0], segments[1], segments[2], generation)
	record, err := c.App.accepted(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(storage.IngestBatchStatus{TenantID: record.Tenant, WriterID: c.App.Store.InstanceID, Source: record.Request.Source, CollectorID: record.Request.CollectorID, BatchID: record.Request.BatchID, State: record.State, Durability: "raft_majority", AcceptedLSN: record.Index, AcceptedAt: record.AcceptedAt, EstimatedFlush: record.AcceptedAt.Add(c.App.FlushInterval), FinishedAt: record.FinishedAt, Result: record.Result, LastError: record.Error})
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
		if err != nil {
			return err
		}
		if record.Result != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Preference-Applied", "wait=committed")
			status := http.StatusOK
			if record.Result.Failed > 0 {
				status = http.StatusMultiStatus
			}
			w.WriteHeader(status)
			return json.NewEncoder(w).Encode(record.Result)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Cluster) flushPending(ctx context.Context) error {
	queue, err := c.App.pendingSnapshot(ctx)
	if err != nil {
		return err
	}
	type pending struct {
		key    string
		record pendingAcceptance
	}
	var requests []pending
	for key, record := range queue {
		requests = append(requests, pending{key, record})
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].record.index < requests[j].record.index })
	if len(requests) == 0 {
		return nil
	}
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
