package ha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type partitionIngestAlias struct {
	Key    string `json:"key"`
	Digest string `json:"digest"`
	Index  uint64 `json:"accepted_lsn,omitempty"`
}

func partitionIngestIdentity(key string) string {
	return fmt.Sprintf("ingest:%x", sha256.Sum256([]byte(key)))
}

func partitionIngestTxnIdentity(key string, index uint64, indexed bool) string {
	identity := partitionIngestIdentity(key)
	if indexed {
		return identity + ":" + strconv.FormatUint(index, 10)
	}
	return identity
}

func partitionIngestReference(key string) string {
	return strings.TrimSuffix(path.Base(key), ".json")
}

func (a *Application) readPartitionIngestInput(ctx context.Context, objects storage.ObjectStore, tenant string, generation int64, ref string) ([]byte, error) {
	key := a.ingestPrefix() + ref + ".json"
	record, err := loadAcceptance(ctx, objects, key)
	if err != nil {
		return nil, err
	}
	if !record.Partitioned || record.Tenant != tenant || record.Generation != generation || record.State != "accepted" ||
		key != a.ingestKey(tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, generation) {
		return nil, fmt.Errorf("invalid partition ingest input scope or state")
	}
	return (partition.IngestInput{Request: record.Request, AcceptedAt: record.AcceptedAt}).Encode()
}

// An idempotency key may be retried with a new batch ID. Bind it to the first
// acceptance, keeping aliases outside the pending-record scan.
func partitionIngestDigest(request storage.IngestRequest) string {
	request.BatchID = ""
	data, _ := json.Marshal(request)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func (a *Application) partitionIngestAliasKey(tenant string, generation int64, request storage.IngestRequest) string {
	key := a.ingestKey(tenant, request.Source, request.CollectorID, request.IdempotencyKey, generation)
	return path.Join(a.Store.Prefix, "control", "partition-ingest-identities", path.Base(key))
}

func (a *Application) partitionIngestAlias(ctx context.Context, tenant string, generation int64, request storage.IngestRequest) (partitionIngestAlias, bool, error) {
	data, err := a.Store.Objects.Get(ctx, a.partitionIngestAliasKey(tenant, generation, request))
	if errors.Is(err, storage.ErrNotFound) {
		return partitionIngestAlias{}, false, nil
	}
	var alias partitionIngestAlias
	if err == nil {
		err = json.Unmarshal(data, &alias)
	}
	return alias, err == nil, err
}

func (a *Application) savePartitionIngestAlias(ctx context.Context, record acceptedRequest, key string) error {
	alias := partitionIngestAlias{Key: key, Digest: partitionIngestDigest(record.Request)}
	if record.ReceiptRetention > 0 {
		alias.Index = record.Index
	}
	data, err := json.Marshal(alias)
	if err != nil {
		return err
	}
	return a.Store.Objects.Put(ctx, a.partitionIngestAliasKey(record.Tenant, record.Generation, record.Request), data)
}

func (c *Cluster) servePartitionIngest(w http.ResponseWriter, r *http.Request, body []byte, g partition.Graph) bool {
	status := r.Method == http.MethodGet && (strings.HasPrefix(r.URL.Path, "/v1/ingest/batches/") || strings.HasPrefix(r.URL.Path, "/v1/ingest/writers/"))
	if !status && (r.Method != http.MethodPost || r.URL.Path != "/v1/ingest/batches") {
		return false
	}
	if raw := r.Header.Get("X-GraphDB-Read-Generation"); raw != "" {
		generation, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || generation != g.Generation {
			data, _ := resultJSON(http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "partitioned graph generation changed"})
			writeResult(w, data)
			return true
		}
	}
	w.Header().Set("X-GraphDB-Tenant-Generation", fmt.Sprint(g.Generation))
	if status {
		c.batchStatus(w, r)
		return true
	}
	if c.config.Protocol < partition.IngestProtocol {
		data, _ := resultJSON(http.StatusNotImplemented, map[string]any{"code": "operation_disabled", "error": fmt.Sprintf("partitioned ingest requires home Raft protocol %d", partition.IngestProtocol)})
		writeResult(w, data)
		return true
	}
	var request storage.IngestRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&request)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = fmt.Errorf("a single JSON value is required")
		}
	}
	if err == nil {
		request, err = storage.PrepareIngestRequest(g.Tenant, request)
	}
	if err != nil {
		data, _ := resultJSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		writeResult(w, data)
		return true
	}
	cmd, err := newCommand("partition_accept")
	if err != nil {
		c.partitionError(w, err)
		return true
	}
	if err := c.checkAdmissionDiskSpace(r.Context(), int64(len(body))*4); err != nil {
		c.writeError(w, err)
		return true
	}
	cmd.Tenant, cmd.ExpectedGeneration = g.Tenant, g.Generation
	cmd.ReplaySourceIngest = true
	cmd.Method, cmd.URI = r.Method, r.URL.RequestURI()
	cmd.Header = http.Header{"X-Tenant-ID": []string{g.Tenant}}
	cmd.RouteEpoch, _ = strconv.ParseUint(r.Header.Get(sharding.EpochHeader), 10, 64)
	cmd.Body, err = json.Marshal(request)
	var data []byte
	if err == nil {
		data, err = c.propose(r.Context(), cmd)
	}
	if err != nil {
		c.writeError(w, err)
		return true
	}
	if !c.WAL || strings.Contains(strings.ToLower(r.Header.Get("Prefer")), "wait=committed") {
		if err := c.waitCommitted(r.Context(), w, data); err != nil {
			c.writeError(w, err)
		}
	} else {
		writeResult(w, data)
	}
	return true
}

func (a *Application) firstPartitionIngest(ctx context.Context, tenant string, generation int64) (string, error) {
	return a.partitionIngestAfter(ctx, tenant, generation, 0)
}

func (a *Application) partitionIngestAfter(ctx context.Context, tenant string, generation int64, after uint64) (string, error) {
	if err := a.ensurePending(ctx); err != nil {
		return "", err
	}
	key := ""
	var first uint64
	for candidate, pending := range a.pending {
		if pending.partitioned && pending.tenant == tenant && pending.generation == generation &&
			(after == 0 || pending.index > after) && (key == "" || pending.index < first) {
			key, first = candidate, pending.index
		}
	}
	return key, nil
}

func (a *Application) validatePartitionIngestBegin(ctx context.Context, action partition.Action) error {
	key, err := a.firstPartitionIngest(ctx, action.Tenant, action.Generation)
	if err != nil {
		return err
	}
	pending := a.pending[key]
	if key == "" || partitionIngestTxnIdentity(key, pending.index, pending.indexedTxn) != action.Identity {
		return fmt.Errorf("%w: ingest must execute in durable acceptance order", storage.ErrConflict)
	}
	if len(action.Inputs) == 1 && action.Inputs[0].Acceptance != "" {
		if action.Inputs[0].Acceptance != partitionIngestReference(key) {
			return fmt.Errorf("%w: ingest reference differs from the durable queue head", storage.ErrConflict)
		}
		return nil // Store.Input validates the referenced payload against the digest.
	}
	record, err := a.accepted(ctx, key)
	if err != nil {
		return err
	}
	data, err := (partition.IngestInput{Request: record.Request, AcceptedAt: record.AcceptedAt}).Encode()
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != action.Digest {
		return fmt.Errorf("%w: ingest input differs from its durable acceptance", storage.ErrConflict)
	}
	return nil
}

// Only this replicated transition may remove an acceptance from the queue. A
// coordinator's response is insufficient evidence of publication after a crash.
func (a *Application) completePartitionIngest(ctx context.Context, cmd command) ([]byte, error) {
	if len(cmd.IDs) != 1 || !strings.HasPrefix(cmd.IDs[0], a.ingestPrefix()) || a.ShardID == "" || a.Catalog {
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid partitioned acceptance identity"})
	}
	record, err := a.accepted(ctx, cmd.IDs[0])
	if errors.Is(err, storage.ErrNotFound) && cmd.PartitionAcceptedIndex != 0 {
		return resultJSON(http.StatusConflict, map[string]any{"error": "ingest acceptance was reclaimed"})
	}
	if err != nil {
		return nil, err
	}
	if !record.Partitioned || record.Tenant != cmd.Tenant || record.Generation != cmd.ExpectedGeneration ||
		((record.ReceiptRetention > 0 || cmd.PartitionAcceptedIndex != 0) && cmd.PartitionAcceptedIndex != record.Index) {
		return resultJSON(http.StatusConflict, map[string]any{"error": "partitioned acceptance scope changed"})
	}
	if record.State != "accepted" {
		return resultJSON(http.StatusOK, record.Result)
	}
	txn, err := a.partitionStore().Transaction(ctx, record.Tenant, record.Generation, partitionIngestTxnIdentity(cmd.IDs[0], record.Index, record.ReceiptRetention > 0))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return resultJSON(http.StatusConflict, map[string]any{"error": "ingest transaction is not decided"})
		}
		return nil, err
	}
	data, err := (partition.IngestInput{Request: record.Request, AcceptedAt: record.AcceptedAt}).Encode()
	if err != nil {
		return nil, err
	}
	if txn.Purpose != "ingest" || (txn.State != "committed" && txn.State != "aborted") || txn.IngestResult == nil ||
		len(txn.Inputs) != 0 || len(txn.Participants) != 0 ||
		txn.IngestResult.BatchID != record.Request.BatchID || txn.Digest != fmt.Sprintf("%x", sha256.Sum256(data)) {
		return resultJSON(http.StatusConflict, map[string]any{"error": "ingest transaction has no matching durable result"})
	}
	if err := a.savePartitionIngestResult(ctx, cmd.IDs[0], record, txn, cmd.At, cmd.CompactPartitionIngest); err != nil {
		return nil, err
	}
	return resultJSON(http.StatusOK, txn.IngestResult)
}

func (a *Application) completeGraphIngestReceipt(ctx context.Context, ref string, txn partition.Transaction, at time.Time) error {
	key := a.ingestPrefix() + ref + ".json"
	record, err := a.accepted(ctx, key)
	if err != nil {
		return err
	}
	if !record.Partitioned || record.Tenant != txn.Tenant || record.Generation != txn.Generation || record.State != "accepted" ||
		txn.Identity != partitionIngestTxnIdentity(key, record.Index, record.ReceiptRetention > 0) ||
		record.Request.BatchID != txn.IngestResult.BatchID {
		return fmt.Errorf("graph completion does not match the durable ingest acceptance")
	}
	return a.savePartitionIngestResult(ctx, key, record, txn, at, true)
}

func (a *Application) beginNextPartitionIngest(ctx context.Context, complete partition.Action, at time.Time) error {
	next := complete.NextIngest
	begin := partition.Action{
		Operation: "begin", Purpose: "ingest", Tenant: complete.Tenant, Generation: complete.Generation,
		Txn: next.Txn, Identity: next.Identity, Digest: next.Input.Digest, Inputs: []partition.Input{next.Input},
		At: at, BaseVersion: next.BaseVersion, LayoutRevision: next.LayoutRevision, Participants: next.Participants,
		Revision: partition.BatchGCRevision,
	}
	err := a.validatePartitionIngestBegin(ctx, begin)
	if err == nil {
		_, err = a.partitionStore().Apply(ctx, begin)
	}
	// A stale successor is optional. Both begin validations report conflicts
	// before writing. Storage failures still abort the entire journal.
	if errors.Is(err, storage.ErrConflict) || errors.Is(err, storage.ErrInvalidArgument) {
		return nil
	}
	return err
}

func (a *Application) savePartitionIngestResult(ctx context.Context, key string, record acceptedRequest, txn partition.Transaction, at time.Time, compact bool) error {
	record.State, record.Result, record.FinishedAt = "committed", txn.IngestResult, at
	if record.Result.Failed > 0 {
		record.State = "failed"
	}
	identity := storage.IngestRequest{Source: record.Request.Source, CollectorID: record.Request.CollectorID, BatchID: record.Request.BatchID}
	if record.ReceiptRetention > 0 {
		identity.IdempotencyKey = record.Request.IdempotencyKey
	}
	record.Request = identity
	if !record.receiptExpiresAt().IsZero() {
		if managed := a.managedReceiptKey(key); managed != "" {
			if err := a.Files.JournalReplicatedObjects(ctx, []string{managed, key}); err != nil {
				return err
			}
		}
	}
	if err := a.saveAccepted(ctx, key, record); err != nil {
		return err
	}
	if compact {
		return a.partitionStore().RetireIngestDecision(ctx, txn)
	}
	return nil
}

func (c *Cluster) advancePartitionIngest(ctx context.Context, g partition.Graph, publishAfter <-chan struct{}) (bool, error) {
	const writeSlice = 1500 * time.Millisecond
	worked := false
	batched := c.config.Protocol >= partition.BatchProtocol
	sliceEnd := time.Now().Add(writeSlice)
	limit := max(1, c.FlushMaxRequests)
	var run *partitionIngestRun
	if c.config.Protocol >= partition.IngestHandoffProtocol {
		run = &partitionIngestRun{}
	}
	for i := range limit {
		if deadline, ok := ctx.Deadline(); ok {
			if batched && time.Until(deadline) < 2*time.Second || !batched && worked && time.Until(deadline) < time.Second {
				break
			}
		}
		// Give an active collection regular turns even while a hot tenant's
		// WAL remains nonempty. A started transaction gets its own deadline.
		if batched && worked && g.GC != nil && time.Now().After(sliceEnd) {
			break
		}
		transaction, cancel := context.WithTimeout(ctx, 8*time.Second)
		if run != nil {
			run.canContinue = func() bool {
				deadline, bounded := ctx.Deadline()
				return i+1 < limit && ctx.Err() == nil && (!bounded || time.Until(deadline) >= 2*time.Second) &&
					(publishAfter != nil || g.GC == nil || time.Now().Before(sliceEnd))
			}
		}
		applied, err := c.flushPartitionIngest(transaction, g, publishAfter, run)
		cancel()
		worked = worked || applied
		if err != nil || !applied {
			return worked, err
		}
		if publishAfter != nil {
			// The first batch waited for GC before publishing. Give the WAL
			// its own time slice once the overlap is over.
			publishAfter = nil
			sliceEnd = time.Now().Add(writeSlice)
		}
	}
	return worked, nil
}

func (c *Cluster) flushPartitionIngest(ctx context.Context, g partition.Graph, publishAfter <-chan struct{}, run *partitionIngestRun) (bool, error) {
	c.App.mu.Lock()
	key, err := c.App.firstPartitionIngest(ctx, g.Tenant, g.Generation)
	var record acceptedRequest
	if err == nil && key != "" {
		record, err = c.App.accepted(ctx, key)
	}
	c.App.mu.Unlock()
	if err != nil || key == "" {
		return false, err
	}
	finish := c.metrics.Start("partition_wal_execute")
	finishBarrier := c.metrics.Start("partition_wal_read_barrier")
	err = c.Node.ReadBarrier(ctx)
	finishBarrier(err)
	if err != nil {
		finish(err)
		return false, err
	}
	engine := c.partitionEngine(g.Tenant, g.Generation, c.config.Protocol >= partition.BatchProtocol)
	engine.PublishAfter = publishAfter
	if run != nil {
		remote := engine.Remote.(partitionCoordinator)
		remote.ingest = run
		engine.Remote = remote
		engine.IngestPreparation = run.preparation
		run.preparation = nil
	}
	ref := ""
	if c.config.Protocol >= partition.CompactIngestProtocol {
		ref = partitionIngestReference(key)
	}
	_, submitErr := engine.SubmitAcceptedIngest(ctx, partitionIngestTxnIdentity(key, record.Index, record.ReceiptRetention > 0), partition.IngestInput{Request: record.Request, AcceptedAt: record.AcceptedAt}, ref)
	finish(submitErr)
	if ref != "" && (submitErr != nil || engine.BatchFragments) {
		// Protocol 8 completes the receipt with graph cleanup. An older
		// decision may still require the separate completion command.
		c.App.mu.RLock()
		latest, readErr := c.App.accepted(ctx, key)
		c.App.mu.RUnlock()
		if readErr == nil && latest.Index == record.Index && latest.Result != nil && latest.State != "accepted" {
			return true, nil
		}
	}
	if submitErr != nil {
		return true, submitErr
	}
	if publishAfter != nil {
		select {
		case <-publishAfter:
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
	cmd, err := newCommand("partition_ingest_complete")
	if err != nil {
		return true, err
	}
	cmd.Tenant, cmd.ExpectedGeneration, cmd.IDs = g.Tenant, g.Generation, []string{key}
	if record.ReceiptRetention > 0 {
		cmd.PartitionAcceptedIndex = record.Index
	}
	response, err := c.propose(ctx, cmd)
	if err == nil {
		var result httpResult
		err = json.Unmarshal(response, &result)
		if err == nil && result.Status != http.StatusOK {
			err = fmt.Errorf("complete partitioned ingest: %s", result.Body)
		}
	}
	return true, err
}
