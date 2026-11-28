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
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type command struct {
	snapshotCleanup bool // Leader-side admission only.

	// Logged so replay keeps the original acceptance and completion semantics.
	ReplaySourceIngest        bool          `json:"replay_source_ingest,omitempty"`
	CompactPartitionIngest    bool          `json:"compact_partition_ingest,omitempty"`
	PartitionReceiptRetention time.Duration `json:"partition_receipt_retention_ns,omitempty"`
	PartitionAcceptedIndex    uint64        `json:"partition_accepted_index,omitempty"`
	IndexIngestQueue          bool          `json:"index_ingest_queue,omitempty"`
	CompactOrdinaryReceipts   bool          `json:"compact_ordinary_ingest_receipts,omitempty"`
	OrdinaryReceiptRetention  time.Duration `json:"ordinary_receipt_retention_ns,omitempty"`

	TupleIdentity         bool                        `json:"tuple_identity,omitempty"`
	TenantScopeValidation bool                        `json:"tenant_scope_validation,omitempty"`
	AdminAuthorized       bool                        `json:"admin_authorized,omitempty"`
	ExpectedGeneration    int64                       `json:"expected_generation,omitempty"`
	RouteEpoch            uint64                      `json:"route_epoch,omitempty"`
	ID                    string                      `json:"id"`
	At                    time.Time                   `json:"at"`
	Kind                  string                      `json:"kind"`
	Role                  string                      `json:"role,omitempty"`
	Prefix                string                      `json:"prefix,omitempty"`
	Tenant                string                      `json:"tenant,omitempty"`
	Method                string                      `json:"method,omitempty"`
	URI                   string                      `json:"uri,omitempty"`
	Header                http.Header                 `json:"header,omitempty"`
	Body                  []byte                      `json:"body,omitempty"`
	IDs                   []string                    `json:"ids,omitempty"`
	Restore               []byte                      `json:"restore,omitempty"`
	Error                 string                      `json:"error,omitempty"`
	QueueBudget           *int64                      `json:"queue_budget,omitempty"`
	Backpressure          *storage.BackpressureConfig `json:"backpressure,omitempty"`
	BackupNamespace       *backupstore.Namespace      `json:"backup_namespace,omitempty"`
}

type httpResult struct {
	Status int         `json:"status"`
	Header http.Header `json:"header,omitempty"`
	Body   []byte      `json:"body,omitempty"`
}

// Application is the deterministic replicated Raft state machine.
//
// Lock Hierarchy (strictly top-to-bottom; never perform network I/O or
// multi-megabyte codecs while holding Application locks):
//
//	Application.readers (ReadersBarrier)
//	  -> Application.mu
//	    -> TenantStore tenantLock (foreground / maintenance)
//	      -> FileStore.objectLock
//	        -> FileStore.receiptMu
type Application struct {
	metrics              *observability.OperationMetrics
	graphCacheMu         sync.Mutex
	graphCache           map[string]graphDescriptor
	logicalDigests       partition.LogicalDigestCache
	Store                *storage.TenantStore
	Files                *storage.FileStore
	Handler              http.Handler
	MaxSnapshotBytes     int64
	MaxPendingBytes      int64
	FlushInterval        time.Duration
	ShardID              string
	Catalog              bool
	mu                   sync.RWMutex
	readers              sync.RWMutex
	readMu               sync.Mutex
	activeReads          map[*applicationRead]struct{}
	pending              map[string]pendingAcceptance
	pendingIngestAliases map[string][]string
	pendingBytes         int64
	pendingIndex         pendingReceiptIndex
	tenantReceiptsMu      sync.Mutex
	tenantReceipts        map[string]map[string]struct{}
	tenantReceiptOwners   map[string]string
	tenantReceiptScanning bool
	tenantReceiptScanWait chan struct{}
	tenantReceiptDeltas   []tenantReceiptDelta
	ingestNotifyMu       sync.Mutex
	ingestNotify         chan struct{}
	queueObservation     atomic.Pointer[queueObservation]
	catalogObservation   atomic.Pointer[catalogObservation]
	catalogPending       *catalogObservation
	catalogState         atomic.Pointer[sharding.Catalog]
	catalogStatePending  *sharding.Catalog
}

func (a *Application) Applied() (uint64, error) {
	checkpoint, err := a.Files.ReplicationCheckpoint()
	if err == nil && checkpoint.Index == 0 {
		objects, listErr := a.Files.List(context.Background(), "")
		if listErr != nil {
			return 0, listErr
		}
		if len(objects) > 0 {
			return 0, fmt.Errorf("a new Raft replica requires an empty data directory; existing standalone data cannot be bootstrapped")
		}
		a.initEmptyTenantReceipts()
	}
	return checkpoint.Index, err
}
func (a *Application) Snapshot(ctx context.Context) ([]byte, error) {
	source, err := a.captureSnapshot(ctx)
	var data []byte
	if err == nil {
		defer source.Close()
		data, err = source.Bytes(ctx)
	}
	return data, replicationSnapshotError(err)
}
func (a *Application) Restore(ctx context.Context, index uint64, data []byte) error {
	a.lockReaders()
	defer a.readers.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resetPending()
	a.queueObservation.Store(nil)
	a.catalogObservation.Store(nil)
	a.catalogState.Store(nil)
	a.graphCache = nil
	err := a.Files.InstallReplicationSnapshotValidated(ctx, index, bytes.NewReader(data), a.MaxSnapshotBytes, a.snapshotTenantPrefix(), a.validatePartitionSnapshot)
	if a.Store != nil {
		a.Store.ResetCaches()
	}
	a.notifyIngestChanged()
	return err
}

func (a *Application) CaptureSnapshot(ctx context.Context) (replication.SnapshotSource, error) {
	source, err := a.captureSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return applicationSnapshotSource{source}, nil
}

type applicationSnapshotSource struct {
	*storage.ReplicationSnapshotSource
}

func (s applicationSnapshotSource) WriteTo(ctx context.Context, output io.WriteSeeker) error {
	return replicationSnapshotError(s.ReplicationSnapshotSource.WriteTo(ctx, output))
}

func (s applicationSnapshotSource) WriteToWithPayloadDigest(ctx context.Context, output io.WriteSeeker) ([sha256.Size]byte, error) {
	digest, err := s.ReplicationSnapshotSource.WriteToWithPayloadDigest(ctx, output)
	return digest, replicationSnapshotError(err)
}

func replicationSnapshotError(err error) error {
	if errors.Is(err, storage.ErrReplicationSnapshotTooLarge) {
		return fmt.Errorf("%w: %w", replication.ErrSnapshotTooLarge, err)
	}
	var pressure *storage.BackpressureError
	if errors.As(err, &pressure) || errors.Is(err, storage.ErrObjectStoreUnavailable) {
		return fmt.Errorf("%w: %w", replication.ErrSnapshotTemporarilyUnavailable, err)
	}
	return err
}

func (a *Application) captureSnapshot(ctx context.Context) (*storage.ReplicationSnapshotSource, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	source, err := a.Files.CaptureReplicationSnapshot(ctx, a.MaxSnapshotBytes)
	if err == nil {
		source.TenantPrefix = a.snapshotTenantPrefix()
		source.Validate = a.validatePartitionSnapshot
	}
	return source, replicationSnapshotError(err)
}

func (a *Application) RestoreSnapshot(ctx context.Context, index uint64, source io.ReadSeeker) error {
	a.lockReaders()
	defer a.readers.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resetPending()
	a.queueObservation.Store(nil)
	a.catalogObservation.Store(nil)
	a.catalogState.Store(nil)
	a.graphCache = nil
	err := a.Files.InstallReplicationSnapshotValidated(ctx, index, source, a.MaxSnapshotBytes, a.snapshotTenantPrefix(), a.validatePartitionSnapshot)
	if a.Store != nil {
		a.Store.ResetCaches()
	}
	a.notifyIngestChanged()
	return err
}

func (a *Application) snapshotTenantPrefix() string {
	if a.Store != nil {
		return a.Store.Prefix
	}
	return ""
}

func publicationCommand(cmd command) bool {
	switch cmd.Kind {
	case "accept", "partition_accept", "partition_ingest_complete", "partition_ingest_compact", "partition_ingest_expire", "ordinary_ingest_compact", "ordinary_ingest_reclaim", "ordinary_ingest_expire", "shard_ingest_expire", "flush", "restore_part":
		return true
	case "http":
		uri, _, _ := strings.Cut(cmd.URI, "?")
		return cmd.Method == http.MethodPost && (uri == "/v1/ingest/batches" || uri == "/v1/commits" || uri == "/v1/query/templates" || uri == "/v1/tasks")
	case "graph_partition":
		// Transaction state and input chunks live outside pinned graph views.
		// Publication keeps the immutable roots already selected by readers.
		switch graphCommandOperation(cmd) {
		case "stage_input", "begin", "participants", "prepared", "commit", "abort", "complete",
			"prepare", "finalize", "discard", "pin", "graph_gc_inputs", "sync_groups":
			return true
		}
	case "graph_fragment_batch":
		var actions []struct {
			Operation string `json:"operation"`
		}
		if json.Unmarshal(cmd.Body, &actions) == nil && len(actions) != 0 {
			return actions[0].Operation == "prepare" || actions[0].Operation == "finalize" || actions[0].Operation == "discard"
		}
	}
	return false
}

func graphCommandOperation(cmd command) string {
	var action struct {
		Operation string `json:"operation"`
	}
	if json.Unmarshal(cmd.Body, &action) != nil {
		return ""
	}
	return action.Operation
}

func batchableCommand(cmd command) bool {
	if cmd.Kind == "http" {
		if uri, _, _ := strings.Cut(cmd.URI, "?"); uri == "/v1/tasks" {
			return false
		}
	}
	if cmd.Kind == "graph_fragment_batch" {
		return len(cmd.Body) <= 128<<10 && publicationCommand(cmd)
	}
	if cmd.Kind != "graph_partition" {
		return publicationCommand(cmd)
	}
	// Keep large prepares and graph-wide barriers in separate journals.
	if len(cmd.Body) > 128<<10 {
		return false
	}
	switch graphCommandOperation(cmd) {
	case "prepare", "prepared", "finalize", "discard", "data_gc_begin", "data_gc_step":
		return true
	default:
		return false
	}
}

var _ replication.TypedBatchStateMachine = (*Application)(nil)

func (a *Application) Apply(ctx context.Context, index uint64, data []byte) ([]byte, error) {
	results, fatal := a.ApplyTypedBatch(ctx, []replication.ApplyEntry{{Index: index, Data: data}})
	if fatal != nil {
		return nil, fatal.Unwrap()
	}
	return results[0].ResponseBytes(), nil
}

func (a *Application) ApplyBatch(ctx context.Context, entries []replication.ApplyEntry) ([][]byte, error) {
	results, fatal := a.ApplyTypedBatch(ctx, entries)
	if fatal != nil {
		return nil, fatal.Unwrap()
	}
	responses := make([][]byte, len(results))
	for i, res := range results {
		responses[i] = res.ResponseBytes()
	}
	return responses, nil
}

// ApplyTypedBatch implements replication.TypedBatchStateMachine. Deterministic
// business/failover conflicts are returned as non-fatal replication.ApplyResult
// rejections (recorded in the replication checkpoint); only local storage or
// integrity failures return *replication.FatalMachineError.
func (a *Application) ApplyTypedBatch(ctx context.Context, entries []replication.ApplyEntry) ([]replication.ApplyResult, *replication.FatalMachineError) {
	const retainedCheckpointProtocol = 13
	if replication.ApplyProtocol(ctx) >= retainedCheckpointProtocol {
		ctx = storage.WithCheckpointInReplicationJournal(ctx)
	}
	responses, attempted, err := a.applyBatch(ctx, entries)
	if err == nil {
		results := make([]replication.ApplyResult, len(responses))
		for i, data := range responses {
			results[i] = replication.AcceptResult(data)
		}
		return results, nil
	}
	rejected, ok := err.(*storage.ReplicationRejectedError)
	if !ok {
		return nil, replication.FatalApplyError(err)
	}
	if attempted > 1 {
		// The shared journal was rolled back. Retry one entry so successful
		// neighbours are committed independently of the rejected command.
		return a.ApplyTypedBatch(ctx, entries[:1])
	}
	var cmd command
	if err := json.Unmarshal(entries[0].Data, &cmd); err != nil {
		return nil, replication.FatalApplyError(err)
	}
	if !publicationCommand(cmd) {
		a.lockReaders()
		defer a.readers.Unlock()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	response, err := a.Files.ApplyReplicated(ctx, entries[0].Index, cmd.ID, cmd.At, func(context.Context) ([]byte, error) {
		return rejected.Response, nil
	})
	if err != nil {
		return nil, replication.FatalApplyError(err)
	}
	return []replication.ApplyResult{replication.RejectResult(response, "replicated_conflict", "deterministic rejection")}, nil
}

func (a *Application) applyBatch(ctx context.Context, entries []replication.ApplyEntry) (responses [][]byte, attempted int, err error) {
	if len(entries) == 0 {
		return nil, 0, nil
	}
	commands := make([]command, 0, min(len(entries), 8))
	for _, entry := range entries[:min(len(entries), 8)] {
		var cmd command
		if len(entry.Data) > 0 {
			if err := json.Unmarshal(entry.Data, &cmd); err != nil {
				if len(commands) > 0 {
					break
				}
				return nil, 0, err
			}
		}
		batchable := batchableCommand(cmd)
		if len(commands) > 0 && (!batchable || cmd.ID == "" || cmd.At.IsZero() ||
			(cmd.Role != "" && cmd.Role != a.replicationRole()) ||
			(cmd.Prefix != "" && cmd.Prefix != a.snapshotTenantPrefix()) || entry.Index != entries[len(commands)-1].Index+1 ||
			cmd.Kind != commands[0].Kind || cmd.URI != commands[0].URI) {
			break
		}
		commands = append(commands, cmd)
		if !batchable {
			break
		}
	}
	entries = entries[:len(commands)]
	attempted = len(entries)
	publication := publicationCommand(commands[0])
	for _, cmd := range commands[1:] {
		publication = publication && publicationCommand(cmd)
	}
	allHTTP := true
	for _, cmd := range commands {
		if cmd.Kind != "http" {
			allHTTP = false
			break
		}
	}
	// Graph participant actions can share a journal while still holding the
	// reader barrier for the whole application. Multi-entry HTTP batches write
	// live manifests before later entries run, so hold the reader barrier to
	// prevent uncommitted entries from becoming visible if a sibling rejects.
	if !publication || (len(commands) > 1 && allHTTP) {
		a.lockReaders()
		defer a.readers.Unlock()
	}
	a.mu.Lock()
	a.catalogPending = nil
	a.catalogStatePending = nil
	defer func() {
		if err != nil {
			if _, rejected := err.(*storage.ReplicationRejectedError); !rejected || !allHTTP {
				a.resetPending()
			}
			a.catalogObservation.Store(nil)
			a.catalogState.Store(nil)
		} else if a.catalogPending != nil {
			a.catalogObservation.Store(a.catalogPending)
			a.catalogState.Store(a.catalogStatePending)
		}
		a.catalogPending = nil
		a.catalogStatePending = nil
		for _, cmd := range commands {
			if cmd.Kind == "graph_partition" {
				delete(a.graphCache, cmd.Tenant)
			}
		}
		a.observePending()
		a.mu.Unlock()
	}()
	for i, cmd := range commands {
		if len(entries[i].Data) == 0 {
			continue
		}
		if cmd.ID == "" || cmd.At.IsZero() {
			return nil, attempted, fmt.Errorf("replication command has no identity or timestamp")
		}
		if cmd.Role != "" && cmd.Role != a.replicationRole() {
			return nil, attempted, fmt.Errorf("replicated group role %q differs from configured role %q", cmd.Role, a.replicationRole())
		}
		if cmd.Prefix != "" && cmd.Prefix != a.snapshotTenantPrefix() {
			return nil, attempted, fmt.Errorf("replicated data prefix %q differs from configured prefix %q", cmd.Prefix, a.snapshotTenantPrefix())
		}

	}
	var checkpoint storage.ReplicationCheckpoint
	if len(entries) > 1 {
		checkpoint, err = a.Files.ReplicationCheckpoint()
		if err != nil {
			return nil, attempted, err
		}
	}
	responses = make([][]byte, len(entries))
	for i, entry := range entries {
		if entry.Index <= checkpoint.Index {
			responses[i] = checkpoint.Response
		}
	}
	last := len(entries) - 1
	data, err := a.Files.ApplyReplicated(storage.WithReplicationMetrics(ctx, a.metrics), entries[last].Index, commands[last].ID, commands[last].At, func(applyCtx context.Context) ([]byte, error) {
		indexQueue := a.ShardID != ""
		for i, entry := range entries {
			if entry.Index <= checkpoint.Index || len(entry.Data) == 0 {
				continue
			}
			cmd := commands[i]
			if cmd.IndexIngestQueue {
				indexQueue = true
				if err := a.ensurePending(applyCtx); err != nil {
					return nil, err
				}
			} else if cmd.Kind == "ordinary_ingest_reclaim" {
				indexQueue = true
			}
			commandCtx := storage.ReplicatedContext(applyCtx, cmd.ID, cmd.At)
			if !cmd.TupleIdentity {
				commandCtx = storage.LegacyIdentityContext(commandCtx)
			}
			if cmd.Backpressure != nil {
				commandCtx = storage.ReplicatedBackpressureContext(commandCtx, *cmd.Backpressure)
			}
			if cmd.BackupNamespace != nil {
				commandCtx = storage.ReplicatedBackupContext(commandCtx, *cmd.BackupNamespace)
			}
			response, err := a.applyCommand(commandCtx, entry.Index, cmd)
			if err != nil {
				return nil, err
			}
			responses[i] = response
		}
		if indexQueue && a.pending != nil {
			if err := a.checkpointPending(applyCtx); err != nil {
				return nil, err
			}
		}
		return responses[last], nil
	})
	if err != nil {
		if a.Store != nil {
			a.Store.ResetCaches()
		}
		clear(a.graphCache)
		return nil, attempted, err
	}
	responses[last] = data
	for _, cmd := range commands {
		if cmd.Kind == "sharding" && a.ShardID != "" && shardingMutatesReceipts(cmd.Body) {
			a.notifyIngestChanged()
			break
		}
	}
	return responses, attempted, nil
}

func shardingMutatesReceipts(body []byte) bool {
	var action struct {
		Operation string `json:"operation"`
	}
	if json.Unmarshal(body, &action) != nil {
		return true
	}
	switch action.Operation {
	case "install", "retire", "discard":
		return true
	default:
		return false
	}
}

func (a *Application) ingestWaitCh() <-chan struct{} {
	a.ingestNotifyMu.Lock()
	if a.ingestNotify == nil {
		a.ingestNotify = make(chan struct{})
	}
	ch := a.ingestNotify
	a.ingestNotifyMu.Unlock()
	return ch
}

func (a *Application) notifyIngestChanged() {
	a.ingestNotifyMu.Lock()
	if a.ingestNotify != nil {
		close(a.ingestNotify)
		a.ingestNotify = make(chan struct{})
	}
	a.ingestNotifyMu.Unlock()
}

func (a *Application) applyCommand(applyCtx context.Context, index uint64, cmd command) ([]byte, error) {
	if a.ShardID != "" && storage.ValidateTenantID(cmd.Tenant) == nil && (cmd.Kind == "http" || cmd.Kind == "accept" || cmd.Kind == "task" || cmd.Kind == "restore_part" || cmd.Kind == "capture_backup") {
		_, err := a.partitionStore().GetGraph(applyCtx, cmd.Tenant)
		if err == nil {
			return resultJSON(http.StatusConflict, map[string]any{"code": "graph_mode_changed", "error": "tenant uses partitioned graph commands", "retryable": true})
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	if a.ShardID != "" && (cmd.Kind == "http" || cmd.Kind == "accept" || cmd.Kind == "partition_accept") {
		valid, err := a.checkOwnership(applyCtx, cmd.Tenant, cmd.RouteEpoch)
		if err != nil {
			return nil, err
		}
		if !valid {
			return resultJSON(http.StatusConflict, map[string]any{"code": "shard_epoch_changed", "error": "tenant ownership changed before application", "retryable": true})
		}
		if err := a.checkShardedMutation(applyCtx, cmd); err != nil {
			if errors.Is(err, storage.ErrConflict) || errors.Is(err, storage.ErrInvalidArgument) {
				return resultJSON(http.StatusConflict, map[string]any{"code": "shard_epoch_changed", "error": err.Error(), "retryable": true})
			}
			return nil, err
		}
	}
	if cmd.ExpectedGeneration > 0 && cmd.Kind != "partition_accept" && cmd.Kind != "partition_ingest_complete" {
		generation, err := a.Store.ReplicationTenantGeneration(applyCtx, cmd.Tenant)
		if err != nil {
			return nil, err
		}
		if generation != cmd.ExpectedGeneration {
			if len(cmd.IDs) == 1 && (cmd.Kind == "restore_part" || cmd.Kind == "task") {
				_, err := a.Store.FailReplicatedTask(applyCtx, cmd.Tenant, cmd.IDs[0], "tenant generation changed during restore")
				if err != nil && !errors.Is(err, storage.ErrNotFound) {
					return nil, err
				}
				if err := a.clearRestore(applyCtx, cmd.Tenant, cmd.IDs[0]); err != nil {
					return nil, err
				}
			}
			return resultJSON(http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "tenant has been replaced; obtain a new read token"})
		}
	}
	if cmd.Kind == "task" || cmd.Kind == "capture_backup" || cmd.Kind == "restore_part" || cmd.Kind == "maintenance_reset" {
		if len(cmd.IDs) != 1 {
			return nil, fmt.Errorf("invalid replicated task identity")
		}
		// Purge or GC can remove a task after the leader prepares its command.
		// Such a committed command is obsolete, rather than a replica failure.
		if _, err := a.Store.GetTask(applyCtx, cmd.Tenant, cmd.IDs[0]); errors.Is(err, storage.ErrNotFound) {
			if err := a.clearRestore(applyCtx, cmd.Tenant, cmd.IDs[0]); err != nil {
				return nil, err
			}
			return resultJSON(http.StatusConflict, map[string]any{"code": "task_not_found", "error": "task was removed before application"})
		} else if err != nil {
			return nil, err
		}
	}
	switch cmd.Kind {
	case "graph_fragment_batch":
		return a.applyFragmentBatch(applyCtx, cmd)
	case "graph_partition", "graph_conversion_reclaim":
		var action partition.Action
		if err := json.Unmarshal(cmd.Body, &action); err != nil {
			return nil, err
		}
		if a.Catalog || a.ShardID == "" || action.Tenant != cmd.Tenant || (cmd.Kind == "graph_conversion_reclaim") != (action.Operation == "reclaim_conversion") {
			return resultJSON(http.StatusConflict, map[string]any{"code": "graph_partition_conflict", "error": "graph actions require a matching data-group tenant"})
		}
		if action.Operation == "begin" && action.Purpose == "ingest" {
			if err := a.validatePartitionIngestBegin(applyCtx, action); err != nil {
				if !errors.Is(err, storage.ErrConflict) {
					return nil, err
				}
				return resultJSON(http.StatusConflict, map[string]any{"code": "graph_busy", "error": err.Error(), "retryable": true})
			}
		}
		if action.Operation == "initialize" {
			if action.Graph == nil {
				return resultJSON(http.StatusBadRequest, map[string]any{"error": "graph descriptor is required"})
			}
			action.Graph.LastGCAt = cmd.At
			manifest, err := a.Store.CurrentManifest(applyCtx, cmd.Tenant)
			if err != nil {
				return nil, err
			}
			generation, err := a.Store.ReplicationTenantGeneration(applyCtx, cmd.Tenant)
			if err != nil {
				return nil, err
			}
			if manifest.Version != action.Graph.Version || !action.Graph.ConvertsSource(generation) || action.Generation <= generation {
				return resultJSON(http.StatusConflict, map[string]any{"error": "source graph changed before partition initialization"})
			}
			valid, err := a.checkOwnership(applyCtx, cmd.Tenant, action.Epoch)
			if err != nil {
				return nil, err
			}
			if !valid {
				return resultJSON(http.StatusConflict, map[string]any{"error": "tenant ownership changed before partition initialization"})
			}
			if err := a.ensurePending(applyCtx); err != nil {
				return nil, err
			}
			for _, pending := range a.pending {
				if pending.tenant == cmd.Tenant {
					return resultJSON(http.StatusConflict, map[string]any{"error": "drain accepted writes before partition initialization"})
				}
			}
		}
		if action.Operation == "cancel_conversion" {
			valid, err := a.checkOwnership(applyCtx, cmd.Tenant, action.Epoch)
			if err != nil {
				return nil, err
			}
			if !valid {
				return resultJSON(http.StatusConflict, map[string]any{"error": "tenant ownership changed before conversion cancellation"})
			}
		}
		if action.Operation == "retire_source" {
			g, err := a.partitionStore().GetGraph(applyCtx, cmd.Tenant)
			if errors.Is(err, storage.ErrNotFound) {
				last, markerErr := a.partitionStore().LastConversionCancellation(applyCtx, cmd.Tenant)
				if markerErr != nil {
					return nil, markerErr
				}
				if last.Generation >= action.Generation {
					return resultJSON(http.StatusConflict, map[string]any{"error": "source cleanup generation was cancelled"})
				}
				return resultJSON(http.StatusConflict, map[string]any{"error": "source cleanup generation is not active"})
			}
			if err != nil {
				return nil, err
			}
			if g.Generation != action.Generation || g.State != "active" {
				return resultJSON(http.StatusConflict, map[string]any{"error": "source cleanup generation is not active"})
			}
			if g.SourceRetired && g.SourceCleanupRevision >= action.Revision {
				return resultJSON(http.StatusOK, g)
			}
			done, err := a.Store.RetirePartitionSource(applyCtx, cmd.Tenant, action.Revision != 0)
			if err != nil {
				return nil, err
			}
			if !done {
				return resultJSON(http.StatusOK, map[string]any{"done": false})
			}
			action.Operation = "source_retired"
		}
		value, err := a.partitionStore().Apply(applyCtx, action)
		if err != nil {
			status, code := partitionErrorStatus(err)
			if code == "shard_unavailable" || code == "partition_corrupt" {
				return nil, err
			}
			response, encodeErr := resultJSON(status, map[string]any{"code": code, "error": err.Error(), "retryable": code == "graph_input_expired"})
			if encodeErr != nil {
				return nil, encodeErr
			}
			if replication.ApplyProtocol(applyCtx) >= ingestQueueCheckpointProtocol {
				return nil, &storage.ReplicationRejectedError{Response: response}
			}
			return response, nil
		}
		if action.Operation == "complete" && action.ReceiptRef != "" {
			if err := a.completeGraphIngestReceipt(applyCtx, action.ReceiptRef, value.(partition.Transaction), cmd.At); err != nil {
				if errors.Is(err, storage.ErrConflict) || errors.Is(err, storage.ErrNotFound) {
					response, encodeErr := resultJSON(http.StatusConflict, map[string]any{"code": "graph_conflict", "error": err.Error()})
					if encodeErr != nil {
						return nil, encodeErr
					}
					return nil, &storage.ReplicationRejectedError{Response: response}
				}
				return nil, err
			}
			if action.NextIngest != nil {
				if err := a.beginNextPartitionIngest(applyCtx, action, cmd.At); err != nil {
					return nil, err
				}
			}
		}
		return resultJSON(http.StatusOK, value)
	case "sharding":
		var action sharding.Action
		if err := json.Unmarshal(cmd.Body, &action); err != nil {
			return nil, err
		}
		if a.Catalog {
			return a.applyCatalog(applyCtx, action)
		}
		if a.ShardID != "" {
			return a.applyOwnership(applyCtx, action)
		}
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "Raft group has no sharding role"})
	case "http":
		if !cmd.TenantScopeValidation {
			applyCtx = storage.LegacyTenantScopeContext(applyCtx)
		}
		if cmd.AdminAuthorized {
			applyCtx = storage.WithAdminAuthorized(applyCtx)
		}
		request, err := http.NewRequestWithContext(applyCtx, cmd.Method, cmd.URI, bytes.NewReader(cmd.Body))
		if err != nil {
			return nil, err
		}
		request.Header = cmd.Header
		writer := httptest.NewRecorder()
		a.Handler.ServeHTTP(writer, request)
		// Local deadlines, pressure and unknown server failures cannot establish
		// a deterministic rejection of a committed command on every replica.
		var applicationError struct {
			Code   string `json:"code"`
			Detail struct {
				BatchID string `json:"batch_id"`
			} `json:"detail"`
		}
		_ = json.Unmarshal(writer.Body.Bytes(), &applicationError)
		transient429 := writer.Code == 429 && applicationError.Code != string(httpapi.ErrorCodeQuotaExceeded)
		if (writer.Code >= 500 && writer.Code != http.StatusNotImplemented) || writer.Code == 408 || transient429 || writer.Code == 499 {
			return nil, fmt.Errorf("replicated HTTP application failed with status %d", writer.Code)
		}
		persistedIngestFailure := replication.ApplyProtocol(applyCtx) >= ingestQueueCheckpointProtocol &&
			request.URL.Path == "/v1/ingest/batches" && applicationError.Detail.BatchID != "" && writer.Code >= 400 && writer.Code < 500
		if writer.Code >= 400 && !persistedIngestFailure {
			response, err := json.Marshal(httpResult{Status: writer.Code, Header: writer.Header(), Body: writer.Body.Bytes()})
			if err != nil {
				return nil, err
			}
			return nil, &storage.ReplicationRejectedError{Response: response}
		}
		if writer.Code < 400 && strings.HasPrefix(request.URL.Path, "/v1/tasks/") && strings.HasSuffix(request.URL.Path, "/cancel") {
			var task storage.Task
			if err := json.Unmarshal(writer.Body.Bytes(), &task); err != nil {
				return nil, err
			}
			if err := a.clearRestore(applyCtx, task.TenantID, task.ID); err != nil {
				return nil, err
			}
		}
		if cmd.Tenant != "" && writer.Code < 400 {
			generation, err := a.Store.ReplicationTenantGeneration(applyCtx, cmd.Tenant)
			if err != nil {
				return nil, err
			}
			if request.URL.Path == "/v1/tenants/"+cmd.Tenant+"/purge" || (cmd.ExpectedGeneration > 0 && generation != cmd.ExpectedGeneration) {
				if err := a.clearRestore(applyCtx, cmd.Tenant, ""); err != nil {
					return nil, err
				}
			}
			writer.Header().Set("X-GraphDB-Tenant-Generation", fmt.Sprint(generation))
		}
		return json.Marshal(httpResult{Status: writer.Code, Header: writer.Header(), Body: writer.Body.Bytes()})
	case "accept", "partition_accept":
		return a.accept(applyCtx, index, cmd)
	case "partition_ingest_complete":
		return a.completePartitionIngest(applyCtx, cmd)
	case "partition_ingest_compact":
		return a.compactPartitionIngest(applyCtx, cmd)
	case "partition_ingest_expire", "ordinary_ingest_expire", "shard_ingest_expire":
		return a.expireIngestReceipts(applyCtx, cmd)
	case "ordinary_ingest_compact":
		return a.compactOrdinaryIngest(applyCtx, cmd)
	case "ordinary_ingest_reclaim":
		return a.reclaimOrdinaryIngest(applyCtx, cmd)
	case "flush":
		return a.flush(applyCtx, cmd)
	case "capture_backup":
		return nil, a.Store.CaptureReplicatedObjectBackup(applyCtx, cmd.Tenant, cmd.IDs[0])
	case "restore_part":
		return a.stageRestore(applyCtx, cmd).toCommandResult()
	case "maintenance_reset":
		return nil, a.clearRestore(applyCtx, cmd.Tenant, cmd.IDs[0])
	case "task":
		if cmd.Error != "" {
			task, err := a.Store.FailReplicatedTask(applyCtx, cmd.Tenant, cmd.IDs[0], cmd.Error)
			if err != nil {
				return nil, err
			}
			if err := a.clearRestore(applyCtx, cmd.Tenant, task.ID); err != nil {
				return nil, err
			}
			return json.Marshal(task)
		}
		if len(cmd.Body) > 0 {
			return a.runStagedRestore(applyCtx, cmd).toCommandResult()
		}
		task, err := a.Store.RunReplicatedTask(applyCtx, cmd.Tenant, cmd.IDs[0], cmd.Restore)
		if err != nil {
			return nil, err
		}
		if transferTask(task) {
			if err := a.clearRestore(applyCtx, cmd.Tenant, task.ID); err != nil {
				return nil, err
			}
		}
		return json.Marshal(task)
	default:
		return nil, fmt.Errorf("unsupported replicated command %q", cmd.Kind)
	}
}

func writeResult(w http.ResponseWriter, data []byte) {
	var response httpResult
	if err := json.Unmarshal(data, &response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for key, values := range response.Header {
		w.Header()[key] = values
	}
	w.WriteHeader(response.Status)
	w.Write(response.Body)
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	return io.ReadAll(http.MaxBytesReader(w, r.Body, httpapi.MaxWriteRequestBytes))
}
