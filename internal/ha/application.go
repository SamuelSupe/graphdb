package ha

import (
	"bytes"
	"context"
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
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type command struct {
	ExpectedGeneration int64                       `json:"expected_generation,omitempty"`
	RouteEpoch         uint64                      `json:"route_epoch,omitempty"`
	ID                 string                      `json:"id"`
	At                 time.Time                   `json:"at"`
	Kind               string                      `json:"kind"`
	Role               string                      `json:"role,omitempty"`
	Tenant             string                      `json:"tenant,omitempty"`
	Method             string                      `json:"method,omitempty"`
	URI                string                      `json:"uri,omitempty"`
	Header             http.Header                 `json:"header,omitempty"`
	Body               []byte                      `json:"body,omitempty"`
	IDs                []string                    `json:"ids,omitempty"`
	Restore            []byte                      `json:"restore,omitempty"`
	Error              string                      `json:"error,omitempty"`
	QueueBudget        *int64                      `json:"queue_budget,omitempty"`
	Backpressure       *storage.BackpressureConfig `json:"backpressure,omitempty"`
	BackupNamespace    *backupstore.Namespace      `json:"backup_namespace,omitempty"`
}

type httpResult struct {
	Status int         `json:"status"`
	Header http.Header `json:"header,omitempty"`
	Body   []byte      `json:"body,omitempty"`
}

type Application struct {
	Store              *storage.TenantStore
	Files              *storage.FileStore
	Handler            http.Handler
	MaxSnapshotBytes   int64
	MaxPendingBytes    int64
	FlushInterval      time.Duration
	ShardID            string
	Catalog            bool
	mu                 sync.RWMutex
	readers            sync.RWMutex
	pending            map[string]pendingAcceptance
	pendingBytes       int64
	queueObservation   atomic.Pointer[queueObservation]
	catalogObservation atomic.Pointer[catalogObservation]
	catalogPending     *catalogObservation
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
	}
	return checkpoint.Index, err
}
func (a *Application) Snapshot(ctx context.Context) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	data, err := a.Files.ReplicationSnapshot(ctx, a.MaxSnapshotBytes)
	if errors.Is(err, storage.ErrReplicationSnapshotTooLarge) {
		err = replication.ErrSnapshotTooLarge
	}
	return data, err
}
func (a *Application) Restore(ctx context.Context, index uint64, data []byte) error {
	a.readers.Lock()
	defer a.readers.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pending = nil
	a.queueObservation.Store(nil)
	a.catalogObservation.Store(nil)
	return a.Files.InstallReplicationSnapshot(ctx, index, data, a.MaxSnapshotBytes)
}

func (a *Application) CaptureSnapshot(ctx context.Context) (replication.SnapshotSource, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	source, err := a.Files.CaptureReplicationSnapshot(ctx, a.MaxSnapshotBytes)
	if errors.Is(err, storage.ErrReplicationSnapshotTooLarge) {
		err = replication.ErrSnapshotTooLarge
	}
	return source, err
}

func (a *Application) RestoreSnapshot(ctx context.Context, index uint64, source io.ReadSeeker) error {
	a.readers.Lock()
	defer a.readers.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pending = nil
	a.queueObservation.Store(nil)
	a.catalogObservation.Store(nil)
	return a.Files.InstallReplicationSnapshotReader(ctx, index, source, a.MaxSnapshotBytes)
}

func publicationCommand(cmd command) bool {
	return cmd.Kind == "accept" || cmd.Kind == "flush" || cmd.Kind == "restore_part" ||
		(cmd.Kind == "http" && cmd.Method == http.MethodPost &&
			(cmd.URI == "/v1/ingest/batches" || cmd.URI == "/v1/commits"))
}

func (a *Application) Apply(ctx context.Context, index uint64, data []byte) ([]byte, error) {
	responses, err := a.ApplyBatch(ctx, []replication.ApplyEntry{{Index: index, Data: data}})
	if err != nil {
		return nil, err
	}
	return responses[0], nil
}

func (a *Application) ApplyBatch(ctx context.Context, entries []replication.ApplyEntry) (responses [][]byte, err error) {
	if len(entries) == 0 {
		return nil, nil
	}
	commands := make([]command, 0, min(len(entries), 8))
	for _, entry := range entries[:min(len(entries), 8)] {
		var cmd command
		if len(entry.Data) > 0 {
			if err := json.Unmarshal(entry.Data, &cmd); err != nil {
				if len(commands) > 0 {
					break
				}
				return nil, err
			}
		}
		if len(commands) > 0 && (!publicationCommand(cmd) || cmd.ID == "" || cmd.At.IsZero() ||
			(cmd.Role != "" && cmd.Role != a.replicationRole()) || entry.Index != entries[len(commands)-1].Index+1 ||
			cmd.Kind != commands[0].Kind || cmd.URI != commands[0].URI) {
			break
		}
		commands = append(commands, cmd)
		if !publicationCommand(cmd) {
			break
		}
	}
	entries = entries[:len(commands)]
	publication := publicationCommand(commands[0])
	// Only immutable publications share a journal. A task, ownership change,
	// configuration entry or other control operation remains its own barrier.
	// Keep acceptance separate from graph publication so its durable response
	// does not wait for a later flush in the same log window.
	if !publication {
		a.readers.Lock()
		defer a.readers.Unlock()
	}
	a.mu.Lock()
	a.catalogPending = nil
	defer func() {
		if err != nil {
			a.pending = nil
			a.catalogObservation.Store(nil)
		} else if a.catalogPending != nil {
			a.catalogObservation.Store(a.catalogPending)
		}
		a.catalogPending = nil
		a.observePending()
		a.mu.Unlock()
	}()
	for i, cmd := range commands {
		if len(entries[i].Data) == 0 {
			continue
		}
		if cmd.ID == "" || cmd.At.IsZero() {
			return nil, fmt.Errorf("replication command has no identity or timestamp")
		}
		if cmd.Role != "" && cmd.Role != a.replicationRole() {
			return nil, fmt.Errorf("replicated group role %q differs from configured role %q", cmd.Role, a.replicationRole())
		}
		if !publication {
			a.pending = nil
		}
	}
	var checkpoint storage.ReplicationCheckpoint
	if len(entries) > 1 {
		checkpoint, err = a.Files.ReplicationCheckpoint()
		if err != nil {
			return nil, err
		}
	}
	responses = make([][]byte, len(entries))
	for i, entry := range entries {
		if entry.Index <= checkpoint.Index {
			responses[i] = checkpoint.Response
		}
	}
	last := len(entries) - 1
	data, err := a.Files.ApplyReplicated(ctx, entries[last].Index, commands[last].ID, commands[last].At, func(applyCtx context.Context) ([]byte, error) {
		for i, entry := range entries {
			if entry.Index <= checkpoint.Index || len(entry.Data) == 0 {
				continue
			}
			cmd := commands[i]
			commandCtx := storage.ReplicatedContext(applyCtx, cmd.ID, cmd.At)
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
		return responses[last], nil
	})
	if err != nil {
		return nil, err
	}
	responses[last] = data
	return responses, nil
}

func (a *Application) applyCommand(applyCtx context.Context, index uint64, cmd command) ([]byte, error) {
	if a.ShardID != "" && (cmd.Kind == "http" || cmd.Kind == "accept") {
		valid, err := a.checkOwnership(applyCtx, cmd.Tenant, cmd.RouteEpoch)
		if err != nil {
			return nil, err
		}
		if !valid {
			return resultJSON(http.StatusConflict, map[string]any{"code": "shard_epoch_changed", "error": "tenant ownership changed before application", "retryable": true})
		}
		if err := a.checkShardedMutation(applyCtx, cmd); err != nil {
			return resultJSON(http.StatusConflict, map[string]any{"code": "shard_epoch_changed", "error": err.Error(), "retryable": true})
		}
	}
	if cmd.ExpectedGeneration > 0 {
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
		request, err := http.NewRequestWithContext(applyCtx, cmd.Method, cmd.URI, bytes.NewReader(cmd.Body))
		if err != nil {
			return nil, err
		}
		request.Header = cmd.Header
		writer := httptest.NewRecorder()
		a.Handler.ServeHTTP(writer, request)
		if writer.Code >= 500 {
			return nil, fmt.Errorf("replicated mutation failed locally: HTTP %d: %s", writer.Code, writer.Body.String())
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
	case "accept":
		return a.accept(applyCtx, index, cmd)
	case "flush":
		return a.flush(applyCtx, cmd)
	case "capture_backup":
		return nil, a.Store.CaptureReplicatedObjectBackup(applyCtx, cmd.Tenant, cmd.IDs[0])
	case "restore_part":
		return a.stageRestore(applyCtx, cmd)
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
			task, err := a.runStagedRestore(applyCtx, cmd)
			if err != nil {
				return nil, err
			}
			return json.Marshal(task)
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
	return io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
}
