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
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type command struct {
	ExpectedGeneration int64       `json:"expected_generation,omitempty"`
	RouteEpoch         uint64      `json:"route_epoch,omitempty"`
	ID                 string      `json:"id"`
	At                 time.Time   `json:"at"`
	Kind               string      `json:"kind"`
	Role               string      `json:"role,omitempty"`
	Tenant             string      `json:"tenant,omitempty"`
	Method             string      `json:"method,omitempty"`
	URI                string      `json:"uri,omitempty"`
	Header             http.Header `json:"header,omitempty"`
	Body               []byte      `json:"body,omitempty"`
	IDs                []string    `json:"ids,omitempty"`
	Restore            []byte      `json:"restore,omitempty"`
	Error              string      `json:"error,omitempty"`
}

type httpResult struct {
	Status int         `json:"status"`
	Header http.Header `json:"header,omitempty"`
	Body   []byte      `json:"body,omitempty"`
}

type Application struct {
	Store            *storage.TenantStore
	Files            *storage.FileStore
	Handler          http.Handler
	MaxSnapshotBytes int64
	MaxPendingBytes  int64
	FlushInterval    time.Duration
	ShardID          string
	Catalog          bool
	mu               sync.RWMutex
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
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Files.InstallReplicationSnapshot(ctx, index, data, a.MaxSnapshotBytes)
}

func (a *Application) Apply(ctx context.Context, index uint64, data []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(data) == 0 {
		return a.Files.ApplyReplicated(ctx, index, "", time.Time{}, func(context.Context) ([]byte, error) { return nil, nil })
	}
	var cmd command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	if cmd.ID == "" || cmd.At.IsZero() {
		return nil, fmt.Errorf("replication command has no identity or timestamp")
	}
	if cmd.Role != "" && cmd.Role != a.replicationRole() {
		return nil, fmt.Errorf("replicated group role %q differs from configured role %q", cmd.Role, a.replicationRole())
	}
	return a.Files.ApplyReplicated(ctx, index, cmd.ID, cmd.At, func(applyCtx context.Context) ([]byte, error) {
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
				return resultJSON(http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "tenant has been replaced; obtain a new read token"})
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
			if cmd.Tenant != "" && writer.Code < 400 {
				generation, err := a.Store.ReplicationTenantGeneration(applyCtx, cmd.Tenant)
				if err != nil {
					return nil, err
				}
				writer.Header().Set("X-GraphDB-Tenant-Generation", fmt.Sprint(generation))
			}
			return json.Marshal(httpResult{Status: writer.Code, Header: writer.Header(), Body: writer.Body.Bytes()})
		case "accept":
			return a.accept(applyCtx, index, cmd)
		case "flush":
			return a.flush(applyCtx, cmd)
		case "capture_backup":
			if len(cmd.IDs) != 1 {
				return nil, fmt.Errorf("invalid backup capture command")
			}
			return nil, a.Store.CaptureReplicatedObjectBackup(applyCtx, cmd.Tenant, cmd.IDs[0])
		case "task":
			if len(cmd.IDs) != 1 {
				return nil, fmt.Errorf("invalid replicated task command")
			}
			if cmd.Error != "" {
				task, err := a.Store.FailReplicatedTask(applyCtx, cmd.Tenant, cmd.IDs[0], cmd.Error)
				if err != nil {
					return nil, err
				}
				return json.Marshal(task)
			}
			task, err := a.Store.RunReplicatedTask(applyCtx, cmd.Tenant, cmd.IDs[0], cmd.Restore)
			if err != nil {
				return nil, err
			}
			return json.Marshal(task)
		default:
			return nil, fmt.Errorf("unsupported replicated command %q", cmd.Kind)
		}
	})
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
