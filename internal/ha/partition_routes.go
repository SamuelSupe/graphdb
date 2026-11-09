package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) partitionStore() partition.Store {
	return partition.Store{Objects: a.Files, Prefix: a.Store.Prefix}
}

func (c *Cluster) partitionAction(ctx context.Context, action partition.Action) ([]byte, error) {
	if c.config.Protocol < 5 || c.App.Catalog || c.App.ShardID == "" {
		return nil, fmt.Errorf("graph partitions require a protocol-5 data group")
	}
	if storage.ValidateTenantID(action.Tenant) != nil || action.Generation <= 0 {
		return nil, fmt.Errorf("%w: invalid graph scope", storage.ErrInvalidArgument)
	}
	c.App.mu.RLock()
	var scopeErr error
	switch action.Operation {
	case "initialize", "reserve", "reserve_partitions", "data_move_reserve":
	case "prepare", "finalize", "discard", "fence", "data_move_page", "data_move_fence", "data_move_install", "data_move_thaw", "data_move_retire", "data_move_capture", "data_gc_begin", "data_gc_step":
		_, scopeErr = c.App.partitionStore().Data(ctx, action.Tenant, action.Generation, action.Partition)
	default:
		var g partition.Graph
		g, scopeErr = c.App.partitionStore().GetGraph(ctx, action.Tenant)
		if scopeErr == nil && g.Generation != action.Generation {
			scopeErr = fmt.Errorf("%w: graph generation changed", storage.ErrConflict)
		}
	}
	c.App.mu.RUnlock()
	if scopeErr != nil {
		return nil, scopeErr
	}
	if action.Operation == "prepare" || action.Operation == "stage_input" || action.Operation == "data_gc_begin" || action.Operation == "data_move_page" {
		if err := c.App.Store.CheckWriteDiskSpace(ctx, int64(len(action.Data))+partition.MaxChangeBytes*2); err != nil {
			return nil, err
		}
	}
	cmd, err := newCommand("graph_partition")
	if err != nil {
		return nil, err
	}
	if action.Operation == "pin" || strings.HasPrefix(action.Operation, "graph_move_") || strings.HasPrefix(action.Operation, "graph_gc_") {
		action.At = cmd.At
	}
	cmd.Tenant = action.Tenant
	cmd.Body, err = json.Marshal(action)
	if err != nil {
		return nil, err
	}
	return c.propose(ctx, cmd)
}

func (c *Cluster) registerPartitionRoutes(mux *http.ServeMux) {
	c.registerPartitionEnable(mux)
	c.registerPartitionMove(mux)
	c.registerPartitionGC(mux)
	ready := func(w http.ResponseWriter, r *http.Request) bool {
		if c.config.Protocol < 5 || c.App.Catalog || c.App.ShardID == "" {
			http.Error(w, "graph partitions require protocol 5", http.StatusConflict)
			return false
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return false
		}
		return true
	}
	mux.HandleFunc("GET /cluster/graph/descriptor/{tenant}", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		tenant := r.PathValue("tenant")
		if storage.ValidateTenantID(tenant) != nil {
			http.Error(w, "invalid tenant", http.StatusBadRequest)
			return
		}
		c.App.mu.RLock()
		g, err := c.App.partitionStore().GetGraph(r.Context(), tenant)
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g)
	})
	mux.HandleFunc("GET /cluster/graph/transactions/{tenant}/{identity}", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		tenant := r.PathValue("tenant")
		generation, err := strconv.ParseInt(r.URL.Query().Get("generation"), 10, 64)
		if err != nil || generation <= 0 || storage.ValidateTenantID(tenant) != nil || len(r.PathValue("identity")) > 512 {
			http.Error(w, "invalid graph transaction scope", http.StatusBadRequest)
			return
		}
		c.App.mu.RLock()
		g, err := c.App.partitionStore().GetGraph(r.Context(), tenant)
		var txn partition.Transaction
		if err == nil && g.Generation != generation {
			err = fmt.Errorf("%w: graph generation changed", storage.ErrConflict)
		}
		if err == nil {
			txn, err = c.App.partitionStore().Transaction(r.Context(), tenant, generation, r.PathValue("identity"))
		}
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(txn)
	})
	mux.HandleFunc("GET /cluster/graph/pins/{tenant}/{pin}", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		c.App.mu.RLock()
		value, err := c.App.partitionStore().Pinned(r.Context(), r.PathValue("tenant"), r.PathValue("pin"), r.URL.Query().Get("query"), time.Now().UTC())
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(value)
	})
	mux.HandleFunc("POST /cluster/graph/action", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		var action partition.Action
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&action) != nil {
			http.Error(w, "invalid partition action", http.StatusBadRequest)
			return
		}
		data, err := c.partitionAction(r.Context(), action)
		if err != nil {
			c.partitionError(w, err)
			return
		}
		writeResult(w, data)
	})
	mux.HandleFunc("POST /cluster/graph/input", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		var input partition.Action
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || storage.ValidateTenantID(input.Tenant) != nil || input.Generation <= 0 || len(input.Inputs) != 1 {
			http.Error(w, "invalid graph input reference", http.StatusBadRequest)
			return
		}
		c.App.mu.RLock()
		data, err := c.App.partitionStore().Input(r.Context(), input.Tenant, input.Generation, input.Inputs[0])
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	})
	mux.HandleFunc("POST /cluster/graph/read", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		var input partition.ReadRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil {
			http.Error(w, "invalid partition read", http.StatusBadRequest)
			return
		}
		c.App.mu.RLock()
		result, err := c.App.partitionStore().Read(r.Context(), input)
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
}

func (c *Cluster) partitionError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, storage.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, storage.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, storage.ErrInvalidArgument):
		status = http.StatusBadRequest
	}
	data, _ := resultJSON(status, map[string]any{"code": "graph_partition_error", "error": err.Error(), "retryable": status >= 500})
	writeResult(w, data)
}
