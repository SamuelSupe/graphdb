package ha

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) replicationRole() string {
	if a.Catalog {
		return "catalog"
	}
	if a.ShardID != "" {
		return "shard:" + a.ShardID
	}
	return ""
}

func (c *Cluster) PrivateHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", c.Node.Handler())
	mux.HandleFunc("GET /cluster/identity", func(w http.ResponseWriter, r *http.Request) {
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"shard_id": c.App.ShardID, "catalog": c.App.Catalog, "cluster_id": c.config.ClusterID, "prefix": c.App.Store.Prefix})
	})
	mux.HandleFunc("GET /cluster/catalog", func(w http.ResponseWriter, r *http.Request) {
		if !c.App.Catalog {
			http.NotFound(w, r)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		c.App.mu.RLock()
		state, err := c.App.catalog(r.Context())
		c.App.mu.RUnlock()
		if err != nil {
			c.writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(state)
	})
	mux.HandleFunc("POST /cluster/action", func(w http.ResponseWriter, r *http.Request) {
		if !c.App.Catalog && c.App.ShardID == "" {
			http.NotFound(w, r)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		var action sharding.Action
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&action); err != nil {
			http.Error(w, "invalid sharding action", http.StatusBadRequest)
			return
		}
		if c.App.Catalog && action.Operation == "register" {
			if action.Shard == nil || action.Shard.Validate() != nil {
				http.Error(w, "invalid shard definition", http.StatusBadRequest)
				return
			}
			var identity struct {
				Shard   string `json:"shard_id"`
				Cluster string `json:"cluster_id"`
				Prefix  string `json:"prefix"`
				Catalog bool   `json:"catalog"`
			}
			if err := c.shards.JSON(r.Context(), *action.Shard, http.MethodGet, "/cluster/identity", nil, &identity); err != nil {
				c.writeError(w, err)
				return
			}
			if identity.Catalog || identity.Shard != action.Shard.ID || identity.Cluster != action.Shard.ClusterID || identity.Prefix != c.App.Store.Prefix {
				http.Error(w, "shard identity, role or object prefix does not match", http.StatusConflict)
				return
			}
		}
		data, err := c.shardAction(r.Context(), action)
		if err != nil {
			c.writeError(w, err)
			return
		}
		writeResult(w, data)
	})
	mux.HandleFunc("GET /cluster/placement/{tenant}", func(w http.ResponseWriter, r *http.Request) {
		if !c.App.Catalog || storage.ValidateTenantID(r.PathValue("tenant")) != nil {
			http.NotFound(w, r)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		c.App.mu.RLock()
		state, err := c.App.catalog(r.Context())
		c.App.mu.RUnlock()
		if err != nil {
			c.writeError(w, err)
			return
		}
		placement, ok := state.Tenants[r.PathValue("tenant")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sharding.Resolution{Placement: placement, Shard: state.Shards[placement.Shard]})
	})
	mux.HandleFunc("POST /cluster/export", func(w http.ResponseWriter, r *http.Request) {
		if c.App.ShardID == "" {
			http.NotFound(w, r)
			return
		}
		var action sharding.Action
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&action) != nil || storage.ValidateTenantID(action.Tenant) != nil || sharding.ValidateIdentifier(action.MoveID) != nil {
			http.Error(w, "invalid migration export", http.StatusBadRequest)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		c.App.mu.RLock()
		data, err := c.App.tenantTransfer(r.Context(), action.Tenant, action.MoveID, action.Epoch)
		c.App.mu.RUnlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})
	mux.Handle("/cluster/data/", http.StripPrefix("/cluster/data", c.App.Handler))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+c.config.Token)) != 1 || r.Header.Get("X-Raft-Cluster") != c.config.ClusterID {
			http.Error(w, "unauthorized cluster request", http.StatusUnauthorized)
			return
		}
		r = r.WithContext(forwardedContext(r))
		if strings.HasPrefix(r.URL.Path, "/cluster/") && !strings.HasPrefix(r.URL.Path, "/cluster/data/") && (c.Node.LeaderID() != c.Node.ID() || c.Node.Draining()) {
			c.forward(w, r, nil, true)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (c *Cluster) checkShardRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, tenant string) (uint64, bool) {
	if c.App.ShardID == "" {
		return 0, true
	}
	if tenant == "" && r.Method == http.MethodGet && (r.URL.Path == "/v1/tenants" || r.URL.Path == "/v1/queries/running") {
		return 0, true
	}
	epoch, err := strconv.ParseUint(r.Header.Get(sharding.EpochHeader), 10, 64)
	if err != nil || storage.ValidateTenantID(tenant) != nil {
		http.Error(w, "sharded requests require a tenant routing epoch; use the router", http.StatusConflict)
		return 0, false
	}
	c.App.mu.RLock()
	valid, err := c.App.checkOwnership(ctx, tenant, epoch)
	c.App.mu.RUnlock()
	if err != nil {
		c.writeError(w, err)
		return 0, false
	}
	if !valid {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{"code": "shard_epoch_changed", "error": "tenant is moving or belongs to another shard", "retryable": true})
		return 0, false
	}
	w.Header().Set("X-GraphDB-Shard-ID", c.App.ShardID)
	w.Header().Set(sharding.EpochHeader, fmt.Sprint(epoch))
	return epoch, true
}

func (a *Application) checkShardedMutation(ctx context.Context, cmd command) error {
	u, err := url.ParseRequestURI(cmd.URI)
	if err != nil {
		return err
	}
	if u.Path == "/v1/tenants" && cmd.Method == http.MethodPost {
		var request struct {
			Tenant string `json:"tenant_id"`
		}
		if json.Unmarshal(cmd.Body, &request) != nil || request.Tenant != cmd.Tenant {
			return fmt.Errorf("tenant creation must match its routed tenant")
		}
	}
	if strings.HasSuffix(u.Path, "/clone") {
		return a.checkCloneOwnership(ctx, cmd.Body, cmd.Header.Get(sharding.TargetEpochHeader))
	}
	if strings.HasSuffix(u.Path, "/restore-drill") || u.Path == "/v1/tasks" {
		var request struct {
			Type    string         `json:"type"`
			Cleanup *bool          `json:"cleanup"`
			Params  map[string]any `json:"params"`
		}
		if json.Unmarshal(cmd.Body, &request) == nil && (strings.HasSuffix(u.Path, "/restore-drill") || request.Type == storage.TaskTypeTenantRestoreDrill) {
			if (request.Cleanup != nil && !*request.Cleanup) || request.Params["cleanup"] == false {
				return fmt.Errorf("sharded restore drills must clean up their temporary target; use tenant migration for persistent placement")
			}
		}
	}
	return nil
}

func (a *Application) checkCloneOwnership(ctx context.Context, body []byte, raw string) error {
	var request struct {
		Target string `json:"target_tenant_id"`
	}
	epoch, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || json.Unmarshal(body, &request) != nil || storage.ValidateTenantID(request.Target) != nil {
		return fmt.Errorf("clone target must be assigned through the router")
	}
	valid, err := a.checkOwnership(ctx, request.Target, epoch)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("clone target ownership changed")
	}
	return nil
}
