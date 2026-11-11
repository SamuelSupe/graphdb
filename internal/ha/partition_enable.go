package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (c *Cluster) graphHome() partition.Group {
	return partition.Group{ID: c.App.ShardID, ClusterID: c.config.ClusterID, Peers: c.config.Peers}
}

func (c *Cluster) enablePartitionedGraph(ctx context.Context, input partition.EnableRequest) (partition.Graph, error) {
	if c.config.Protocol < 5 || c.App.Catalog || c.App.ShardID == "" || storage.ValidateTenantID(input.Tenant) != nil || input.Epoch == 0 || (len(input.Groups) < 2 || len(input.Groups) > 256) {
		return partition.Graph{}, fmt.Errorf("%w: partitioning requires a tenant, epoch and 2 to 256 protocol-5 data groups", storage.ErrInvalidArgument)
	}
	ids := make([]string, 0, len(input.Groups))
	for id, g := range input.Groups {
		shard := sharding.Shard{ID: g.ID, ClusterID: g.ClusterID, Peers: g.Peers}
		if id != g.ID || shard.Validate() != nil {
			return partition.Graph{}, fmt.Errorf("%w: invalid data group", storage.ErrInvalidArgument)
		}
		var identity struct {
			Shard   string `json:"shard_id"`
			Cluster string `json:"cluster_id"`
			Prefix  string `json:"prefix"`
			Format  int    `json:"graph_partition_format"`
		}
		if err := c.shards.JSON(ctx, shard, http.MethodGet, "/cluster/identity", nil, &identity); err != nil {
			return partition.Graph{}, err
		}
		if identity.Shard != id || identity.Cluster != g.ClusterID || identity.Prefix != c.App.Store.Prefix || identity.Format != partition.FormatVersion {
			return partition.Graph{}, fmt.Errorf("%w: data group does not support the graph layout", storage.ErrConflict)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	c.maintenanceMu.Lock()
	defer c.maintenanceMu.Unlock()
	resume, err := c.pauseTenantWrites(ctx, input.Tenant)
	if err != nil {
		return partition.Graph{}, err
	}
	defer resume()
	c.App.mu.RLock()
	existing, existingErr := c.App.partitionStore().GetGraph(ctx, input.Tenant)
	c.App.mu.RUnlock()
	if existingErr == nil {
		if existing.State == "active" && existing.SourceRetired {
			return existing, nil
		}
		return c.copyPartitionedGraph(ctx, existing)
	}
	if !errors.Is(existingErr, storage.ErrNotFound) {
		return partition.Graph{}, existingErr
	}
	if err := c.checkPartitionSource(ctx, input.Tenant); err != nil {
		return partition.Graph{}, err
	}
	schema, manifest, err := c.App.Store.ReadCurrentSchema(ctx, input.Tenant)
	if err != nil {
		return partition.Graph{}, err
	}
	sourceHash := manifest.DataHash
	if sourceHash == "" && manifest.Version == 0 {
		sourceHash, err = schema.ContentHash()
		if err != nil {
			return partition.Graph{}, err
		}
	}
	if sourceHash == "" {
		return partition.Graph{}, fmt.Errorf("%w: the source requires a verified published logical digest before conversion", storage.ErrConflict)
	}
	tasks, err := c.App.Store.ListTasks(ctx, input.Tenant, storage.TaskListOptions{})
	if err != nil {
		return partition.Graph{}, err
	}
	for _, task := range tasks {
		if task.Status == storage.TaskStatusQueued || task.Status == storage.TaskStatusRunning {
			return partition.Graph{}, fmt.Errorf("%w: finish tenant tasks before enabling partitions", storage.ErrConflict)
		}
	}
	generation, err := c.App.Store.ReplicationTenantGeneration(ctx, input.Tenant)
	if err != nil {
		return partition.Graph{}, err
	}
	policy, policyConfigured, err := c.App.Store.GetSourcePolicy(ctx, input.Tenant)
	if err != nil {
		return partition.Graph{}, err
	}
	config, _, err := c.App.Store.GetTenantConfig(ctx, input.Tenant)
	if err != nil {
		return partition.Graph{}, err
	}
	if c.App.Store.Backpressure != nil {
		limits := c.App.Store.Backpressure.Config()
		if config.Quota.MaxEntitiesPerTenant == nil {
			maximum := limits.MaxEntitiesPerTenant
			config.Quota.MaxEntitiesPerTenant = &maximum
		}
		if config.Quota.MaxEdgesPerTenant == nil {
			maximum := limits.MaxEdgesPerTenant
			config.Quota.MaxEdgesPerTenant = &maximum
		}
	}
	relationSchemas, err := c.App.Store.GetRelationSchemas(ctx, input.Tenant)
	if err != nil {
		return partition.Graph{}, err
	}
	g := partition.Graph{Format: partition.FormatVersion, Tenant: input.Tenant, Generation: generation + 1, Version: manifest.Version, Groups: input.Groups, SourcePolicy: policy, SourcePolicyConfigured: policyConfigured, LayoutRevision: 1, CITypes: schema.ListCITypes(), RelationTypes: schema.ListRelationTypes(), Config: storage.TenantConfigWithDefaults(config), RelationSchemas: relationSchemas, Copy: &partition.CopyProgress{Phase: "schema", DataHash: sourceHash}}
	for i := range g.Partitions {
		g.Partitions[i] = partition.Placement{Group: ids[i%len(ids)], Epoch: 1}
	}
	remote := sharding.GraphRemote{Client: c.shards}
	for _, id := range ids {
		var buckets []uint8
		for i, p := range g.Partitions {
			if p.Group == id {
				buckets = append(buckets, uint8(i))
			}
		}
		if err := remote.Action(ctx, input.Groups[id], partition.Action{Operation: "reserve_partitions", Tenant: input.Tenant, Generation: g.Generation, Epoch: 1, Participants: buckets}, nil); err != nil {
			return partition.Graph{}, err
		}
	}
	if err := remote.Action(ctx, c.graphHome(), partition.Action{Operation: "initialize", Tenant: input.Tenant, Generation: g.Generation, Epoch: input.Epoch, Graph: &g}, nil); err != nil {
		return partition.Graph{}, err
	}
	return c.copyPartitionedGraph(ctx, g)
}

func (c *Cluster) runPartitionBackground(ctx context.Context) {
	const concurrency = 8
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	type result struct {
		tenant string
		err    error
	}
	done := make(chan result, concurrency)
	active := map[string]bool{}
	failed := map[string]time.Time{}
	failures := map[string]string{}
	var workers sync.WaitGroup
	defer workers.Wait()
	offset := 0
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-done:
			delete(active, result.tenant)
			if result.err == nil {
				delete(failed, result.tenant)
				delete(failures, result.tenant)
			} else if ctx.Err() == nil {
				failed[result.tenant] = time.Now().Add(time.Second)
				if failures[result.tenant] != result.err.Error() {
					failures[result.tenant] = result.err.Error()
					slog.Warn("graph background step failed", "tenant", result.tenant, "group", c.App.ShardID, "error", result.err)
				}
			}
		case <-ticker.C:
			if len(active) == concurrency || c.Node.LeaderID() != c.config.ID {
				continue
			}
			operation, cancel := context.WithTimeout(ctx, time.Second)
			if c.Node.ReadBarrier(operation) != nil {
				cancel()
				continue
			}
			c.App.mu.RLock()
			tenants, err := c.App.partitionStore().GraphTenants(operation)
			c.App.mu.RUnlock()
			cancel()
			if err != nil {
				slog.Warn("list graph background work", "group", c.App.ShardID, "error", err)
				continue
			}
			for count := 0; count < len(tenants) && len(active) < concurrency; count++ {
				offset %= len(tenants)
				tenant := tenants[offset]
				offset++
				if active[tenant] || time.Now().Before(failed[tenant]) {
					continue
				}
				active[tenant] = true
				workers.Go(func() {
					operation, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					done <- result{tenant, c.advancePartitionBackground(operation, tenant)}
				})
			}
		}
	}
}

func (c *Cluster) advancePartitionBackground(ctx context.Context, tenant string) error {
	g, err := c.App.graphDescriptor(ctx, tenant)
	if err != nil {
		return err
	}
	remote := sharding.GraphRemote{Client: c.shards}
	if g.Active != nil {
		_, err := (partition.Engine{Remote: remote, Home: c.graphHome(), Tenant: tenant}).Resume(ctx)
		if err != nil {
			return err
		}
		g, err = c.App.graphDescriptor(ctx, tenant)
		if err != nil {
			return err
		}
	}
	if g.Move != nil {
		if err := c.advancePartitionMove(ctx, g); err != nil {
			return err
		}
		g, err = c.App.graphDescriptor(ctx, tenant)
		if err != nil {
			return err
		}
	}
	if g.GC != nil {
		if err := c.advancePartitionGC(ctx, g); err != nil {
			return err
		}
		g, err = c.App.graphDescriptor(ctx, tenant)
		if err != nil {
			return err
		}
	}
	if g.State == "active" && g.SourceCleanupRevision < partition.ActionRevision {
		return remote.Action(ctx, c.graphHome(), partition.Action{Operation: "retire_source", Tenant: tenant, Generation: g.Generation}, nil)
	}
	if g.State == "active" && g.SourceRetired && g.GC == nil && g.Move == nil {
		interval := g.Config.Maintenance.GCIntervalSeconds
		if interval != nil && *interval > 0 && time.Now().After(g.LastGCAt.Add(time.Duration(*interval)*time.Second)) {
			id, err := newCommand("graph_gc")
			if err != nil {
				return err
			}
			return remote.Action(ctx, c.graphHome(), partition.Action{Operation: "graph_gc_begin", Tenant: tenant, Generation: g.Generation, Txn: id.ID}, nil)
		}
	}
	if g.State == "active" && !g.Move.BlocksWrites() && !g.GC.BlocksWrites() {
		_, err := c.advancePartitionIngest(ctx, g)
		return err
	}
	return nil
}

func (c *Cluster) registerPartitionEnable(mux *http.ServeMux) {
	mux.HandleFunc("GET /cluster/graph/check/{tenant}", c.partitionSourceCheck)
	mux.HandleFunc("POST /cluster/graph/enable", func(w http.ResponseWriter, r *http.Request) {
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		var input partition.EnableRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil {
			http.Error(w, "invalid graph partition configuration", http.StatusBadRequest)
			return
		}
		g, err := c.enablePartitionedGraph(r.Context(), input)
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g)
	})
}
