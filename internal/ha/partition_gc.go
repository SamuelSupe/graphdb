package ha

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"golang.org/x/sync/errgroup"
)

func (c *Cluster) planGCSteps(ctx context.Context, actions []partition.Action) error {
	if c.Node.LeaderID() != c.config.ID {
		return replication.ErrNotLeader
	}
	store := c.App.partitionStore()
	states := make([]partition.DataState, len(actions))
	c.App.mu.RLock()
	for i, action := range actions {
		state, err := store.Data(ctx, action.Tenant, action.Generation, action.Partition)
		if err != nil {
			c.App.mu.RUnlock()
			return err
		}
		states[i] = state
	}
	c.App.mu.RUnlock()
	workers, workCtx := errgroup.WithContext(ctx)
	workers.SetLimit(4)
	for i, state := range states {
		if state.GC != nil && state.GC.Phase == "sweep" {
			workers.Go(func() error { return c.planGCStep(workCtx, &actions[i], state) })
		}
	}
	return workers.Wait()
}

func (c *Cluster) planGCStep(ctx context.Context, a *partition.Action, state partition.DataState) error {
	finish := c.metrics.Start("partition_gc_scan")
	err := c.App.partitionStore().PlanGCSweep(ctx, a, state)
	finish(err)
	return err
}

func (c *Cluster) advancePartitionGC(ctx context.Context, g partition.Graph) (err error) {
	if g.GC == nil {
		return nil
	}
	remote := sharding.GraphRemote{Client: c.shards}
	base := partition.Action{Tenant: g.Tenant, Generation: g.Generation, Txn: g.GC.ID}
	if g.GC.Phase == "starting" && !g.GC.FenceAt.IsZero() {
		defer func() {
			if err == nil || time.Since(g.GC.FenceAt) < 30*time.Second {
				return
			}
			report, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			action := base
			action.Operation = "graph_gc_cancel_start"
			action.Error = err.Error()
			if len(action.Error) > 1024 {
				action.Error = action.Error[:1024]
			}
			if cancelErr := remote.Action(report, c.graphHome(), action, nil); cancelErr != nil {
				err = errors.Join(err, cancelErr)
			}
		}()
	}
	if g.GC.Phase == "inputs" {
		plan, err := c.App.partitionStore().InputGCPlan(ctx, g)
		if err != nil {
			return err
		}
		plan.Tenant, plan.Generation, plan.Txn = g.Tenant, g.Generation, g.GC.ID
		return remote.Action(ctx, c.graphHome(), plan, nil)
	}
	if g.GC.Phase == "prune" || g.GC.Phase == "collect" {
		base.Operation = "graph_gc_" + g.GC.Phase
		base.ExpectedCursor = g.GC.Cursor
		return remote.Action(ctx, c.graphHome(), base, nil)
	}
	ids := pendingGCPartitions(g, c.config.Protocol)
	var roots map[uint8][]partition.Root
	if g.GC.Phase == "starting" {
		var err error
		roots, err = c.App.partitionStore().GCRootPlan(ctx, g, ids)
		if err != nil {
			return err
		}
	}
	actions := make([]partition.Action, 0, len(ids))
	for _, id := range ids {
		action := base
		action.Partition = id
		action.Epoch = g.Partitions[id].Epoch
		if g.GC.Phase == "starting" {
			action.Roots = roots[id]
			action.Operation = "data_gc_begin"
		} else {
			action.Operation = "data_gc_step"
			if g.GC.Phase == "cancel" {
				action.Purpose = "cancel"
			}
		}
		actions = append(actions, action)
	}
	states, err := partition.ExecuteFragmentActions[partition.DataState](ctx, remote, g, actions, c.config.Protocol >= partition.BatchProtocol)
	if err != nil {
		return err
	}
	base.Operation = "graph_gc_done"
	if g.GC.Phase == "starting" {
		base.Operation = "graph_gc_started"
	}
	for index, id := range ids {
		if g.GC.Phase == "starting" || states[index].GC == nil {
			base.Participants = append(base.Participants, id)
		}
	}
	if len(base.Participants) == 0 {
		return nil
	}
	return remote.Action(ctx, c.graphHome(), base, nil)
}

func pendingGCPartitions(g partition.Graph, protocol int) []uint8 {
	limit := partition.MaxFragmentBatch
	var perGroup map[string]int
	starting := g.GC.Phase == "starting"
	if protocol >= partition.GCSchedulingProtocol && (g.GC.Phase == "running" || starting && len(g.GC.Versions) == 1) {
		// Startup can install one full Raft batch per group for a single
		// protected view. Multiple views can carry a larger root payload.
		// Running GC still splits each group into bounded sweep entries.
		limit *= 2
		perGroup = make(map[string]int)
	}
	processed := g.GC.Done[:]
	if starting {
		processed = g.GC.Started[:]
	}
	ids := make([]uint8, 0, limit)
	for i, placement := range g.Partitions {
		if processed[i] {
			continue
		}
		if perGroup != nil {
			if perGroup[placement.Group] == partition.MaxFragmentBatch {
				continue
			}
			perGroup[placement.Group]++
		}
		ids = append(ids, uint8(i))
		if len(ids) == limit {
			break
		}
	}
	return ids
}

func (c *Cluster) registerPartitionGC(mux *http.ServeMux) {
	mux.HandleFunc("GET /cluster/graph/gc-roots/{tenant}/{partition}", func(w http.ResponseWriter, r *http.Request) {
		if c.config.Protocol < 5 || c.App.ShardID == "" {
			http.Error(w, "graph GC requires a protocol-5 data group", http.StatusConflict)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		id, err := strconv.ParseUint(r.PathValue("partition"), 10, 8)
		if err != nil {
			http.Error(w, "invalid partition", http.StatusBadRequest)
			return
		}
		c.App.mu.RLock()
		roots, err := c.App.partitionStore().GCRoots(r.Context(), r.PathValue("tenant"), r.URL.Query().Get("gc"), uint8(id))
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(roots)
	})
}
