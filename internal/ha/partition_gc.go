package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	c.App.mu.RLock()
	err := c.App.partitionStore().PlanGCSweep(ctx, a, state)
	c.App.mu.RUnlock()
	finish(err)
	return err
}

func (c *Cluster) recoverPartitionSnapshotBudget(ctx context.Context, g partition.Graph, blocked error) error {
	if g.GC != nil && g.GC.Phase != "collect" {
		_, err := c.drivePartitionGC(ctx, g)
		return err
	}
	if c.config.Protocol < partition.RecoveryGCProtocol || g.Active == nil || g.Active.State != "preparing" ||
		(g.State != "active" && g.State != "initializing") || g.Move != nil || g.LastRecoveryGCSequence == g.Active.Sequence {
		return blocked
	}
	workers, workCtx := errgroup.WithContext(ctx)
	workers.SetLimit(4)
	seen := map[string]bool{c.App.ShardID: true}
	for _, placement := range g.Partitions {
		if seen[placement.Group] {
			continue
		}
		seen[placement.Group] = true
		group := g.Groups[placement.Group]
		workers.Go(func() error {
			shard := sharding.Shard{ID: group.ID, ClusterID: group.ClusterID, Peers: group.Peers}
			var identity struct {
				Shard    string `json:"shard_id"`
				Cluster  string `json:"cluster_id"`
				Protocol int    `json:"protocol_version"`
			}
			if err := c.shards.JSON(workCtx, shard, http.MethodGet, "/cluster/identity", nil, &identity); err != nil {
				return err
			}
			if identity.Shard != group.ID || identity.Cluster != group.ClusterID || identity.Protocol < partition.RecoveryGCProtocol {
				return fmt.Errorf("snapshot recovery GC requires protocol %d on graph group %s", partition.RecoveryGCProtocol, group.ID)
			}
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return errors.Join(blocked, err)
	}
	id := ""
	if g.GC != nil {
		id = g.GC.ID
	} else {
		cmd, err := newCommand("graph_gc")
		if err != nil {
			return err
		}
		id = cmd.ID
	}
	remote := sharding.GraphRemote{Client: c.shards}
	return remote.Action(ctx, c.graphHome(), partition.Action{Operation: "graph_gc_begin", Tenant: g.Tenant,
		Generation: g.Generation, Txn: id, Sequence: g.Active.Sequence}, nil)
}

func (c *Cluster) drivePartitionGC(ctx context.Context, g partition.Graph) (partition.Graph, error) {
	maintenance, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		if err := c.advancePartitionGC(maintenance, g); err != nil {
			return g, err
		}
		next, err := c.App.graphDescriptor(maintenance, g.Tenant)
		if err != nil || c.config.Protocol < partition.BufferedGCProtocol || next.GC == nil || next.GC.Phase != "starting" {
			return next, err
		}
		// Keep installing bounded waves while the startup fence blocks writes.
		g = next
	}
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
		c.App.mu.RLock()
		plan, err := c.App.partitionStore().InputGCPlan(ctx, g)
		c.App.mu.RUnlock()
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
		c.App.mu.RLock()
		roots, err = c.App.partitionStore().GCRootPlan(ctx, g, ids)
		c.App.mu.RUnlock()
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
			action.Sequence = g.GC.RecoverySequence
		} else {
			action.Operation = "data_gc_step"
			if g.GC.Phase == "cancel" {
				action.Purpose = "cancel"
				action.Sequence = g.GC.RecoverySequence
			}
		}
		actions = append(actions, action)
	}
	states, err := partition.ExecuteFragmentActions[json.RawMessage](ctx, remote, g, actions, c.config.Protocol >= partition.BatchProtocol)
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	base.Operation = "graph_gc_done"
	if g.GC.Phase == "starting" {
		base.Operation = "graph_gc_started"
	}
	for index, reply := range states {
		// Batched fragments return a compact GC marker, or JSON null when
		// complete. A nil RawMessage means this fragment never replied.
		if len(reply) == 0 {
			continue
		}
		var state partition.DataState
		if decodeErr := json.Unmarshal(reply, &state); decodeErr != nil {
			err = errors.Join(err, decodeErr)
			continue
		}
		if g.GC.Phase == "starting" || state.GC == nil {
			base.Participants = append(base.Participants, ids[index])
		}
	}
	if len(base.Participants) == 0 {
		return err
	}
	return errors.Join(err, remote.Action(ctx, c.graphHome(), base, nil))
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
