package ha

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"golang.org/x/sync/errgroup"
)

func (c *Cluster) advancePartitionGC(ctx context.Context, g partition.Graph) error {
	if g.GC == nil {
		return nil
	}
	remote := sharding.GraphRemote{Client: c.shards}
	base := partition.Action{Tenant: g.Tenant, Generation: g.Generation, Txn: g.GC.ID}
	if g.GC.Phase == "prune" {
		base.Operation = "graph_gc_prune"
		base.ExpectedCursor = g.GC.Cursor
		return remote.Action(ctx, c.graphHome(), base, nil)
	}
	var ids []uint8
	for i := range g.Partitions {
		pending := !g.GC.Done[i]
		if g.GC.Phase == "starting" {
			pending = !g.GC.Started[i]
		}
		if pending {
			ids = append(ids, uint8(i))
			if len(ids) == 8 {
				break
			}
		}
	}
	workers, workerCtx := errgroup.WithContext(ctx)
	workers.SetLimit(4)
	completed := make([]bool, len(ids))
	for index, id := range ids {
		workers.Go(func() error {
			group, err := g.GroupFor(id)
			if err != nil {
				return err
			}
			action := base
			action.Partition = id
			action.Epoch = g.Partitions[id].Epoch
			var state partition.DataState
			if g.GC.Phase == "starting" {
				if err := c.shards.JSON(workerCtx, partitionShard(c.graphHome()), http.MethodGet, "/cluster/graph/gc-roots/"+url.PathEscape(g.Tenant)+"/"+strconv.Itoa(int(id))+"?gc="+url.QueryEscape(g.GC.ID), nil, &action.Roots); err != nil {
					return err
				}
				action.Operation = "data_gc_begin"
			} else {
				action.Operation = "data_gc_step"
			}
			if err := remote.Action(workerCtx, group, action, &state); err != nil {
				return err
			}
			completed[index] = g.GC.Phase == "starting" || state.GC == nil
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}
	base.Operation = "graph_gc_done"
	if g.GC.Phase == "starting" {
		base.Operation = "graph_gc_started"
	}
	for index, id := range ids {
		if completed[index] {
			base.Participants = append(base.Participants, id)
		}
	}
	if len(base.Participants) == 0 {
		return nil
	}
	return remote.Action(ctx, c.graphHome(), base, nil)
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
