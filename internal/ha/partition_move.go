package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

func partitionShard(g partition.Group) sharding.Shard {
	return sharding.Shard{ID: g.ID, ClusterID: g.ClusterID, Peers: g.Peers}
}

func (c *Cluster) advancePartitionMove(ctx context.Context, g partition.Graph) (err error) {
	m := g.Move
	if m == nil {
		return nil
	}
	remote := sharding.GraphRemote{Client: c.shards}
	control := func(a partition.Action) error {
		a.Tenant = g.Tenant
		a.Generation = g.Generation
		a.Txn = m.ID
		a.Phase = m.Phase
		a.ExpectedCursor = m.Cursor
		return remote.Action(ctx, c.graphHome(), a, nil)
	}
	defer func() {
		if err == nil {
			return
		}
		message := err.Error()
		if len(message) > 1024 {
			message = message[:1024]
		}
		action := partition.Action{Operation: "graph_move_error", Error: message}
		if (m.Phase == "capture" || m.Phase == "fence" || m.Phase == "final_copy" || m.Phase == "install") &&
			!m.FailureSince.IsZero() && time.Since(m.FailureSince) >= 30*time.Second {
			action.Operation = "graph_move_cancel"
		} else if message == m.Error {
			return
		}
		// A failed remote step may have used up its own deadline.
		report, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		original := ctx
		ctx = report
		reportErr := control(action)
		ctx = original
		if reportErr != nil {
			err = errors.Join(err, fmt.Errorf("record move failure: %w", reportErr))
		}
	}()
	data := func(group partition.Group, operation string, epoch uint64, result any) error {
		return remote.Action(ctx, group, partition.Action{Operation: operation, Tenant: g.Tenant, Generation: g.Generation, Partition: m.Partition, Epoch: epoch, Txn: m.ID, Sequence: m.Sequence, Base: m.Root}, result)
	}
	switch m.Phase {
	case "capture":
		if err := data(m.Target, "data_move_reserve", m.Epoch, nil); err != nil {
			return err
		}
		roots, err := c.App.partitionStore().MoveRoots(ctx, g, m.Partition, m.CaptureAt)
		if err != nil {
			return err
		}
		capture := partition.Action{Operation: "data_move_capture", Purpose: "reachable", Tenant: g.Tenant,
			Generation: g.Generation, Partition: m.Partition, Epoch: m.Epoch - 1, Txn: m.ID, Roots: roots}
		if err := remote.Action(ctx, m.Source, capture, nil); err != nil {
			return err
		}
		return control(partition.Action{Operation: "graph_move_progress", Purpose: "copy"})
	case "copy", "final_copy":
		var exported partition.PageExport
		purpose := ""
		nextPhase := m.Phase
		if m.Phase == "final_copy" {
			purpose = "delta"
		}
		input := partition.Action{Tenant: g.Tenant, Generation: g.Generation, Partition: m.Partition, Epoch: m.Epoch - 1, Txn: m.ID, Cursor: m.Cursor, Purpose: purpose}
		if err := c.shards.JSON(ctx, partitionShard(m.Source), http.MethodPost, "/cluster/graph/pages", input, &exported); err != nil {
			return err
		}
		if len(exported.Objects) != 0 {
			if err := remote.Action(ctx, m.Target, partition.Action{Operation: "data_move_pages", Tenant: g.Tenant, Generation: g.Generation, Partition: m.Partition, Epoch: m.Epoch, Txn: m.ID, Pages: exported.Objects}, nil); err != nil {
				return err
			}
		}
		if exported.Next == "" {
			if m.Phase == "copy" {
				nextPhase = "fence"
			} else {
				nextPhase = "install"
			}
		}
		return control(partition.Action{Operation: "graph_move_progress", Purpose: nextPhase, Cursor: exported.Next})
	case "fence":
		if err := data(m.Source, "data_move_fence", m.Epoch-1, nil); err != nil {
			return err
		}
		return control(partition.Action{Operation: "graph_move_progress", Purpose: "final_copy"})
	case "install":
		var installed partition.DataState
		if err := data(m.Target, "data_move_install", m.Epoch, &installed); err != nil {
			return err
		}
		if installed.State != "active" {
			if m.Error != "" {
				return control(partition.Action{Operation: "graph_move_error"})
			}
			return nil
		}
		return control(partition.Action{Operation: "graph_move_switch", DataState: &installed})
	case "cleanup":
		if time.Now().Before(m.RetireAfter) {
			return nil
		}
		var retired partition.DataState
		if err := data(m.Source, "data_move_retire", m.Epoch-1, &retired); err != nil {
			return err
		}
		if retired.State != "retired" {
			return nil
		}
		return control(partition.Action{Operation: "graph_move_complete"})
	case "cancel":
		var restored partition.DataState
		if err := data(m.Source, "data_move_thaw", m.Epoch-1, &restored); err != nil {
			return err
		}
		return control(partition.Action{Operation: "graph_move_cancelled", DataState: &restored})
	case "cancel_cleanup":
		var retired partition.DataState
		if err := data(m.Target, "data_move_reserve", m.Epoch, nil); err != nil {
			return err
		}
		if err := data(m.Target, "data_move_retire", m.Epoch, &retired); err != nil {
			return err
		}
		if retired.State != "retired" {
			return nil
		}
		return control(partition.Action{Operation: "graph_move_complete"})
	}
	return nil
}

func (c *Cluster) registerPartitionMove(mux *http.ServeMux) {
	mux.HandleFunc("POST /cluster/graph/pages", func(w http.ResponseWriter, r *http.Request) {
		if c.config.Protocol < 5 || c.App.ShardID == "" {
			http.Error(w, "graph partitions require a protocol-5 data group", http.StatusConflict)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		var input partition.Action
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil {
			http.Error(w, "invalid partition export", http.StatusBadRequest)
			return
		}
		c.App.mu.RLock()
		result, err := c.App.partitionStore().ExportPages(r.Context(), input)
		c.App.mu.RUnlock()
		if err != nil {
			c.partitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
}
