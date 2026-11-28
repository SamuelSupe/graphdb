package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (c *Cluster) registerPartitionConversionCancellation(mux *http.ServeMux) {
	mux.HandleFunc("POST /cluster/graph/conversion-cancel", func(w http.ResponseWriter, r *http.Request) {
		var input partition.EnableRequest
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input) != nil || storage.ValidateTenantID(input.Tenant) != nil || input.Epoch == 0 || input.GenerationFloor < 0 {
			http.Error(w, "invalid graph conversion cancellation", http.StatusBadRequest)
			return
		}
		if c.config.Protocol < partition.ConversionCancellationProtocol || c.App.ShardID == "" {
			http.Error(w, "conversion cancellation requires data Raft protocol 13", http.StatusConflict)
			return
		}
		if err := c.Node.ReadBarrier(r.Context()); err != nil {
			c.writeError(w, err)
			return
		}
		generation, err := c.cancellationGeneration(r.Context(), input)
		if err == nil {
			var data []byte
			data, err = c.partitionAction(r.Context(), partition.Action{Operation: "cancel_conversion", Tenant: input.Tenant, Generation: generation, Epoch: input.Epoch, ConversionAttempt: input.Attempt})
			if err == nil {
				writeResult(w, data)
				return
			}
		}
		c.partitionError(w, err)
	})
	mux.HandleFunc("POST /cluster/graph/conversion-reclaim", func(w http.ResponseWriter, r *http.Request) {
		var input partition.Action
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input) != nil || storage.ValidateTenantID(input.Tenant) != nil || input.Generation <= 0 {
			http.Error(w, "invalid conversion reclamation scope", http.StatusBadRequest)
			return
		}
		if c.config.Protocol < partition.ConversionCancellationProtocol || c.App.ShardID == "" {
			http.Error(w, "conversion reclamation requires data Raft protocol 13", http.StatusConflict)
			return
		}
		data, err := c.partitionAction(r.Context(), partition.Action{Operation: "reclaim_conversion", Tenant: input.Tenant, Generation: input.Generation})
		if err != nil {
			c.partitionError(w, err)
			return
		}
		writeResult(w, data)
	})
}

func (c *Cluster) cancellationGeneration(ctx context.Context, input partition.EnableRequest) (int64, error) {
	c.App.mu.RLock()
	defer c.App.mu.RUnlock()
	store := c.App.partitionStore()
	last, err := store.LastConversionCancellation(ctx, input.Tenant)
	if err != nil {
		return 0, err
	}
	if last.Generation > 0 && last.Attempt == input.Attempt {
		return last.Generation, nil
	}
	if g, err := store.GetGraph(ctx, input.Tenant); err == nil {
		return g.Generation, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return 0, err
	}
	source, err := c.App.Store.ReplicationTenantGeneration(ctx, input.Tenant)
	if err != nil {
		return 0, err
	}
	floor := max(source, input.GenerationFloor, last.Generation)
	if floor == math.MaxInt64 {
		return 0, fmt.Errorf("%w: graph generations exhausted", storage.ErrConflict)
	}
	return floor + 1, nil
}

func (c *Cluster) advanceConversionCancellations(ctx context.Context) error {
	state, ids, err := c.observedCatalog(ctx)
	if err != nil {
		return err
	}
	var pendingErr error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := state.Tenants[id]
		if p.State == "partition_cancelling" {
			call, cancel := context.WithTimeout(ctx, 3*time.Second)
			var decision partition.ConversionCancellation
			err := c.shards.JSON(call, state.Shards[p.Shard], http.MethodPost, "/cluster/graph/conversion-cancel", partition.EnableRequest{
				Tenant: p.Tenant, Epoch: p.Epoch, Attempt: p.PartitionAttempt, GenerationFloor: p.GraphGenerationFloor,
			}, &decision)
			cancel()
			if err != nil {
				pendingErr = err
				continue
			}
			if decision.Attempt != p.PartitionAttempt || decision.Generation <= 0 {
				pendingErr = fmt.Errorf("mismatched graph conversion decision for %s", p.Tenant)
				continue
			}
			action := sharding.Action{Operation: "partition_cancelled", Tenant: p.Tenant, Epoch: p.Epoch, PartitionAttempt: p.PartitionAttempt, GraphGeneration: decision.Generation}
			if decision.Activated {
				action.Operation = "partition_cancel_lost"
			}
			if err := c.partitionCatalogAction(ctx, action); err != nil {
				pendingErr = err
			}
		}
	}
	for _, id := range ids {
		p := state.Tenants[id]
		if p.ConversionCleanup == nil {
			continue
		}
		for _, group := range p.ConversionCleanup.Groups {
			if err := ctx.Err(); err != nil {
				return err
			}
			call, cancel := context.WithTimeout(ctx, 3*time.Second)
			var result struct {
				Done bool `json:"done"`
			}
			err := c.shards.JSON(call, state.Shards[group], http.MethodPost, "/cluster/graph/conversion-reclaim", partition.Action{Tenant: p.Tenant, Generation: p.ConversionCleanup.Generation}, &result)
			cancel()
			if err != nil {
				pendingErr = err
				continue
			}
			if result.Done {
				err := c.partitionCatalogAction(ctx, sharding.Action{Operation: "partition_cleanup_group", Tenant: p.Tenant, Target: group, GraphGeneration: p.ConversionCleanup.Generation})
				if err != nil {
					pendingErr = err
				}
			}
		}
	}
	return pendingErr
}

func (c *Cluster) partitionCatalogAction(ctx context.Context, action sharding.Action) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	data, err := c.shardAction(ctx, action)
	if err != nil {
		return err
	}
	var result httpResult
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if result.Status != http.StatusOK {
		return fmt.Errorf("catalog action %s rejected with status %d: %s", action.Operation, result.Status, result.Body)
	}
	return nil
}
