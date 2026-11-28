package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (c *Cluster) runCatalogWorker(ctx context.Context, interval, timeout time.Duration, work func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operationCtx, cancel := ctx, func() {}
			if timeout > 0 {
				operationCtx, cancel = context.WithTimeout(ctx, timeout)
			}
			barrier, endBarrier := context.WithTimeout(operationCtx, 3*time.Second)
			err := c.Node.ReadBarrier(barrier)
			endBarrier()
			if err == nil {
				_ = work(operationCtx)
			}
			cancel()
		}
	}
}

func (c *Cluster) observedCatalog(ctx context.Context) (sharding.Catalog, []string, error) {
	c.App.mu.RLock()
	state, err := c.App.catalog(ctx)
	if err == nil {
		c.App.catalogObservation.Store(summarizeCatalog(state))
	}
	c.App.mu.RUnlock()
	if err != nil {
		return state, nil, err
	}
	ids := make([]string, 0, len(state.Tenants))
	for id := range state.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return state, ids, nil
}

func (c *Cluster) advanceShardAssignments(ctx context.Context) error {
	state, ids, err := c.observedCatalog(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		placement := state.Tenants[id]
		if placement.State != "assigning" {
			continue
		}
		finish := c.metrics.Start("tenant_assignment")
		err := c.shards.JSON(ctx, state.Shards[placement.Shard], http.MethodPost, "/cluster/action", sharding.Action{Operation: "own", Tenant: id, Epoch: placement.Epoch}, nil)
		finish(err)
		if err != nil {
			continue
		}
		_, err = c.shardAction(ctx, sharding.Action{Operation: "assigned", Tenant: id, Epoch: placement.Epoch})
		return err
	}
	return nil
}

func (c *Cluster) advanceShardMigration(ctx context.Context) error {
	state, ids, err := c.observedCatalog(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		p := state.Tenants[id]
		if p.State != "partitioning" {
			continue
		}
		groups := map[string]partition.Group{}
		for _, id := range p.GraphGroups {
			g := state.Shards[id]
			groups[id] = partition.Group{ID: g.ID, ClusterID: g.ClusterID, Peers: g.Peers}
		}
		input := partition.EnableRequest{Tenant: p.Tenant, Epoch: p.Epoch, Groups: groups}
		home := state.Shards[p.Shard]
		managed := false
		if c.config.Protocol >= partition.ConversionCancellationProtocol {
			var identity struct {
				Protocol int `json:"protocol_version"`
			}
			probe, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := c.shards.JSON(probe, home, http.MethodGet, "/cluster/identity", nil, &identity)
			cancel()
			if err != nil {
				continue
			}
			managed = identity.Protocol >= partition.ConversionCancellationProtocol
			if managed {
				input.Attempt, input.GenerationFloor = p.PartitionAttempt, p.GraphGenerationFloor
			} else if p.GraphGenerationFloor != 0 {
				continue
			}
		}
		action := sharding.Action{Tenant: p.Tenant, Epoch: p.Epoch, PartitionAttempt: p.PartitionAttempt}
		var descriptor partition.Graph
		err := c.shards.JSON(ctx, home, http.MethodPost, "/cluster/graph/enable", input, &descriptor)
		if err != nil {
			var rejected *sharding.HTTPError
			if c.config.Protocol >= partition.ConversionPreflightProtocol && errors.As(err, &rejected) && rejected.Status == http.StatusBadRequest {
				var status struct {
					Code string `json:"code"`
				}
				if json.Unmarshal([]byte(rejected.Body), &status) == nil && status.Code == errPartitionSourceValueBudget.Error() {
					action.Operation, action.Error = "partition_preflight_rejected", err.Error()
					return c.partitionCatalogAction(ctx, action)
				}
				if managed && status.Code == "bad_request" {
					action.Operation, action.Error = "cancel_partition", err.Error()
					return c.partitionCatalogAction(ctx, action)
				}
			}
			if p.PartitionError != err.Error() {
				action.Operation, action.Error = "partition_error", err.Error()
				_ = c.partitionCatalogAction(ctx, action)
			}
			continue
		}
		if descriptor.State == "active" {
			action.Operation = "partition_ready"
			return c.partitionCatalogAction(ctx, action)
		}
	}
	for _, id := range ids {
		placement := state.Tenants[id]
		if placement.Move == nil {
			continue
		}
		err := c.advanceTenantMove(ctx, state, placement)
		if err != nil {
			if placement.Move.Error != err.Error() {
				_, _ = c.shardAction(ctx, sharding.Action{Operation: "move_error", Tenant: id, MoveID: placement.Move.ID, Phase: placement.Move.Phase, Error: err.Error()})
			}
			continue
		}
		return nil
	}
	return nil
}

func (c *Cluster) syncPartitionGroupTopology(ctx context.Context) error {
	state, ids, err := c.observedCatalog(ctx)
	if err != nil {
		return err
	}
	remote := sharding.GraphRemote{Client: c.shards}
	var syncErr error
	for _, tenant := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		placement := state.Tenants[tenant]
		if placement.Mode != "partitioned" && placement.State != "partitioning" {
			continue
		}
		shard := state.Shards[placement.Shard]
		home := partition.Group{ID: shard.ID, ClusterID: shard.ClusterID, Peers: shard.Peers}
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		g, err := remote.Describe(call, home, tenant)
		if err == nil {
			groups := changedPartitionGroups(g, state.Shards)
			if len(groups) != 0 {
				err = remote.Action(call, home, partition.Action{
					Operation: "sync_groups", Tenant: tenant, Generation: g.Generation,
					CatalogVersion: state.Version, Groups: groups,
				}, nil)
			}
			if err == nil && c.config.Protocol >= partition.GroupTopologyProtocol &&
				placement.Mode == "partitioned" && placement.State == "active" && placement.ConversionCleanup == nil && g.State == "active" && g.Move == nil {
				used := map[string]bool{placement.Shard: true}
				for _, part := range g.Partitions {
					used[part.Group] = true
				}
				for _, groupID := range placement.GraphGroups {
					if !used[groupID] && state.Shards[groupID].Draining {
						fenceGroups := map[string]partition.Group{}
						if _, ok := g.Groups[home.ID]; ok {
							fenceGroups[home.ID] = home
						}
						fenceErr := remote.Action(call, home, partition.Action{
							Operation:      "sync_groups",
							Tenant:         tenant,
							Generation:     g.Generation,
							CatalogVersion: state.Version,
							Groups:         fenceGroups,
							Target:         &partition.Group{ID: groupID},
						}, nil)
						if fenceErr == nil {
							_, _ = c.shardAction(ctx, sharding.Action{
								Operation:      "prune_graph_group",
								Tenant:         tenant,
								Epoch:          placement.Epoch,
								Target:         groupID,
								CatalogVersion: state.Version,
							})
						}
					}
				}
			}
		}
		cancel()
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			syncErr = err
		}
	}
	return syncErr
}

func changedPartitionGroups(g partition.Graph, shards map[string]sharding.Shard) map[string]partition.Group {
	changed := map[string]partition.Group{}
	check := func(group partition.Group) {
		current, ok := shards[group.ID]
		if ok && current.ClusterID == group.ClusterID && !maps.Equal(current.Peers, group.Peers) {
			changed[group.ID] = partition.Group{ID: current.ID, ClusterID: current.ClusterID, Peers: current.Peers}
		}
	}
	for _, group := range g.Groups {
		check(group)
	}
	if g.Move != nil {
		check(g.Move.Source)
		check(g.Move.Target)
	}
	return changed
}

func (c *Cluster) advanceTenantMove(ctx context.Context, state sharding.Catalog, p sharding.Placement) (err error) {
	operation := "migration_other"
	switch p.Move.Phase {
	case "copy", "activate", "cleanup", "cancel", "discard":
		operation = "migration_" + p.Move.Phase
	}
	finish := c.metrics.Start(operation)
	defer func() { finish(err) }()
	m := p.Move
	source, target := state.Shards[m.Source], state.Shards[m.Target]
	call := func(shard sharding.Shard, operation string, epoch uint64) error {
		callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		return c.shards.JSON(callCtx, shard, http.MethodPost, "/cluster/action", sharding.Action{Operation: operation, Tenant: p.Tenant, MoveID: m.ID, Epoch: epoch}, nil)
	}
	switch m.Phase {
	case "copy":
		if err := call(source, "freeze", m.Epoch-1); err != nil {
			return err
		}
		var owner sharding.Ownership
		reserveCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := c.shards.JSON(reserveCtx, target, http.MethodPost, "/cluster/action", sharding.Action{Operation: "reserve", Tenant: p.Tenant, MoveID: m.ID, Epoch: m.Epoch}, &owner)
		cancel()
		if err != nil {
			return err
		}
		if owner.State == "installed" {
			return c.catalogStep(ctx, p, "cutover")
		}
		return c.copyTenantMove(ctx, source, target, p, owner)
	case "activate":
		if err := call(target, "activate", m.Epoch); err != nil {
			return err
		}
		return c.catalogStep(ctx, p, "activated")
	case "cleanup":
		if err := call(source, "retire", m.Epoch-1); err != nil {
			return err
		}
		return c.catalogStep(ctx, p, "cleaned")
	case "cancel":
		if err := call(source, "thaw", m.Epoch); err != nil {
			return err
		}
		return c.catalogStep(ctx, p, "thawed")
	case "discard":
		if err := call(target, "discard", m.Epoch); err != nil {
			return err
		}
		return c.catalogStep(ctx, p, "cancelled")
	default:
		return fmt.Errorf("unknown tenant migration phase %q", m.Phase)
	}
}
