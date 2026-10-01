package ha

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

func (c *Cluster) runShardMigrations(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operationCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			if c.Node.ReadBarrier(operationCtx) == nil {
				_ = c.advanceShardMigration(operationCtx)
			}
			cancel()
		}
	}
}

func (c *Cluster) advanceShardMigration(ctx context.Context) error {
	c.App.mu.RLock()
	state, err := c.App.catalog(ctx)
	c.App.mu.RUnlock()
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(state.Tenants))
	for id := range state.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		placement := state.Tenants[id]
		if placement.State != "assigning" {
			continue
		}
		err := c.shards.JSON(ctx, state.Shards[placement.Shard], http.MethodPost, "/cluster/action", sharding.Action{Operation: "own", Tenant: id, Epoch: placement.Epoch}, nil)
		if err != nil {
			continue
		}
		_, err = c.shardAction(ctx, sharding.Action{Operation: "assigned", Tenant: id, Epoch: placement.Epoch})
		return err
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

func (c *Cluster) advanceTenantMove(ctx context.Context, state sharding.Catalog, p sharding.Placement) error {
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
		response, err := c.shards.Do(ctx, source, http.MethodPost, "/cluster/export", sharding.Action{Tenant: p.Tenant, MoveID: m.ID, Epoch: m.Epoch - 1})
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, c.App.MaxSnapshotBytes+1))
		response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if response.StatusCode >= 400 {
			return &sharding.HTTPError{Status: response.StatusCode, Body: string(data)}
		}
		if int64(len(data)) > c.App.MaxSnapshotBytes {
			return fmt.Errorf("migration exceeds the catalog transfer budget")
		}
		parts := (len(data) + sharding.ChunkBytes - 1) / sharding.ChunkBytes
		digest := fmt.Sprintf("%x", sha256.Sum256(data))
		if owner.NextPart > parts || (owner.Digest != "" && owner.Digest != digest) {
			return fmt.Errorf("migration source changed after transfer began")
		}
		// Make bounded progress per pass. The target's replicated ownership
		// checkpoint survives lost responses and either coordinator's restart.
		end := min(parts, owner.NextPart+8)
		for part := owner.NextPart; part < end; part++ {
			action := sharding.Action{Operation: "stage", Tenant: p.Tenant, MoveID: m.ID, Epoch: m.Epoch, Part: part, Digest: digest, Data: data[part*sharding.ChunkBytes : min((part+1)*sharding.ChunkBytes, len(data))]}
			stageCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			err := c.shards.JSON(stageCtx, target, http.MethodPost, "/cluster/action", action, nil)
			cancel()
			if err != nil {
				return err
			}
		}
		if end < parts {
			return nil
		}
		action := sharding.Action{Operation: "install", Tenant: p.Tenant, MoveID: m.ID, Epoch: m.Epoch, Parts: parts, Bytes: int64(len(data)), Digest: digest}
		if err := c.shards.JSON(ctx, target, http.MethodPost, "/cluster/action", action, nil); err != nil {
			return err
		}
		// Only the committed catalog transition permits destination activation.
		// A superseded/cancelled coordinator cannot activate its old destination.
		return c.catalogStep(ctx, p, "cutover")
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
