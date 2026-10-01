package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) catalog(ctx context.Context) (sharding.Catalog, error) {
	state := sharding.Catalog{Shards: make(map[string]sharding.Shard), Tenants: make(map[string]sharding.Placement)}
	data, err := a.Files.Get(ctx, path.Join(a.Store.Prefix, "control/sharding/catalog.json"))
	if errors.Is(err, storage.ErrNotFound) {
		return state, nil
	}
	if err == nil {
		err = json.Unmarshal(data, &state)
	}
	return state, err
}

func (a *Application) applyCatalog(ctx context.Context, action sharding.Action) ([]byte, error) {
	state, err := a.catalog(ctx)
	if err != nil {
		return nil, err
	}
	conflict := func(message string) ([]byte, error) {
		return resultJSON(http.StatusConflict, map[string]any{"code": "shard_conflict", "error": message})
	}
	var result any
	if action.Operation == "register" {
		if action.Shard == nil || action.Shard.Validate() != nil {
			return conflict("a valid shard definition is required")
		}
		if previous, ok := state.Shards[action.Shard.ID]; ok {
			if previous.ClusterID != action.Shard.ClusterID {
				return conflict("a shard's Raft cluster identity is immutable")
			}
			updated := *action.Shard
			updated.Draining = previous.Draining
			state.Shards[action.Shard.ID] = updated
		} else {
			for _, existing := range state.Shards {
				if existing.ClusterID == action.Shard.ClusterID {
					return conflict("a Raft cluster can only represent one shard")
				}
			}
			state.Shards[action.Shard.ID] = *action.Shard
		}
		result = state.Shards[action.Shard.ID]
	} else if action.Operation == "drain" || action.Operation == "resume" || action.Operation == "unregister" {
		shard, exists := state.Shards[action.Target]
		if !exists {
			if action.Operation == "unregister" {
				return resultJSON(http.StatusOK, map[string]any{"id": action.Target, "removed": true})
			}
			return conflict("shard is not registered")
		}
		if action.Operation == "unregister" {
			if !shard.Draining {
				return conflict("drain the shard before unregistering it")
			}
			for _, placement := range state.Tenants {
				if placement.Shard == shard.ID || (placement.Move != nil && (placement.Move.Source == shard.ID || placement.Move.Target == shard.ID)) {
					return conflict("shard still owns tenants or participates in a migration")
				}
			}
			delete(state.Shards, shard.ID)
			result = map[string]any{"id": shard.ID, "removed": true}
		} else {
			shard.Draining = action.Operation == "drain"
			state.Shards[shard.ID] = shard
			result = shard
		}
	} else {
		if err := storage.ValidateTenantID(action.Tenant); err != nil {
			return conflict(err.Error())
		}
		placement, exists := state.Tenants[action.Tenant]
		switch action.Operation {
		case "assign":
			if exists {
				if action.Target != "" && action.Target != placement.Shard {
					return conflict("tenant is already assigned; use a move operation")
				}
				return resultJSON(http.StatusOK, placement)
			}
			if action.Target == "" {
				action.Target = leastPopulatedShard(state)
			}
			if shard, ok := state.Shards[action.Target]; !ok || shard.Draining {
				return conflict("register a data shard before assigning tenants")
			}
			placement = sharding.Placement{Tenant: action.Tenant, Shard: action.Target, Epoch: 1, State: "assigning"}
		case "assigned":
			if !exists || placement.Epoch != action.Epoch || placement.State != "assigning" {
				return conflict("tenant assignment changed")
			}
			placement.State = "active"
		case "move":
			if !exists || state.Shards[action.Target].ID == "" || state.Shards[action.Target].Draining || action.Target == placement.Shard {
				return conflict("move requires an existing tenant and a different registered target shard")
			}
			if placement.Move != nil {
				if placement.Move.Target == action.Target {
					return resultJSON(http.StatusOK, placement)
				}
				return conflict("another tenant move is still in progress")
			}
			if placement.State != "active" || placement.Epoch == math.MaxUint64 {
				return conflict("tenant is not ready for migration")
			}
			placement.State = "moving"
			placement.Move = &sharding.Move{ID: action.MoveID, Source: placement.Shard, Target: action.Target, Epoch: placement.Epoch + 1, Phase: "copy"}
		case "cutover":
			if !matchesMove(placement, action, "copy") {
				return conflict("move was cancelled or superseded before cutover")
			}
			placement.Shard, placement.Epoch = placement.Move.Target, placement.Move.Epoch
			placement.State, placement.Move.Phase = "activating", "activate"
		case "activated":
			if !matchesMove(placement, action, "activate") {
				return conflict("move is not awaiting activation")
			}
			placement.State, placement.Move.Phase = "active", "cleanup"
		case "cleaned":
			if !matchesMove(placement, action, "cleanup") {
				return conflict("move is not awaiting cleanup")
			}
			placement.Move = nil
		case "cancel":
			if placement.Move == nil || placement.Move.Phase != "copy" {
				return conflict("only a move before cutover can be cancelled")
			}
			placement.State, placement.Move.Phase = "cancelling", "cancel"
		case "cancelled":
			if !matchesMove(placement, action, "discard") {
				return conflict("move is not awaiting cancellation")
			}
			placement.Epoch, placement.State = placement.Move.Epoch, "active"
			placement.Move = nil
		case "thawed":
			if !matchesMove(placement, action, "cancel") {
				return conflict("move is not awaiting source thaw")
			}
			placement.Epoch, placement.State = placement.Move.Epoch, "active"
			placement.Move.Phase = "discard"
		case "move_error":
			if placement.Move == nil || placement.Move.ID != action.MoveID || placement.Move.Phase != action.Phase {
				return conflict("move progressed before its error was recorded")
			}
			placement.Move.Error = action.Error
		default:
			return conflict("unsupported catalog operation")
		}
		if placement.Move != nil && action.Operation != "move_error" {
			placement.Move.Error = ""
		}
		state.Tenants[action.Tenant] = placement
		result = placement
	}
	state.Version++
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if err := a.Files.Put(ctx, path.Join(a.Store.Prefix, "control/sharding/catalog.json"), data); err != nil {
		return nil, err
	}
	return resultJSON(http.StatusOK, result)
}

func matchesMove(p sharding.Placement, action sharding.Action, phase string) bool {
	return p.Move != nil && p.Move.ID == action.MoveID && p.Move.Phase == phase
}

func leastPopulatedShard(state sharding.Catalog) string {
	counts := make(map[string]int)
	ids := make([]string, 0, len(state.Shards))
	for id, shard := range state.Shards {
		if !shard.Draining {
			ids = append(ids, id)
		}
	}
	for _, tenant := range state.Tenants {
		counts[tenant.Shard]++
	}
	sort.Strings(ids)
	selected := ""
	for _, id := range ids {
		if selected == "" || counts[id] < counts[selected] {
			selected = id
		}
	}
	return selected
}

func (c *Cluster) shardAction(ctx context.Context, action sharding.Action) ([]byte, error) {
	cmd, err := newCommand("sharding")
	if err != nil {
		return nil, err
	}
	if action.Operation == "move" {
		action.MoveID = cmd.ID
	}
	cmd.Body, err = json.Marshal(action)
	if err != nil {
		return nil, err
	}
	return c.propose(ctx, cmd)
}

func (c *Cluster) catalogStep(ctx context.Context, p sharding.Placement, operation string) error {
	data, err := c.shardAction(ctx, sharding.Action{Operation: operation, Tenant: p.Tenant, Epoch: p.Epoch, MoveID: p.Move.ID})
	if err != nil {
		return err
	}
	var result httpResult
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if result.Status >= 400 {
		return fmt.Errorf("catalog transition failed: %s", result.Body)
	}
	return nil
}
