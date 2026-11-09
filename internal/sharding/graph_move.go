package sharding

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (r *Router) graphMove(w http.ResponseWriter, request *http.Request, state Catalog) bool {
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/v1/cluster/graphs/"), "/")
	if len(parts) != 4 || parts[1] != "partitions" || parts[3] != "move" && parts[3] != "cancel" {
		return false
	}
	tenant := parts[0]
	id, err := strconv.ParseUint(parts[2], 10, 8)
	if storage.ValidateTenantID(tenant) != nil || err != nil {
		r.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid tenant or partition ID"})
		return true
	}
	p, ok := state.Tenants[tenant]
	if !ok || p.Mode != "partitioned" || p.State != "active" {
		r.writeJSON(w, http.StatusConflict, map[string]any{"error": "partition migration requires an active partitioned graph"})
		return true
	}
	remote := GraphRemote{Client: r.Client}
	home := groupDescriptor(state.Shards[p.Shard])
	g, err := remote.Describe(request.Context(), home, tenant)
	if err != nil {
		r.writeError(w, err)
		return true
	}
	if request.Method == http.MethodGet {
		r.writeJSON(w, http.StatusOK, g.Move)
		return true
	}
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	var input struct {
		Target string `json:"target_group"`
		MoveID string `json:"move_id"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil {
		r.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid partition move request"})
		return true
	}
	action := partition.Action{Tenant: tenant, Generation: g.Generation, Partition: uint8(id)}
	if parts[3] == "cancel" {
		if g.Move == nil || g.Move.ID != input.MoveID || g.Move.Partition != uint8(id) {
			r.writeJSON(w, http.StatusConflict, map[string]any{"error": "partition move identity changed"})
			return true
		}
		action.Operation = "graph_move_cancel"
		action.Txn = g.Move.ID
		action.Phase = g.Move.Phase
		action.ExpectedCursor = g.Move.Cursor
	} else {
		if g.Move != nil && g.Move.Partition == uint8(id) && g.Move.Target.ID == input.Target {
			r.writeJSON(w, http.StatusAccepted, g.Move)
			return true
		}
		target, ok := state.Shards[input.Target]
		if !ok || target.Draining || input.Target == g.Partitions[id].Group {
			r.writeJSON(w, http.StatusConflict, map[string]any{"error": "choose a different registered active target group"})
			return true
		}
		var identity struct {
			Format int `json:"graph_partition_format"`
		}
		if err := r.Client.JSON(request.Context(), target, http.MethodGet, "/cluster/identity", nil, &identity); err != nil {
			r.writeError(w, err)
			return true
		}
		if identity.Format != partition.FormatVersion {
			r.writeJSON(w, http.StatusConflict, map[string]any{"error": "target group requires Raft protocol 5"})
			return true
		}
		if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodPost, "/cluster/action", Action{Operation: "graph_group", Tenant: tenant, Epoch: p.Epoch, Target: input.Target}, nil); err != nil {
			r.writeError(w, err)
			return true
		}
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			r.writeError(w, err)
			return true
		}
		targetGroup := groupDescriptor(target)
		action.Operation = "graph_move_begin"
		action.Txn = hex.EncodeToString(random[:])
		action.Target = &targetGroup
	}
	var updated partition.Graph
	if err := remote.Action(request.Context(), home, action, &updated); err != nil {
		r.writeError(w, err)
		return true
	}
	r.writeJSON(w, http.StatusAccepted, updated.Move)
	return true
}
