package sharding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func groupDescriptor(s Shard) partition.Group {
	return partition.Group{ID: s.ID, ClusterID: s.ClusterID, Peers: s.Peers}
}

func (r *Router) serveGraph(w http.ResponseWriter, request *http.Request, resolution Resolution, tenant string) bool {
	if !graphDataSupported(request) {
		return false
	}
	remote := GraphRemote{Client: r.Client}
	home := groupDescriptor(resolution.Shard)
	release := r.Client.admitGraphRequest(w, request, home, tenant)
	if release == nil {
		return true
	}
	defer release()
	ctx, cancel := context.WithTimeout(request.Context(), partition.MaxQueryLifetime)
	defer cancel()
	g, err := remote.Describe(ctx, home, tenant)
	if err != nil {
		graphError(w, err)
		return true
	}
	return serveGraphData(w, request.WithContext(ctx), g, home, r.Client)
}

func (r *Router) graphAdmin(w http.ResponseWriter, request *http.Request) bool {
	if request.URL.Path != "/v1/cluster/graphs" && !strings.HasPrefix(request.URL.Path, "/v1/cluster/graphs/") {
		return false
	}
	var state Catalog
	if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
		r.writeError(w, err)
		return true
	}
	if r.graphGC(w, request, state) {
		return true
	}
	if r.graphMove(w, request, state) {
		return true
	}
	if request.Method == http.MethodGet {
		tenant := strings.TrimPrefix(request.URL.Path, "/v1/cluster/graphs/")
		p, ok := state.Tenants[tenant]
		if !ok {
			r.writeJSON(w, http.StatusNotFound, map[string]any{"error": "tenant placement not found"})
			return true
		}
		if p.State != "active" || p.Mode != "partitioned" {
			r.writeJSON(w, http.StatusOK, p)
			return true
		}
		g, err := (GraphRemote{Client: r.Client}).Describe(request.Context(), groupDescriptor(state.Shards[p.Shard]), tenant)
		if err != nil {
			r.writeError(w, err)
		} else {
			r.writeJSON(w, http.StatusOK, g)
		}
		return true
	}
	if request.Method != http.MethodPost || request.URL.Path != "/v1/cluster/graphs" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	var input struct {
		Tenant string   `json:"tenant_id"`
		Groups []string `json:"data_groups"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || storage.ValidateTenantID(input.Tenant) != nil || (len(input.Groups) < 2 || len(input.Groups) > 256) {
		r.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "a valid tenant and 2 to 256 data_groups are required"})
		return true
	}
	p, ok := state.Tenants[input.Tenant]
	if !ok {
		r.writeJSON(w, http.StatusNotFound, map[string]any{"error": "create the tenant before enabling graph partitions"})
		return true
	}
	for _, id := range append(append([]string(nil), input.Groups...), p.Shard) {
		group, ok := state.Shards[id]
		if !ok || group.Draining {
			r.writeJSON(w, http.StatusConflict, map[string]any{"error": "data group is unavailable or draining"})
			return true
		}
		var identity struct {
			Format int `json:"graph_partition_format"`
		}
		if err := r.Client.JSON(request.Context(), group, http.MethodGet, "/cluster/identity", nil, &identity); err != nil {
			r.writeError(w, err)
			return true
		}
		if identity.Format != partition.FormatVersion {
			r.writeJSON(w, http.StatusConflict, map[string]any{"error": "upgrade all selected groups to Raft protocol 5 before enabling graph partitions"})
			return true
		}
	}
	if err := r.Client.JSON(request.Context(), state.Shards[p.Shard], http.MethodGet, "/cluster/graph/check/"+url.PathEscape(input.Tenant), nil, nil); err != nil {
		r.writeError(w, err)
		return true
	}
	var result Placement
	if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodPost, "/cluster/action", Action{Operation: "begin_partition", Tenant: input.Tenant, Epoch: p.Epoch, GraphGroups: input.Groups}, &result); err != nil {
		r.writeError(w, err)
		return true
	}
	r.forgetPlacement(input.Tenant)
	r.writeJSON(w, http.StatusAccepted, result)
	return true
}
