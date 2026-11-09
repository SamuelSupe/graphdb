package sharding

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (r *Router) graphGC(w http.ResponseWriter, request *http.Request, state Catalog) bool {
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/v1/cluster/graphs/"), "/")
	if len(parts) != 2 || parts[1] != "gc" {
		return false
	}
	tenant := parts[0]
	p, ok := state.Tenants[tenant]
	if storage.ValidateTenantID(tenant) != nil || !ok || p.Mode != "partitioned" || p.State != "active" {
		r.writeJSON(w, http.StatusConflict, map[string]any{"error": "GC requires an active partitioned graph"})
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
		r.writeJSON(w, http.StatusOK, g.GC)
		return true
	}
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	if g.GC != nil {
		r.writeJSON(w, http.StatusAccepted, g.GC)
		return true
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		r.writeError(w, err)
		return true
	}
	var updated partition.Graph
	if err := remote.Action(request.Context(), home, partition.Action{Operation: "graph_gc_begin", Tenant: tenant, Generation: g.Generation, Txn: hex.EncodeToString(random[:])}, &updated); err != nil {
		r.writeError(w, err)
		return true
	}
	r.writeJSON(w, http.StatusAccepted, updated.GC)
	return true
}
