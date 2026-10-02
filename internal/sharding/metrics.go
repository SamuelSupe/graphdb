package sharding

import (
	"crypto/subtle"
	"io"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
)

func (c *Client) localStatus() map[string]any {
	c.mu.Lock()
	entries, valid := len(c.leaders), 0
	now := time.Now()
	for _, cached := range c.leaders {
		if now.Before(cached.expires) {
			valid++
		}
	}
	c.mu.Unlock()
	return map[string]any{"leader_cache_entries": entries, "leader_cache_valid_entries": valid, "last_discovery_timestamp_seconds": float64(c.lastDiscovery.Load()) / 1e9}
}

func (c *Client) WriteMetrics(w io.Writer) {
	c.metrics.WritePrometheus(w, "graphdb_sharding_client")
	status := c.localStatus()
	observability.WriteScalar(w, "graphdb_sharding_client_leader_cache_entries", "Cached shard leaders including expired entries, bounded at 1024.", "gauge", float64(status["leader_cache_entries"].(int)))
	observability.WriteScalar(w, "graphdb_sharding_client_leader_cache_valid_entries", "Leader cache entries within their local TTL; not a quorum guarantee.", "gauge", float64(status["leader_cache_valid_entries"].(int)))
	observability.WriteScalar(w, "graphdb_sharding_client_last_discovery_timestamp_seconds", "Last successful network leader discovery in this process, zero if never.", "gauge", status["last_discovery_timestamp_seconds"].(float64))
}

func (r *Router) localStatus() map[string]any {
	r.mu.Lock()
	entries, valid := len(r.placements), 0
	now := time.Now()
	for _, cached := range r.placements {
		if now.Before(cached.expires) {
			valid++
		}
	}
	r.mu.Unlock()
	return map[string]any{"deployment": "sharded_raft", "component": "router", "checked_at": now.UTC(), "build": buildinfo.Current(), "draining": r.draining.Load(), "placement_cache_entries": entries, "placement_cache_valid_entries": valid, "placement_cache_retained_entries": r.retainedRoutes(), "catalog_unavailable": r.catalogUnavailable.Load(), "catalog_last_success_timestamp_seconds": float64(r.catalogLastSuccess.Load()) / 1e9, "client": r.Client.localStatus()}
}

func (r *Router) diagnostics(w http.ResponseWriter, request *http.Request) {
	if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+r.Client.Token)) != 1 {
		http.Error(w, "router diagnostics requires the router token", http.StatusUnauthorized)
		return
	}
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "diagnostics requires GET", http.StatusMethodNotAllowed)
		return
	}
	if request.URL.Path == "/v1/diagnostics" {
		r.writeJSON(w, http.StatusOK, r.localStatus())
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	observability.WriteRuntimeMetrics(w)
	r.metrics.WritePrometheus(w, "graphdb_router")
	r.Client.WriteMetrics(w)
	status := r.localStatus()
	draining := 0.
	if r.draining.Load() {
		draining = 1
	}
	observability.WriteScalar(w, "graphdb_router_draining", "Router has entered drain mode.", "gauge", draining)
	observability.WriteScalar(w, "graphdb_router_placement_cache_entries", "Cached tenant placements including expired entries, bounded at 4096.", "gauge", float64(status["placement_cache_entries"].(int)))
	observability.WriteScalar(w, "graphdb_router_placement_cache_valid_entries", "Placement cache entries within their local TTL; not a cluster freshness guarantee.", "gauge", float64(status["placement_cache_valid_entries"].(int)))
	observability.WriteScalar(w, "graphdb_router_placement_cache_retained_entries", "Known routes within the bounded catalog-outage fallback window; data-group fencing still applies.", "gauge", float64(status["placement_cache_retained_entries"].(int)))
	unavailable := 0.
	if r.catalogUnavailable.Load() {
		unavailable = 1
	}
	observability.WriteScalar(w, "graphdb_router_catalog_unavailable", "Last catalog read observed a transient failure, not a real-time quorum proof.", "gauge", unavailable)
	observability.WriteScalar(w, "graphdb_router_catalog_last_success_timestamp_seconds", "Last successful catalog read in this process, zero if never.", "gauge", status["catalog_last_success_timestamp_seconds"].(float64))
}
