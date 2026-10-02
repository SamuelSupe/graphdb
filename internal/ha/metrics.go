package ha

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type queueObservation struct {
	Requests int       `json:"pending_requests"`
	Bytes    int64     `json:"pending_bytes"`
	At       time.Time `json:"observed_at"`
}

type catalogObservation struct {
	Version        uint64         `json:"version"`
	Shards         int            `json:"shards"`
	DrainingShards int            `json:"draining_shards"`
	Tenants        map[string]int `json:"tenants_by_state"`
	Moves          map[string]int `json:"moves_by_phase"`
	MoveErrors     int            `json:"moves_with_errors"`
	At             time.Time      `json:"observed_at"`
}

func summarizeCatalog(state sharding.Catalog) *catalogObservation {
	observation := &catalogObservation{Version: state.Version, Shards: len(state.Shards), Tenants: map[string]int{"active": 0, "assigning": 0, "moving": 0, "activating": 0, "cancelling": 0, "other": 0}, Moves: map[string]int{"copy": 0, "activate": 0, "cleanup": 0, "cancel": 0, "discard": 0, "other": 0}, At: time.Now().UTC()}
	for _, shard := range state.Shards {
		if shard.Draining {
			observation.DrainingShards++
		}
	}
	for _, placement := range state.Tenants {
		state := placement.State
		if _, exists := observation.Tenants[state]; !exists {
			state = "other"
		}
		observation.Tenants[state]++
		if placement.Move != nil {
			phase := placement.Move.Phase
			if _, exists := observation.Moves[phase]; !exists {
				phase = "other"
			}
			observation.Moves[phase]++
			if placement.Move.Error != "" {
				observation.MoveErrors++
			}
		}
	}
	return observation
}

func (a *Application) observePending() {
	if a.pending == nil {
		a.queueObservation.Store(nil)
		return
	}
	a.queueObservation.Store(&queueObservation{Requests: len(a.pending), Bytes: a.pendingBytes, At: time.Now().UTC()})
}

func (c *Cluster) DiskSpace(ctx context.Context) (storage.DiskSpaceStatus, error) {
	return storage.InspectDiskSpace(ctx, c.config.Dir, c.diskPolicy)
}

func (c *Cluster) WriteMetrics(w io.Writer) {
	c.Node.WriteMetrics(w)
	c.metrics.WritePrometheus(w, "graphdb_ha")
	paused := 0.
	if c.maintenanceWritesPaused() {
		paused = 1
	}
	observability.WriteScalar(w, "graphdb_ha_maintenance_writes_paused", "This process temporarily pauses one tenant's new mutations while preparing maintenance; other tenants remain admitted.", "gauge", paused)
	if c.config.Catalog {
		c.shards.WriteMetrics(w)
	}
	role := "single"
	if c.App.Catalog {
		role = "catalog"
	} else if c.App.ShardID != "" {
		role = "shard"
	}
	observability.WriteInfo(w, "graphdb_raft_deployment_info", "Local Raft application role.", []string{"role", "shard_id"}, []string{role, c.App.ShardID})
	queue := c.App.queueObservation.Load()
	known := 0.
	if queue != nil {
		known = 1
		observability.WriteScalar(w, "graphdb_raft_ingest_pending_requests", "Locally cached durable acceptance requests awaiting flush.", "gauge", float64(queue.Requests))
		observability.WriteScalar(w, "graphdb_raft_ingest_pending_bytes", "Encoded bytes represented by the locally cached acceptance queue.", "gauge", float64(queue.Bytes))
		observability.WriteScalar(w, "graphdb_raft_ingest_observed_timestamp_seconds", "Last successful observation of the local acceptance queue.", "gauge", float64(queue.At.UnixNano())/1e9)
	}
	observability.WriteScalar(w, "graphdb_raft_ingest_observation_known", "Queue is initialized locally; zero means pending gauges are unknown.", "gauge", known)
	if !c.App.Catalog {
		return
	}
	known = 0
	if catalog := c.App.catalogObservation.Load(); catalog != nil {
		known = 1
		observability.WriteScalar(w, "graphdb_catalog_version", "Last locally observed placement catalog version.", "gauge", float64(catalog.Version))
		observability.WriteScalar(w, "graphdb_catalog_shards", "Registered shards in the last local catalog observation.", "gauge", float64(catalog.Shards))
		observability.WriteScalar(w, "graphdb_catalog_draining_shards", "Draining shards in the last local catalog observation.", "gauge", float64(catalog.DrainingShards))
		observability.WriteScalar(w, "graphdb_catalog_move_errors", "Moves retaining an error in the last local catalog observation.", "gauge", float64(catalog.MoveErrors))
		observability.WriteScalar(w, "graphdb_catalog_observed_timestamp_seconds", "Last local placement catalog observation time.", "gauge", float64(catalog.At.UnixNano())/1e9)
		fmt.Fprintln(w, "# HELP graphdb_catalog_tenants Tenants by locally observed placement state.\n# TYPE graphdb_catalog_tenants gauge")
		for _, state := range []string{"active", "assigning", "moving", "activating", "cancelling", "other"} {
			fmt.Fprintf(w, "graphdb_catalog_tenants{state=%q} %d\n", state, catalog.Tenants[state])
		}
		fmt.Fprintln(w, "# HELP graphdb_catalog_moves Moves by locally observed migration phase.\n# TYPE graphdb_catalog_moves gauge")
		for _, phase := range []string{"copy", "activate", "cleanup", "cancel", "discard", "other"} {
			fmt.Fprintf(w, "graphdb_catalog_moves{phase=%q} %d\n", phase, catalog.Moves[phase])
		}
	}
	observability.WriteScalar(w, "graphdb_catalog_observation_known", "A local catalog observation is available; not a quorum or freshness guarantee.", "gauge", known)
}
