package ha

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

// The WAL driver runs on the home leader after a read barrier. Its local
// reads avoid serializing the graph over loopback. Raft still fences each
// transaction by generation, version, layout and identity.
type partitionCoordinator struct {
	sharding.GraphRemote
	cluster *Cluster
}

func (r partitionCoordinator) local(g partition.Group) bool {
	return g.ID == r.cluster.App.ShardID && g.ClusterID == r.cluster.config.ClusterID
}

func (r partitionCoordinator) Describe(ctx context.Context, g partition.Group, tenant string) (partition.Graph, error) {
	if !r.local(g) {
		return r.GraphRemote.Describe(ctx, g, tenant)
	}
	return r.cluster.App.graphDescriptor(ctx, tenant)
}

func (r partitionCoordinator) Transaction(ctx context.Context, g partition.Group, tenant string, generation int64, identity string) (partition.Transaction, error) {
	if !r.local(g) {
		return r.GraphRemote.Transaction(ctx, g, tenant, generation, identity)
	}
	app := r.cluster.App
	app.mu.RLock()
	defer app.mu.RUnlock()
	return app.partitionStore().Transaction(ctx, tenant, generation, identity)
}

func (r partitionCoordinator) Input(ctx context.Context, g partition.Group, tenant string, generation int64, input partition.Input) ([]byte, error) {
	if !r.local(g) {
		return r.GraphRemote.Input(ctx, g, tenant, generation, input)
	}
	app := r.cluster.App
	app.mu.RLock()
	defer app.mu.RUnlock()
	return app.partitionStore().Input(ctx, tenant, generation, input)
}

func (c *Cluster) partitionEngine(tenant string, generation int64, batch bool) partition.Engine {
	remote := sharding.GraphRemote{Client: c.shards}
	engine := partition.Engine{Remote: remote, Home: c.graphHome(), Tenant: tenant, Generation: generation, BatchFragments: batch, Metrics: c.metrics}
	if batch {
		engine.Remote = partitionCoordinator{GraphRemote: remote, cluster: c}
	}
	return engine
}
