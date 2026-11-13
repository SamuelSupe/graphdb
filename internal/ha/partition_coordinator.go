package ha

import (
	"context"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

// The WAL driver runs on the home leader after a read barrier. Its local
// reads avoid serializing the graph over loopback. Raft still fences each
// transaction by generation, version, layout and identity.
type partitionCoordinator struct {
	sharding.GraphRemote
	cluster *Cluster
	ingest  *partitionIngestRun
}

type partitionIngestRun struct {
	canContinue func() bool
	preparation *partition.IngestPreparation
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

func (r partitionCoordinator) Action(ctx context.Context, home partition.Group, action partition.Action, result any) error {
	if action.Operation == "complete" && action.ReceiptRef != "" && r.local(home) &&
		r.ingest != nil && r.ingest.canContinue != nil && r.ingest.canContinue() {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) >= time.Second {
			next := r.planNextIngest(ctx, action)
			if next != nil && r.ingest.canContinue() {
				action.NextIngest = &next.Begin
				finish := r.cluster.metrics.Start("partition_ingest_handoff")
				err := r.GraphRemote.Action(ctx, home, action, result)
				finish(err)
				if err == nil {
					r.ingest.preparation = next
				}
				return err
			}
		}
	}
	return r.GraphRemote.Action(ctx, home, action, result)
}

func (r partitionCoordinator) planNextIngest(ctx context.Context, complete partition.Action) *partition.IngestPreparation {
	app := r.cluster.App
	g, err := app.graphDescriptor(ctx, complete.Tenant)
	if err != nil || g.Generation != complete.Generation || g.Active == nil ||
		g.Active.ID != complete.Txn || g.Active.Sequence != complete.Sequence ||
		g.GC.BlocksWrites() || g.Move.BlocksWrites() {
		return nil
	}

	app.mu.Lock()
	current := app.pending[app.ingestPrefix()+complete.ReceiptRef+".json"]
	var key string
	var record acceptedRequest
	if current.index != 0 && current.partitioned && current.tenant == g.Tenant && current.generation == g.Generation {
		key, err = app.partitionIngestAfter(ctx, g.Tenant, g.Generation, current.index)
		if err == nil && key != "" {
			record, err = app.accepted(ctx, key)
		}
	}
	app.mu.Unlock()
	if err != nil || key == "" || record.State != "accepted" {
		return nil
	}
	// The partition acknowledgments for the preceding decision have arrived.
	// The successor can now plan against the published roots.
	engine := partition.Engine{Remote: r, Metrics: r.cluster.metrics}
	next, err := engine.PlanIngestBegin(ctx, g, partitionIngestTxnIdentity(key, record.Index, record.ReceiptRetention > 0),
		partitionIngestReference(key), partition.IngestInput{Request: record.Request, AcceptedAt: record.AcceptedAt})
	if err != nil {
		return nil
	}
	return next
}

func (c *Cluster) partitionEngine(tenant string, generation int64, batch bool) partition.Engine {
	remote := sharding.GraphRemote{Client: c.shards}
	engine := partition.Engine{Remote: remote, Home: c.graphHome(), Tenant: tenant, Generation: generation, BatchFragments: batch, Metrics: c.metrics}
	if batch {
		engine.Remote = partitionCoordinator{GraphRemote: remote, cluster: c}
	}
	return engine
}
