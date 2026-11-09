package ha

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Conversion never falls back to a materialized graph. Preparing the legacy
// indexes is an explicit existing maintenance operation before the cutover.
func (c *Cluster) checkPartitionSource(ctx context.Context, tenant string) error {
	if storage.ValidateTenantID(tenant) != nil {
		return fmt.Errorf("%w: invalid tenant", storage.ErrInvalidArgument)
	}
	if _, err := c.App.partitionStore().GetGraph(ctx, tenant); err == nil {
		return nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if err := c.App.Store.EnsureTenantWritable(ctx, tenant); err != nil {
		return err
	}
	_, manifest, err := c.App.Store.ReadCurrentSchema(ctx, tenant)
	if err != nil {
		return err
	}
	if manifest.Version == 0 {
		return nil
	}
	entities, err := c.App.Store.ListEntities(ctx, tenant, storage.EntityScanOptions{Limit: 1, MinVersion: manifest.Version, SkipGraphFallback: true})
	if err != nil {
		return fmt.Errorf("%w: rebuild source indexes before enabling graph partitions: %v", storage.ErrConflict, err)
	}
	edges, err := c.App.Store.ListEdges(ctx, tenant, storage.EdgeScanOptions{Limit: 1, MinVersion: manifest.Version, SkipGraphFallback: true})
	if err != nil {
		return fmt.Errorf("%w: rebuild source indexes before enabling graph partitions: %v", storage.ErrConflict, err)
	}
	if !entities.IndexedRead || !edges.IndexedRead || entities.Version != manifest.Version || edges.Version != manifest.Version || manifest.DataHash == "" {
		return fmt.Errorf("%w: source indexes and digest must match the current version", storage.ErrConflict)
	}
	return nil
}

func (c *Cluster) partitionSourceCheck(w http.ResponseWriter, r *http.Request) {
	if c.config.Protocol < 5 || c.App.Catalog || c.App.ShardID == "" {
		http.Error(w, "graph partitions require a protocol-5 data group", http.StatusConflict)
		return
	}
	if err := c.Node.ReadBarrier(r.Context()); err != nil {
		c.writeError(w, err)
		return
	}
	c.App.mu.RLock()
	err := c.checkPartitionSource(r.Context(), r.PathValue("tenant"))
	c.App.mu.RUnlock()
	if err != nil {
		c.partitionError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
