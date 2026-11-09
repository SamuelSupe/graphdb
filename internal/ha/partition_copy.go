package ha

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (c *Cluster) copyPartitionedGraph(ctx context.Context, initial partition.Graph) (partition.Graph, error) {
	remote := sharding.GraphRemote{Client: c.shards}
	engine := partition.Engine{Remote: remote, Home: c.graphHome(), Tenant: initial.Tenant, Generation: initial.Generation}
	for {
		g, err := remote.Describe(ctx, c.graphHome(), initial.Tenant)
		if err != nil {
			return g, err
		}
		if g.Generation != initial.Generation {
			return g, fmt.Errorf("%w: graph copy generation changed", storage.ErrConflict)
		}
		if g.State == "active" {
			if !g.SourceRetired {
				if err := remote.Action(ctx, c.graphHome(), partition.Action{Operation: "retire_source", Tenant: g.Tenant, Generation: g.Generation}, nil); err != nil {
					return g, err
				}
				continue
			}
			return g, nil
		}
		if g.State != "initializing" || g.Copy == nil {
			return g, fmt.Errorf("%w: graph copy is not active", storage.ErrConflict)
		}
		if g.Active != nil {
			if _, err := engine.Resume(ctx); err != nil {
				return g, err
			}
			continue
		}
		phase, cursor := g.Copy.Phase, g.Copy.Cursor
		nextPhase, nextCursor := phase, ""
		copyBatch := func(chunk partition.ImportChunk, index int) error {
			_, err := engine.Import(ctx, chunk, fmt.Sprintf("%s:%d", partition.ImportIdentity(phase, cursor), index))
			return err
		}
		switch phase {
		case "schema":
			if err := copyBatch(partition.ImportChunk{Schema: true}, 0); err != nil {
				return g, err
			}
			nextPhase = "entities"
			if g.Version == 0 {
				nextPhase = "identities"
			}
		case "entities":
			batch, err := c.App.Store.ListEntities(ctx, g.Tenant, storage.EntityScanOptions{Limit: 128, Cursor: cursor, MinVersion: g.Version, SkipGraphFallback: true})
			if err != nil {
				return g, err
			}
			if batch.Version != g.Version || !batch.IndexedRead {
				return g, fmt.Errorf("source entity pages do not match the fixed conversion version")
			}
			chunk := partition.ImportChunk{}
			bytes, index := 0, 0
			for _, entity := range batch.Entities {
				encoded, err := json.Marshal(entity)
				if err != nil {
					return g, err
				}
				if len(encoded) > partition.MaxValueBytes {
					return g, fmt.Errorf("%w: entity exceeds partition value budget", storage.ErrInvalidArgument)
				}
				if bytes+len(encoded) > 4<<20 && len(chunk.Entities) > 0 {
					if err := copyBatch(chunk, index); err != nil {
						return g, err
					}
					index++
					chunk = partition.ImportChunk{}
					bytes = 0
				}
				chunk.Entities = append(chunk.Entities, entity)
				bytes += len(encoded)
			}
			if len(chunk.Entities) > 0 {
				if err := copyBatch(chunk, index); err != nil {
					return g, err
				}
			}
			nextCursor = batch.NextCursor
			if nextCursor == "" {
				nextPhase = "edges"
			}
		case "edges":
			batch, err := c.App.Store.ListEdges(ctx, g.Tenant, storage.EdgeScanOptions{Limit: 128, Cursor: cursor, MinVersion: g.Version, SkipGraphFallback: true})
			if err != nil {
				return g, err
			}
			if batch.Version != g.Version || !batch.IndexedRead {
				return g, fmt.Errorf("source edge pages do not match the fixed conversion version")
			}
			chunk := partition.ImportChunk{}
			bytes, index := 0, 0
			for _, edge := range batch.Edges {
				encoded, err := json.Marshal(edge)
				if err != nil {
					return g, err
				}
				if len(encoded) > partition.MaxValueBytes {
					return g, fmt.Errorf("%w: edge exceeds partition value budget", storage.ErrInvalidArgument)
				}
				if bytes+len(encoded) > 4<<20 && len(chunk.Edges) > 0 {
					if err := copyBatch(chunk, index); err != nil {
						return g, err
					}
					index++
					chunk = partition.ImportChunk{}
					bytes = 0
				}
				chunk.Edges = append(chunk.Edges, edge)
				bytes += len(encoded)
			}
			if len(chunk.Edges) > 0 {
				if err := copyBatch(chunk, index); err != nil {
					return g, err
				}
			}
			nextCursor = batch.NextCursor
			if nextCursor == "" {
				nextPhase = "identities"
			}
		case "identities":
			records, next, err := c.App.Store.ListDirectCommitRecords(ctx, g.Tenant, cursor, 32)
			if err != nil {
				return g, err
			}
			for _, record := range records {
				txn, err := partition.LegacyTransaction(record, g.Generation)
				if err != nil {
					return g, err
				}
				if err := remote.Action(ctx, c.graphHome(), partition.Action{Operation: "import_identities", Tenant: g.Tenant, Generation: g.Generation, Transactions: []partition.Transaction{txn}}, nil); err != nil {
					return g, err
				}
			}
			nextCursor = next
			if nextCursor == "" {
				nextPhase = "verify"
			}
		case "verify":
			if err := remote.Action(ctx, c.graphHome(), partition.Action{Operation: "activate", Tenant: g.Tenant, Generation: g.Generation}, nil); err != nil {
				return g, err
			}
			continue
		default:
			return g, fmt.Errorf("invalid graph conversion phase %q", phase)
		}
		if err := remote.Action(ctx, c.graphHome(), partition.Action{Operation: "copy_progress", Tenant: g.Tenant, Generation: g.Generation, Purpose: phase, ExpectedCursor: cursor, Phase: nextPhase, Cursor: nextCursor}, nil); err != nil {
			return g, err
		}
	}
}
