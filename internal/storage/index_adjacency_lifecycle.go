package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (s *TenantStore) walkAdjacency(ctx context.Context, tenant string, root AdjacencyRef, seen map[string]struct{}, visit func(adjacencyKey) error) error {
	key := s.adjacencyObjectKey(root)
	if _, ok := seen[key]; ok {
		return nil
	}
	n, err := s.loadAdjacencyNode(ctx, tenant, root)
	if err != nil {
		return err
	}
	seen[key] = struct{}{}
	for _, e := range n.Entries {
		if visit != nil {
			if err := visit(e); err != nil {
				return err
			}
		}
	}
	for _, child := range n.Children {
		if err := s.walkAdjacency(ctx, tenant, child, seen, visit); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (s *TenantStore) markAdjacencyCatalog(ctx context.Context, tenant string, specs []EdgeShard, keep map[string]struct{}) error {
	for _, spec := range specs {
		if spec.AdjacencyRoot != nil {
			if err := s.walkAdjacency(ctx, tenant, *spec.AdjacencyRoot, keep, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// Mark retained immutable catalog snapshots too. If an input cannot be read,
// abort collection: absence/corruption is never evidence of unreachability.
func (s *TenantStore) retainedAdjacencyKeys(ctx context.Context, tenant string, current IndexCatalog) (map[string]struct{}, error) {
	keep := map[string]struct{}{}
	if err := s.markAdjacencyCatalog(ctx, tenant, current.EdgeShards, keep); err != nil {
		return nil, err
	}
	reverse, _, err := s.getReverseIndexCatalogWithMeta(ctx, tenant)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if err == nil {
		if err := s.markAdjacencyCatalog(ctx, tenant, reverse.EdgeShards, keep); err != nil {
			return nil, err
		}
	}
	err = scanObjectPrefix(ctx, s.Objects, s.parquetVersionRootPrefix(tenant), func(objects []ObjectInfo) error {
		for _, o := range objects {
			if !isVersionedIndexCatalogKey(o.Key) {
				continue
			}
			data, _, err := s.Objects.GetWithMeta(ctx, o.Key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			catalog, err := decodeParquetIndexCatalog(ctx, data)
			if err != nil {
				return err
			}
			if !indexTenantMatches(catalog.TenantID, tenant) {
				return fmt.Errorf("adjacency catalog tenant mismatch")
			}
			if err := s.markAdjacencyCatalog(ctx, tenant, catalog.EdgeShards, keep); err != nil {
				return err
			}
		}
		return nil
	})
	return keep, err
}

func (s *TenantStore) checkAdjacencyShards(ctx context.Context, tenant string, specs []EdgeShard, g *graph.Graph, reverse bool, health *IndexHealth) {
	for _, spec := range specs {
		if spec.AdjacencyRoot == nil {
			continue
		}
		count := 0
		err := s.walkAdjacency(ctx, tenant, *spec.AdjacencyRoot, map[string]struct{}{}, func(k adjacencyKey) error {
			e, ok := g.Edges.Get(k.ID)
			endpoint := e.From
			if reverse {
				endpoint = e.To
			}
			if !ok || e.Type != spec.RelationType || endpoint != k.Endpoint || !indexShardIDMatches(endpoint, spec.Shard) {
				return fmt.Errorf("adjacency entry does not match graph")
			}
			count++
			return nil
		})
		if err == nil && count != spec.EdgeCount {
			err = fmt.Errorf("adjacency count mismatch")
		}
		if err != nil {
			health.Issues = append(health.Issues, "adjacency "+spec.RelationType+"/"+spec.Shard+": "+err.Error())
		}
	}
}
func isAdjacencyObjectKey(key string) bool {
	return strings.Contains(key, "/adjacency/") && strings.HasSuffix(key, ".adj.json")
}
