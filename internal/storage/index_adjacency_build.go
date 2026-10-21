package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type adjacencyBuildKey struct{}

var errAdjacencyIndex = errors.New("adjacency index update failed")

func adjacencyNodeBytes(n adjacencyNode) int64 {
	bytes := int64(96)
	for _, e := range n.Entries {
		bytes += 64 + int64(len(e.Endpoint)+len(e.ID))
	}
	for _, r := range n.Children {
		bytes += 192 + int64(len(r.Key)+len(r.Hash)+len(r.First.Endpoint)+len(r.First.ID)+len(r.Last.Endpoint)+len(r.Last.ID))
	}
	return bytes
}

// Full conversion is confined to explicit rebuilds, with the existing captured
// manifest/fence/CAS publication. Feed bounded payload chunks into the tree;
// no whole-bucket endpoint map or flat adjacency directory is constructed.
func (s *TenantStore) buildAdjacencyRoots(ctx context.Context, tenant string, version int64, specs []EdgeShard, parts []EdgeShardData, reverse bool) error {
	if enabled, _ := ctx.Value(adjacencyBuildKey{}).(bool); !enabled || !s.WriteIndexAdjacencyPages {
		return nil
	}
	if !s.WriteIndexRangePages {
		return fmt.Errorf("adjacency pages require range pages")
	}
	byKey := make(map[string]int, len(specs))
	for i, spec := range specs {
		byKey[edgeShardTargetKey(spec.RelationType, spec.Shard)] = i
	}
	for _, part := range parts {
		if err := ctx.Err(); err != nil {
			return err
		}
		i := byKey[edgeShardTargetKey(part.RelationType, part.Shard)]
		spec := &specs[i]
		scope := fmt.Sprintf("%t/%s/%s", reverse, spec.RelationType, spec.Shard)
		changes := make([]adjacencyChange, 0, len(part.Edges))
		for _, e := range part.Edges {
			endpoint := e.From
			if reverse {
				endpoint = e.To
			}
			changes = append(changes, adjacencyChange{key: adjacencyKey{endpoint, e.ID}})
		}
		root, err := s.updateAdjacency(ctx, tenant, s.adjacencyPrefix(tenant, version, scope), spec.AdjacencyRoot, changes)
		if err != nil {
			return fmt.Errorf("%w: %w", errAdjacencyIndex, err)
		}
		spec.AdjacencyRoot = root
	}
	for _, spec := range specs {
		if spec.AdjacencyRoot == nil || spec.AdjacencyRoot.Count != spec.EdgeCount {
			return fmt.Errorf("incomplete adjacency index")
		}
	}
	return nil
}

func (s *TenantStore) updateEdgeAdjacency(ctx context.Context, tenant string, old EdgeShard, next *EdgeShard, before, after *graph.Graph, ids map[string]bool, version int64, reverse bool) error {
	if old.AdjacencyRoot == nil {
		return nil
	}
	if old.AdjacencyRoot.Count != old.EdgeCount {
		return fmt.Errorf("%w: root count mismatch", errAdjacencyIndex)
	}
	var changes []adjacencyChange
	keyFor := func(g *graph.Graph, id string) (adjacencyKey, bool) {
		e, ok := g.Edges.Get(id)
		endpoint := e.From
		if reverse {
			endpoint = e.To
		}
		return adjacencyKey{endpoint, id}, ok && e.Type == old.RelationType && indexShardIDMatches(endpoint, old.Shard)
	}
	for id := range ids {
		a, aok := keyFor(before, id)
		b, bok := keyFor(after, id)
		if aok && bok && a == b {
			continue
		}
		if aok {
			changes = append(changes, adjacencyChange{key: a, remove: true})
		}
		if bok {
			changes = append(changes, adjacencyChange{key: b})
		}
	}
	scope := fmt.Sprintf("%t/%s/%s", reverse, old.RelationType, old.Shard)
	root, err := s.updateAdjacency(ctx, tenant, s.adjacencyPrefix(tenant, version, scope), old.AdjacencyRoot, changes)
	if err != nil {
		return fmt.Errorf("%w: %w", errAdjacencyIndex, err)
	}
	if root == nil || root.Count != next.EdgeCount {
		return fmt.Errorf("%w: changed root count mismatch", errAdjacencyIndex)
	}
	next.AdjacencyRoot = root
	return nil
}
