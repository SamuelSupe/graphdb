package graph

import (
	"context"
	"crypto/sha256"
	"maps"
	"strings"
)

const entityPartitionCount = 64

type entityPartitions [entityPartitionCount]map[string]struct{}

// EntityStorageShard preserves the persisted 64-bucket entity page layout.
func EntityStorageShard(id string) string {
	if id == "" {
		return "default"
	}
	const hex = "0123456789abcdef"
	n := entityPartition(id)
	return string([]byte{hex[n>>4], hex[n&15]})
}

func entityPartition(id string) byte {
	sum := sha256.Sum256([]byte(strings.ToLower(id)))
	return sum[0] % entityPartitionCount
}

func (g *Graph) shareEntityPartitions() *entityPartitions {
	g.entityPartitionsMu.Lock()
	defer g.entityPartitionsMu.Unlock()
	if g.entityPartitions == nil {
		parts := new(entityPartitions)
		for id := range g.Entities {
			n := entityPartition(id)
			if parts[n] == nil {
				parts[n] = make(map[string]struct{})
			}
			parts[n][id] = struct{}{}
		}
		g.entityPartitions = parts
	}
	return g.entityPartitions
}

// VisitEntityStorageShard visits one page's entities without scanning other
// pages. Values are borrowed from the immutable graph and must not be modified.
func (g *Graph) VisitEntityStorageShard(shard string, visit func(Entity) error) error {
	parts := g.shareEntityPartitions()
	for n, ids := range parts {
		const hex = "0123456789abcdef"
		if shard != string([]byte{hex[n>>4], hex[n&15]}) {
			continue
		}
		for id := range ids {
			if err := visit(g.Entities[id]); err != nil {
				return err
			}
		}
		break
	}
	return nil
}

// VisitEdgeStorageShard uses endpoint membership and adjacency to visit only
// the selected persisted edge shard. Values are borrowed and read-only.
func (g *Graph) VisitEdgeStorageShard(ctx context.Context, relationType, shard string, reverse bool, visit func(Edge) error) error {
	return g.VisitEntityStorageShard(shard, func(entity Entity) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids := g.out[entity.ID]
		if reverse {
			ids = g.in[entity.ID]
		}
		for id := range ids {
			edge := g.Edges[id]
			if edge.Type == relationType {
				if err := visit(edge); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (g *Graph) refreshEntityPartitions(tracker *mutationFingerprintTracker) {
	if g.entityPartitions == nil {
		return
	}
	parts := *g.entityPartitions
	var copied [entityPartitionCount]bool
	for id, before := range tracker.entities {
		_, exists := g.Entities[id]
		if exists == before.exists {
			continue
		}
		n := entityPartition(id)
		if !copied[n] {
			parts[n] = maps.Clone(parts[n])
			if parts[n] == nil {
				parts[n] = make(map[string]struct{})
			}
			copied[n] = true
		}
		if exists {
			parts[n][id] = struct{}{}
		} else {
			delete(parts[n], id)
		}
	}
	g.entityPartitions = &parts
}
