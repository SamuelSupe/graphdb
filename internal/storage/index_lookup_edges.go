package storage

import (
	"context"
	"sort"
	"strconv"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (l *PersistedIndexLookup) cachedParquetOutEdges(ctx context.Context, spec EdgeShard, from string) ([]graph.Edge, bool, error) {
	key := edgeLookupCacheKey(l.Version, spec)
	l.edgeMu.Lock()
	byFrom, cached := l.edgeCache[key]
	if cached {
		edges := copyEdgeSlice(byFrom[from])
		l.edgeMu.Unlock()
		return edges, true, nil
	}
	l.edgeMu.Unlock()

	shard, ok, err := l.Store.loadParquetEdgeShardObject(ctx, l.TenantID, l.Version, spec)
	if err != nil || !ok {
		return nil, ok, err
	}
	if !indexTenantMatches(shard.TenantID, l.TenantID) || !edgeShardMatchesCatalog(shard, spec, l.Version) {
		return nil, false, nil
	}
	byFrom = make(map[string][]graph.Edge)
	for _, edge := range shard.Edges {
		byFrom[edge.From] = append(byFrom[edge.From], edge)
	}
	for id := range byFrom {
		sort.Slice(byFrom[id], func(i, j int) bool { return byFrom[id][i].ID < byFrom[id][j].ID })
	}

	l.edgeMu.Lock()
	if l.edgeCache == nil {
		l.edgeCache = map[string]map[string][]graph.Edge{}
	}
	if existing, exists := l.edgeCache[key]; exists {
		byFrom = existing
	} else {
		l.edgeCache[key] = byFrom
	}
	edges := copyEdgeSlice(byFrom[from])
	l.edgeMu.Unlock()
	return edges, true, nil
}

func edgeLookupCacheKey(version int64, spec EdgeShard) string {
	return strconv.FormatInt(version, 10) + "\x00" + spec.RelationType + "\x00" + spec.Shard + "\x00" + spec.ContentHash + "\x00" + spec.SchemaHash
}

func copyEdgeSlice(edges []graph.Edge) []graph.Edge {
	if len(edges) == 0 {
		return nil
	}
	out := make([]graph.Edge, len(edges))
	for i, edge := range edges {
		out[i] = graph.CopyEdge(edge)
	}
	return out
}
