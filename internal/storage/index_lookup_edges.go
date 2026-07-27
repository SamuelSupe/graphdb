package storage

import (
	"context"
	"sort"
	"strconv"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (l *PersistedIndexLookup) cachedParquetOutEdges(ctx context.Context, spec EdgeShard, from string) ([]graph.Edge, bool, error) {
	byFrom, ok, err := l.cachedParquetOutEdgeMap(ctx, spec)
	if err != nil || !ok {
		return nil, ok, err
	}
	return copyEdgeSlice(byFrom[from]), true, nil
}

func (l *PersistedIndexLookup) cachedParquetOutEdgeMap(
	ctx context.Context,
	spec EdgeShard,
) (map[string][]graph.Edge, bool, error) {
	key := edgeLookupCacheKey(l.TenantID, l.Version, spec, "out")
	l.edgeMu.Lock()
	byFrom, cached := l.edgeCache[key]
	if cached {
		l.edgeMu.Unlock()
		return byFrom, true, nil
	}
	l.edgeMu.Unlock()
	byFrom, ok, err := l.Store.edgeLookupCache.load(
		ctx, key,
		func() (map[string][]graph.Edge, bool, error) {
			shard, ok, err := l.Store.loadParquetEdgeShardObject(
				ctx, l.TenantID, l.Version, spec,
			)
			if err != nil || !ok {
				return nil, ok, err
			}
			if !indexTenantMatches(shard.TenantID, l.TenantID) ||
				!edgeShardMatchesCatalog(shard, spec, l.Version) {
				return nil, false, nil
			}
			decoded := make(map[string][]graph.Edge)
			for _, edge := range shard.Edges {
				decoded[edge.From] = append(decoded[edge.From], edge)
			}
			for id := range decoded {
				sort.Slice(decoded[id], func(left, right int) bool {
					return decoded[id][left].ID < decoded[id][right].ID
				})
			}
			return decoded, true, nil
		},
	)
	if err != nil || !ok {
		return nil, ok, err
	}
	byFrom = l.cacheDecodedEdgeLookup(key, byFrom)
	return byFrom, true, nil
}

func (l *PersistedIndexLookup) cachedParquetInEdges(ctx context.Context, spec EdgeShard, to string) ([]graph.Edge, bool, error) {
	byTo, ok, err := l.cachedParquetInEdgeMap(ctx, spec)
	if err != nil || !ok {
		return nil, ok, err
	}
	return copyEdgeSlice(byTo[to]), true, nil
}

func (l *PersistedIndexLookup) cachedParquetInEdgeMap(
	ctx context.Context,
	spec EdgeShard,
) (map[string][]graph.Edge, bool, error) {
	key := edgeLookupCacheKey(l.TenantID, l.Version, spec, "in")
	l.edgeMu.Lock()
	byTo, cached := l.edgeCache[key]
	if cached {
		l.edgeMu.Unlock()
		return byTo, true, nil
	}
	l.edgeMu.Unlock()
	byTo, ok, err := l.Store.edgeLookupCache.load(
		ctx, key,
		func() (map[string][]graph.Edge, bool, error) {
			shard, ok, err := l.Store.loadParquetEdgeShardObject(
				ctx, l.TenantID, l.Version, spec,
			)
			if err != nil || !ok {
				return nil, ok, err
			}
			if !indexTenantMatches(shard.TenantID, l.TenantID) ||
				!edgeShardMatchesCatalog(shard, spec, l.Version) {
				return nil, false, nil
			}
			decoded := make(map[string][]graph.Edge)
			for _, edge := range shard.Edges {
				decoded[edge.To] = append(decoded[edge.To], edge)
			}
			for id := range decoded {
				sort.Slice(decoded[id], func(left, right int) bool {
					return decoded[id][left].ID < decoded[id][right].ID
				})
			}
			return decoded, true, nil
		},
	)
	if err != nil || !ok {
		return nil, ok, err
	}
	byTo = l.cacheDecodedEdgeLookup(key, byTo)
	return byTo, true, nil
}

func (l *PersistedIndexLookup) cacheDecodedEdgeLookup(
	key string,
	decoded map[string][]graph.Edge,
) map[string][]graph.Edge {
	l.edgeMu.Lock()
	defer l.edgeMu.Unlock()
	if l.edgeCache == nil {
		l.edgeCache = map[string]map[string][]graph.Edge{}
	}
	if existing, exists := l.edgeCache[key]; exists {
		return existing
	}
	l.edgeCache[key] = decoded
	return decoded
}

func edgeLookupCacheKey(
	tenantID string,
	version int64,
	spec EdgeShard,
	direction string,
) string {
	return tenantID + "\x00" + direction + "\x00" +
		strconv.FormatInt(version, 10) + "\x00" + spec.RelationType +
		"\x00" + spec.Shard + "\x00" + spec.ContentHash +
		"\x00" + spec.SchemaHash
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
