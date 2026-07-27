package storage

import (
	"container/list"
	"context"
	"sync"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const defaultEdgeLookupCacheMaxBytes = int64(256 << 20)

type edgeLookupCache struct {
	mu       sync.Mutex
	max      int
	maxBytes int64
	bytes    int64
	data     map[string]*edgeLookupCacheEntry
	order    *list.List
	loading  map[string]*edgeLookupCacheLoad
}

type edgeLookupCacheEntry struct {
	edges map[string][]graph.Edge
	size  int64
	node  *list.Element
}

type edgeLookupCacheLoad struct {
	done     chan struct{}
	edges    map[string][]graph.Edge
	ok       bool
	err      error
	canceled bool
}

func newEdgeLookupCache(max int, maxBytes int64) *edgeLookupCache {
	if max < 0 {
		max = 0
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	return &edgeLookupCache{
		max:      max,
		maxBytes: maxBytes,
		data:     map[string]*edgeLookupCacheEntry{},
		order:    list.New(),
		loading:  map[string]*edgeLookupCacheLoad{},
	}
}

func configuredEdgeLookupCache(max int, rawMaxBytes int64) *edgeLookupCache {
	maxBytes := rawMaxBytes / 2
	if rawMaxBytes <= 0 {
		maxBytes = indexCacheByteLimit(
			max, defaultEdgeLookupCacheMaxBytes,
		) / 2
	}
	return newEdgeLookupCache(max, maxBytes)
}

func (c *edgeLookupCache) load(
	ctx context.Context,
	key string,
	loader func() (map[string][]graph.Edge, bool, error),
) (map[string][]graph.Edge, bool, error) {
	if c == nil || c.max <= 0 || c.maxBytes <= 0 || key == "" {
		return loader()
	}
	for {
		c.mu.Lock()
		if entry := c.data[key]; entry != nil {
			c.order.MoveToBack(entry.node)
			edges := entry.edges
			c.mu.Unlock()
			return edges, true, nil
		}
		if active := c.loading[key]; active != nil {
			c.mu.Unlock()
			select {
			case <-active.done:
				if active.canceled && ctx.Err() == nil {
					continue
				}
				return active.edges, active.ok, active.err
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
		active := &edgeLookupCacheLoad{done: make(chan struct{})}
		c.loading[key] = active
		c.mu.Unlock()

		edges, ok, err := loader()
		if err == nil && ok {
			edges = c.put(key, edges)
		}
		c.mu.Lock()
		active.edges = edges
		active.ok = ok
		active.err = err
		active.canceled = loadCanceledByContext(ctx, err)
		delete(c.loading, key)
		close(active.done)
		c.mu.Unlock()
		return edges, ok, err
	}
}

func (c *edgeLookupCache) put(
	key string,
	edges map[string][]graph.Edge,
) map[string][]graph.Edge {
	if c == nil || c.max <= 0 || c.maxBytes <= 0 || key == "" {
		return edges
	}
	size := estimateEdgeLookupBytes(edges)
	if size > c.maxBytes {
		return edges
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.data[key]; existing != nil {
		c.order.MoveToBack(existing.node)
		return existing.edges
	}
	entry := &edgeLookupCacheEntry{edges: edges, size: size}
	entry.node = c.order.PushBack(key)
	c.data[key] = entry
	c.bytes += size
	for len(c.data) > c.max || c.bytes > c.maxBytes {
		front := c.order.Front()
		if front == nil {
			break
		}
		evictedKey := front.Value.(string)
		c.order.Remove(front)
		evicted := c.data[evictedKey]
		delete(c.data, evictedKey)
		c.bytes -= evicted.size
	}
	return edges
}

func estimateEdgeLookupBytes(byEntity map[string][]graph.Edge) int64 {
	size := int64(128)
	for entityID, edges := range byEntity {
		size += 64 + int64(len(entityID))
		for _, edge := range edges {
			size += 320 + int64(
				len(edge.ID)+len(edge.Type)+len(edge.From)+len(edge.To)+
					len(edge.Source)+len(edge.ExternalID),
			)
			size += estimateAnyMap(edge.Fields)
			for field, source := range edge.FieldSources {
				size += 96 + int64(len(field)+len(source.Source))
			}
			for _, source := range edge.Sources {
				size += 96 + int64(
					len(source.Source)+len(source.ExternalID)+len(source.EdgeID),
				)
			}
		}
	}
	return size
}
