package storage

import (
	"context"
	"errors"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type cachedEntityScanOrder struct {
	done  chan struct{}
	items []scanCandidate
	err   error
}

// ListEntitiesFromReadView accepts only a graph borrowed from this cache's
// read-only callbacks. Ordering belongs to that graph instance, so replacement
// at the same version (restore) cannot reuse an earlier ordering. Results are
// owned copies, just like ListEntitiesFromGraph.
func (c *ReaderCache) ListEntitiesFromReadView(ctx context.Context, tenantID string, g *graph.Graph, manifest Manifest, options EntityScanOptions) (EntityScanResult, error) {
	if err := validateGraphScanInput(tenantID, g, manifest, options.MinVersion); err != nil {
		return EntityScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, manifest.Version, entityScanQueryHash(options))
	if err != nil {
		return EntityScanResult{}, err
	}
	var order []scanCandidate
	for {
		order, err = c.readViewEntityScanOrder(ctx, tenantID, g)
		if ctx.Err() != nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
			break
		}
	}
	if err != nil {
		return EntityScanResult{}, err
	}
	if order == nil {
		return ListEntitiesFromGraph(ctx, tenantID, g, manifest, options)
	}
	start := 0
	if cursor.After != "" {
		after, valid := parseScanPosition(cursor.After)
		start = sort.Search(len(order), func(i int) bool {
			if valid {
				return order[i].position.compare(after) > 0
			}
			return scanKey(order[i].position.group, order[i].position.id) > cursor.After
		})
	}
	limit := normalizedScanLimit(options.Limit)
	items := make([]graph.Entity, 0, min(len(order)-start, limit+1))
	for i := start; i < len(order); i++ {
		if (i-start)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return EntityScanResult{}, err
			}
		}
		entity := g.Entities[order[i].key]
		if !entityMatchesScan(entity, options) {
			continue
		}
		items = append(items, entity)
		if len(items) > limit {
			break
		}
	}
	page := entityScanPage("", manifest.Version, items, options, len(items) > limit, "")
	entities := page.Entities
	for i := range entities {
		entities[i] = graph.CopyEntity(entities[i])
	}
	return EntityScanResult{TenantID: tenantID, Version: manifest.Version, Entities: entities, NextCursor: page.NextCursor}, ctx.Err()
}

func (c *ReaderCache) readViewEntityScanOrder(ctx context.Context, tenantID string, g *graph.Graph) ([]scanCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	entry, ok := c.entries[tenantID]
	if !ok || entry.graph != g {
		c.mu.Unlock()
		return nil, nil
	}
	if order := entry.entityScanOrder; order != nil {
		c.mu.Unlock()
		select {
		case <-order.done:
			return order.items, order.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Reserve before allocating; ID strings are borrowed from the immutable map.
	// If the reader cache is full, the bounded heap remains the fallback.
	bytes := int64(len(g.Entities))*64 + 128
	if bytes > c.MaxBytes-c.bytes {
		c.mu.Unlock()
		return nil, nil
	}
	order := &cachedEntityScanOrder{done: make(chan struct{})}
	entry.entityScanOrder = order
	entry.bytes += bytes
	c.bytes += bytes
	c.entries[tenantID] = entry
	c.mu.Unlock()

	items := make([]scanCandidate, 0, len(g.Entities))
	for key, entity := range g.Entities {
		if len(items)&255 == 0 {
			if order.err = ctx.Err(); order.err != nil {
				break
			}
		}
		items = append(items, scanCandidate{key: key, position: scanPosition{group: entityShardID(entity.ID), id: entity.ID}})
	}
	if order.err == nil {
		sort.Slice(items, func(i, j int) bool { return items[i].position.compare(items[j].position) < 0 })
		order.err = ctx.Err()
	}
	if order.err == nil {
		order.items = items
	} else {
		c.mu.Lock()
		if current, ok := c.entries[tenantID]; ok && current.entityScanOrder == order {
			current.entityScanOrder = nil
			current.bytes -= bytes
			c.bytes -= bytes
			c.entries[tenantID] = current
		}
		c.mu.Unlock()
	}
	close(order.done)
	return order.items, order.err
}
