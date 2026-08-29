package storage

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type scanCandidate[T any] struct {
	position scanPosition
	value    T
}

type scanCandidateHeap[T any] []scanCandidate[T]

type scanPosition struct {
	group string
	id    string
}

func ListEntitiesFromGraph(ctx context.Context, tenantID string, g *graph.Graph, manifest Manifest, options EntityScanOptions) (EntityScanResult, error) {
	if err := validateGraphScanInput(tenantID, g, manifest, options.MinVersion); err != nil {
		return EntityScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, manifest.Version, entityScanQueryHash(options))
	if err != nil {
		return EntityScanResult{}, err
	}
	entities, next, err := pageEntityMap(ctx, g.Entities, manifest.Version, options, cursor)
	if err != nil {
		return EntityScanResult{}, err
	}
	for i := range entities {
		entities[i] = graph.CopyEntity(entities[i])
	}
	return EntityScanResult{TenantID: tenantID, Version: manifest.Version, Entities: entities, NextCursor: next}, nil
}

func ListEdgesFromGraph(ctx context.Context, tenantID string, g *graph.Graph, manifest Manifest, options EdgeScanOptions) (EdgeScanResult, error) {
	if err := validateGraphScanInput(tenantID, g, manifest, options.MinVersion); err != nil {
		return EdgeScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, manifest.Version, edgeScanQueryHash(options))
	if err != nil {
		return EdgeScanResult{}, err
	}
	edges, next, err := pageEdgeMap(ctx, g.Edges, manifest.Version, options, cursor)
	if err != nil {
		return EdgeScanResult{}, err
	}
	for i := range edges {
		edges[i] = graph.CopyEdge(edges[i])
	}
	return EdgeScanResult{TenantID: tenantID, Version: manifest.Version, Edges: edges, NextCursor: next}, nil
}

func validateGraphScanInput(tenantID string, g *graph.Graph, manifest Manifest, minVersion int64) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if g == nil {
		return fmt.Errorf("graph is required")
	}
	if manifest.TenantID != "" && manifest.TenantID != tenantID {
		return fmt.Errorf("graph manifest tenant mismatch: path tenant %q contains tenant %q", tenantID, manifest.TenantID)
	}
	if minVersion > 0 && manifest.Version < minVersion {
		return fmt.Errorf("graph version %d is below required version %d", manifest.Version, minVersion)
	}
	return nil
}

func pageEntityMap(ctx context.Context, candidates map[string]graph.Entity, version int64, options EntityScanOptions, cursor scanCursor) ([]graph.Entity, string, error) {
	items, err := selectBoundedScanCandidates(ctx, candidates, normalizedScanLimit(options.Limit)+1, cursor.After,
		func(entity graph.Entity) scanPosition {
			return scanPosition{group: entityShardID(entity.ID), id: entity.ID}
		},
		func(entity graph.Entity) bool { return entityMatchesScan(entity, options) },
	)
	if err != nil {
		return nil, "", err
	}
	entities, next := pageEntities(items, version, options, cursor)
	return entities, next, nil
}

func pageEdgeMap(ctx context.Context, candidates map[string]graph.Edge, version int64, options EdgeScanOptions, cursor scanCursor) ([]graph.Edge, string, error) {
	items, err := selectBoundedScanCandidates(ctx, candidates, normalizedScanLimit(options.Limit)+1, cursor.After,
		func(edge graph.Edge) scanPosition {
			return scanPosition{group: edge.Type + "\x00" + edgeShardID(edge.From), id: edge.ID}
		},
		func(edge graph.Edge) bool { return edgeMatchesScan(edge, options) },
	)
	if err != nil {
		return nil, "", err
	}
	edges, next := pageEdges(items, version, options, cursor)
	return edges, next, nil
}

func selectBoundedScanCandidates[T any](ctx context.Context, values map[string]T, keep int, after string, positionFor func(T) scanPosition, matches func(T) bool) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if keep <= 0 {
		return nil, nil
	}
	afterPosition, validAfter := parseScanPosition(after)
	candidates := make(scanCandidateHeap[T], 0, min(len(values), keep))
	checked := 0
	for _, value := range values {
		checked++
		if checked&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if !matches(value) {
			continue
		}
		position := positionFor(value)
		if after != "" {
			if validAfter {
				if position.compare(afterPosition) <= 0 {
					continue
				}
			} else if scanKey(position.group, position.id) <= after {
				continue
			}
		}
		candidate := scanCandidate[T]{position: position, value: value}
		if len(candidates) < keep {
			heap.Push(&candidates, candidate)
			continue
		}
		if position.compare(candidates[0].position) >= 0 {
			continue
		}
		candidates[0] = candidate
		heap.Fix(&candidates, 0)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].position.compare(candidates[j].position) < 0 })
	selected := make([]T, len(candidates))
	for i := range candidates {
		selected[i] = candidates[i].value
	}
	return selected, nil
}

func parseScanPosition(key string) (scanPosition, bool) {
	separator := strings.LastIndexByte(key, 0)
	if separator < 0 {
		return scanPosition{}, false
	}
	return scanPosition{group: key[:separator], id: key[separator+1:]}, true
}

func (position scanPosition) compare(other scanPosition) int {
	if group := strings.Compare(position.group, other.group); group != 0 {
		return group
	}
	return strings.Compare(position.id, other.id)
}

func (h scanCandidateHeap[T]) Len() int { return len(h) }

func (h scanCandidateHeap[T]) Less(i, j int) bool {
	return h[i].position.compare(h[j].position) > 0
}

func (h scanCandidateHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *scanCandidateHeap[T]) Push(value any) {
	*h = append(*h, value.(scanCandidate[T]))
}

func (h *scanCandidateHeap[T]) Pop() any {
	values := *h
	last := len(values) - 1
	value := values[last]
	var zero scanCandidate[T]
	values[last] = zero
	*h = values[:last]
	return value
}
