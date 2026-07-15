package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) ListEdges(ctx context.Context, tenantID string, options EdgeScanOptions) (EdgeScanResult, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return EdgeScanResult{}, err
	}
	options.normalize()
	cursorVersion, cursorCatalogHash, hasCursorVersion, err := scanCursorPinnedCatalog(options.Cursor)
	if err != nil {
		return EdgeScanResult{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return EdgeScanResult{}, err
	}
	if hasCursorVersion {
		if options.MinVersion > 0 && cursorVersion < options.MinVersion {
			return EdgeScanResult{}, fmt.Errorf("cursor version %d is below required version %d", cursorVersion, options.MinVersion)
		}
		if catalog, err := s.GetIndexCatalogSnapshot(ctx, tenantID, cursorVersion, cursorCatalogHash); err == nil {
			cursor, err := parseScanCursor(options.Cursor, cursorVersion, edgeScanQueryHash(options))
			if err != nil {
				return EdgeScanResult{}, err
			}
			result, ok, err := s.listEdgesFromShards(ctx, tenantID, cursorVersion, catalog, options, cursor)
			if err != nil {
				return EdgeScanResult{}, err
			}
			if ok {
				return result, nil
			}
		} else if !errors.Is(err, ErrNotFound) {
			return EdgeScanResult{}, err
		}
		if manifest.Version != cursorVersion {
			return EdgeScanResult{}, fmt.Errorf("cursor version %d is no longer available", cursorVersion)
		}
	}
	catalog, indexedRead, err := s.scanCatalog(ctx, tenantID, manifest.Version)
	if err != nil {
		return EdgeScanResult{}, err
	}
	scanVersion := manifest.Version
	if indexedRead {
		scanVersion = catalog.Version
	}
	if options.MinVersion > 0 && scanVersion < options.MinVersion {
		indexedRead = false
		scanVersion = manifest.Version
	}
	cursor, err := parseScanCursor(options.Cursor, scanVersion, edgeScanQueryHash(options))
	if err != nil {
		return EdgeScanResult{}, err
	}
	if indexedRead {
		result, ok, err := s.listEdgesFromShards(ctx, tenantID, scanVersion, catalog, options, cursor)
		if err != nil {
			return EdgeScanResult{}, err
		}
		if ok {
			return result, nil
		}
	}
	minVersion := manifest.Version
	if options.MinVersion > minVersion {
		minVersion = options.MinVersion
	}
	g, loaded, err := s.LoadAtLeast(ctx, tenantID, minVersion)
	if err != nil {
		return EdgeScanResult{}, err
	}
	edges, next := pageEdges(sortedScanEdges(g.Edges), loaded.Version, options, cursor)
	return EdgeScanResult{TenantID: tenantID, Version: loaded.Version, Edges: edges, NextCursor: next}, nil
}

func (s *TenantStore) listEdgesFromShards(ctx context.Context, tenantID string, version int64, catalog IndexCatalog, options EdgeScanOptions, cursor scanCursor) (EdgeScanResult, bool, error) {
	compiled, err := s.compiledScanCatalog(tenantID, catalog, cursor.CatalogHash)
	if err != nil {
		return EdgeScanResult{}, false, err
	}
	items := make([]graph.Edge, 0, normalizedScanLimit(options.Limit)+1)
	targets := compiled.edgeScanTargets(options)
	specs := compiled.edgeSpecs
	targets = targets[edgeScanStart(targets, cursor.After):]
	for i, target := range targets {
		spec, ok := specs[edgeShardTargetKey(target.RelationType, target.Shard)]
		if !ok {
			return EdgeScanResult{}, false, nil
		}
		if specFormat(spec.Format) == IndexFormatParquet {
			if i+1 < len(targets) {
				nextTarget := targets[i+1]
				if next, ok := specs[edgeShardTargetKey(nextTarget.RelationType, nextTarget.Shard)]; ok && specFormat(next.Format) == IndexFormatParquet {
					s.prefetchParquetEdgeShardObject(ctx, tenantID, version, next)
				}
			}
			shard, ok, err := s.loadParquetEdgeShardObject(ctx, tenantID, version, spec)
			if err != nil || !ok {
				return EdgeScanResult{}, ok, err
			}
			if !edgeShardReadable(shard, tenantID, version, spec) {
				return EdgeScanResult{}, false, nil
			}
			for _, edge := range shard.Edges {
				if !edgeMatchesScan(edge, options) || scanKey(target.RelationType+"\x00"+target.Shard, edge.ID) <= cursor.After {
					continue
				}
				items = append(items, edge)
				if len(items) > normalizedScanLimit(options.Limit) {
					return edgeScanPage(tenantID, version, items, options, true, compiled.contentHash), true, nil
				}
			}
			continue
		}
		return EdgeScanResult{}, false, nil
	}
	return edgeScanPage(tenantID, version, items, options, false, compiled.contentHash), true, nil
}

func pageEdges(candidates []graph.Edge, version int64, options EdgeScanOptions, cursor scanCursor) ([]graph.Edge, string) {
	items := make([]graph.Edge, 0, normalizedScanLimit(options.Limit)+1)
	for _, edge := range candidates {
		key := scanKey(edge.Type+"\x00"+edgeShardID(edge.From), edge.ID)
		if !edgeMatchesScan(edge, options) || key <= cursor.After {
			continue
		}
		items = append(items, edge)
		if len(items) > normalizedScanLimit(options.Limit) {
			break
		}
	}
	page := edgeScanPage("", version, items, options, len(items) > normalizedScanLimit(options.Limit), "")
	return page.Edges, page.NextCursor
}

func edgeScanPage(tenantID string, version int64, items []graph.Edge, options EdgeScanOptions, hasMore bool, catalogHash string) EdgeScanResult {
	limit := normalizedScanLimit(options.Limit)
	page := items
	if len(page) > limit {
		page = page[:limit]
	}
	next := ""
	if hasMore && len(page) > 0 {
		last := page[len(page)-1]
		next = encodeScanCursor(scanCursor{Version: version, CatalogHash: catalogHash, After: scanKey(last.Type+"\x00"+edgeShardID(last.From), last.ID), Query: edgeScanQueryHash(options)})
	}
	return EdgeScanResult{TenantID: tenantID, Version: version, Edges: append([]graph.Edge(nil), page...), NextCursor: next, IndexedRead: true}
}

func sortedScanEdges(edges map[string]graph.Edge) []graph.Edge {
	items := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		items = append(items, edge)
	}
	sort.Slice(items, func(i, j int) bool {
		left := scanKey(items[i].Type+"\x00"+edgeShardID(items[i].From), items[i].ID)
		right := scanKey(items[j].Type+"\x00"+edgeShardID(items[j].From), items[j].ID)
		return left < right
	})
	return items
}

func edgeMatchesScan(edge graph.Edge, options EdgeScanOptions) bool {
	if options.Type != "" && edge.Type != options.Type {
		return false
	}
	if options.From != "" && edge.From != options.From {
		return false
	}
	if options.FromShard != "" && !indexShardIDMatches(edge.From, options.FromShard) {
		return false
	}
	return edgeMatchesSource(edge, options.Source)
}

func edgeMatchesSource(edge graph.Edge, source string) bool {
	if source == "" {
		return true
	}
	if edge.Source == source || (edge.ExistenceSource != nil && edge.ExistenceSource.Source == source) {
		return true
	}
	for _, item := range edge.Sources {
		if item.Source == source {
			return true
		}
	}
	return false
}

type edgeShardTarget struct {
	RelationType string
	Shard        string
}

func edgeShardSpec(catalog IndexCatalog, relationType string, shard string) (EdgeShard, bool) {
	for _, spec := range catalog.EdgeShards {
		if spec.RelationType == relationType && spec.Shard == shard {
			return spec, true
		}
	}
	return EdgeShard{}, false
}

func edgeShardReadable(shard EdgeShardData, tenantID string, version int64, spec EdgeShard) bool {
	if !indexTenantMatches(shard.TenantID, tenantID) ||
		shard.RelationType != spec.RelationType ||
		shard.Shard != spec.Shard ||
		shard.Version > version {
		return false
	}
	return spec.ContentHash != "" && edgeShardContentHash(shard) == spec.ContentHash
}

func (options *EdgeScanOptions) normalize() {
	options.Type = strings.TrimSpace(options.Type)
	options.From = strings.TrimSpace(options.From)
	options.FromShard = strings.TrimSpace(options.FromShard)
	options.Source = strings.TrimSpace(options.Source)
}
