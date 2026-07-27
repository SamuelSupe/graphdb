package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

func (s *TenantStore) CurrentScanCatalog(ctx context.Context, tenantID string) (catalog IndexCatalog, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.scan.current_catalog", tenantTraceAttr(tenantID))
	defer func() {
		span.SetAttributes(
			attribute.Bool("graphdb.scan.catalog_available", err == nil),
			attribute.Int64("graphdb.scan.catalog_version", catalog.Version),
			attribute.Int("graphdb.scan.entity_pages", len(catalog.EntityPages)),
			attribute.Int("graphdb.scan.edge_shards", len(catalog.EdgeShards)),
		)
		spanErr := err
		if errors.Is(err, ErrNotFound) {
			spanErr = nil
		}
		endStorageSpan(span, spanErr)
	}()
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, err
	}
	manifestVersion, err := s.CurrentVersion(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, err
	}
	catalog, err = s.GetIndexCatalogAtVersion(
		ctx, tenantID, manifestVersion,
	)
	if err != nil {
		return IndexCatalog{}, err
	}
	if catalog.Version <= 0 || catalog.Version > manifestVersion {
		return IndexCatalog{}, ErrNotFound
	}
	return catalog, nil
}

func (s *TenantStore) ListEntitiesFromCatalog(ctx context.Context, tenantID string, catalog IndexCatalog, options EntityScanOptions) (result EntityScanResult, err error) {
	stats := newEntityScanTraceStats()
	stats.path = "catalog_index_pages"
	stats.catalogVersion = catalog.Version
	stats.catalogPages = len(catalog.EntityPages)
	ctx, span := startStorageSpan(ctx, "graphdb.storage.scan.entities_from_catalog", append(entityScanTraceAttrs(tenantID, options),
		attribute.Int64("graphdb.scan.catalog_version", catalog.Version),
	)...)
	defer func() {
		span.SetAttributes(append(entityScanResultTraceAttrs(result), stats.attrs()...)...)
		endStorageSpan(span, err)
	}()
	if err := validateScanCatalog(tenantID, catalog); err != nil {
		return EntityScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, catalog.Version, entityScanQueryHash(options))
	if err != nil {
		return EntityScanResult{}, err
	}
	result, ok, err := s.listEntitiesFromPages(ctx, tenantID, catalog.Version, catalog, options, cursor, stats)
	if err != nil {
		return EntityScanResult{}, err
	}
	if !ok {
		return EntityScanResult{}, ErrNotFound
	}
	return result, nil
}

func (s *TenantStore) ListEdgesFromCatalog(ctx context.Context, tenantID string, catalog IndexCatalog, options EdgeScanOptions) (result EdgeScanResult, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.scan.edges_from_catalog", append(edgeScanTraceAttrs(tenantID, options),
		attribute.Int64("graphdb.scan.catalog_version", catalog.Version),
	)...)
	defer func() {
		span.SetAttributes(edgeScanResultTraceAttrs(result)...)
		endStorageSpan(span, err)
	}()
	if err := validateScanCatalog(tenantID, catalog); err != nil {
		return EdgeScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, catalog.Version, edgeScanQueryHash(options))
	if err != nil {
		return EdgeScanResult{}, err
	}
	result, ok, err := s.listEdgesFromShards(ctx, tenantID, catalog.Version, catalog, options, cursor)
	if err != nil {
		return EdgeScanResult{}, err
	}
	if !ok {
		return EdgeScanResult{}, ErrNotFound
	}
	return result, nil
}

func validateScanCatalog(tenantID string, catalog IndexCatalog) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		return fmt.Errorf("index catalog tenant mismatch: path tenant %q contains tenant %q", tenantID, catalog.TenantID)
	}
	if catalog.Version <= 0 {
		return ErrNotFound
	}
	return nil
}

func scanCatalogContentHash(catalog IndexCatalog) string {
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return ""
	}
	return hash
}

func entityPageSpecMap(catalog IndexCatalog) map[string]EntityPageSpec {
	specs := make(map[string]EntityPageSpec, len(catalog.EntityPages))
	for _, spec := range catalog.EntityPages {
		specs[spec.Shard] = spec
	}
	return specs
}

func edgeShardSpecMap(catalog IndexCatalog) map[string]EdgeShard {
	specs := make(map[string]EdgeShard, len(catalog.EdgeShards))
	for _, spec := range catalog.EdgeShards {
		specs[edgeShardTargetKey(spec.RelationType, spec.Shard)] = spec
	}
	return specs
}

func entityScanStart(shards []string, after string) int {
	separator := strings.IndexByte(after, 0)
	if separator < 0 {
		return 0
	}
	shard := after[:separator]
	return sort.SearchStrings(shards, shard)
}

func edgeScanStart(targets []edgeShardTarget, after string) int {
	first := strings.IndexByte(after, 0)
	if first < 0 {
		return 0
	}
	secondOffset := strings.IndexByte(after[first+1:], 0)
	if secondOffset < 0 {
		return 0
	}
	group := after[:first+1+secondOffset]
	return sort.Search(len(targets), func(i int) bool {
		return edgeShardTargetKey(targets[i].RelationType, targets[i].Shard) >= group
	})
}

func edgeShardTargetKey(relationType string, shard string) string {
	return relationType + "\x00" + shard
}
