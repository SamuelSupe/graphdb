package storage

import (
	"context"
	"errors"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

const (
	defaultScanLimit = 100
	maxScanLimit     = 1000
)

type EntityScanOptions struct {
	Kind       string
	Source     string
	Shard      string
	Limit      int
	Cursor     string
	MinVersion int64
}

type EntityScanResult struct {
	TenantID    string         `json:"tenant_id"`
	Version     int64          `json:"version"`
	Entities    []graph.Entity `json:"entities"`
	NextCursor  string         `json:"next_cursor,omitempty"`
	IndexedRead bool           `json:"indexed_read"`
}

type EdgeScanOptions struct {
	Type       string
	From       string
	FromShard  string
	Source     string
	Limit      int
	Cursor     string
	MinVersion int64
}

type EdgeScanResult struct {
	TenantID    string       `json:"tenant_id"`
	Version     int64        `json:"version"`
	Edges       []graph.Edge `json:"edges"`
	NextCursor  string       `json:"next_cursor,omitempty"`
	IndexedRead bool         `json:"indexed_read"`
}

type scanCursor struct {
	Version     int64  `json:"version"`
	CatalogHash string `json:"catalog_hash,omitempty"`
	After       string `json:"after,omitempty"`
	Query       string `json:"query,omitempty"`
}

func entityScanTraceAttrs(tenantID string, options EntityScanOptions) []attribute.KeyValue {
	return []attribute.KeyValue{
		tenantTraceAttr(tenantID),
		attribute.String("graphdb.scan.resource", "entity"),
		attribute.String("graphdb.scan.kind", options.Kind),
		attribute.String("graphdb.scan.source", options.Source),
		attribute.String("graphdb.scan.shard", options.Shard),
		attribute.Int("graphdb.scan.limit", normalizedScanLimit(options.Limit)),
		attribute.Int64("graphdb.scan.min_version", options.MinVersion),
		attribute.Bool("graphdb.scan.cursor_present", options.Cursor != ""),
	}
}

func edgeScanTraceAttrs(tenantID string, options EdgeScanOptions) []attribute.KeyValue {
	return []attribute.KeyValue{
		tenantTraceAttr(tenantID),
		attribute.String("graphdb.scan.resource", "edge"),
		attribute.String("graphdb.scan.relation_type", options.Type),
		attribute.String("graphdb.scan.source", options.Source),
		attribute.String("graphdb.scan.from_shard", options.FromShard),
		attribute.Bool("graphdb.scan.from_present", options.From != ""),
		attribute.Int("graphdb.scan.limit", normalizedScanLimit(options.Limit)),
		attribute.Int64("graphdb.scan.min_version", options.MinVersion),
		attribute.Bool("graphdb.scan.cursor_present", options.Cursor != ""),
	}
}

func entityScanResultTraceAttrs(result EntityScanResult) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("graphdb.scan.version", result.Version),
		attribute.Int("graphdb.scan.returned", len(result.Entities)),
		attribute.Bool("graphdb.scan.next_cursor_present", result.NextCursor != ""),
		attribute.Bool("graphdb.scan.indexed_read", result.IndexedRead),
	}
}

func edgeScanResultTraceAttrs(result EdgeScanResult) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("graphdb.scan.version", result.Version),
		attribute.Int("graphdb.scan.returned", len(result.Edges)),
		attribute.Bool("graphdb.scan.next_cursor_present", result.NextCursor != ""),
		attribute.Bool("graphdb.scan.indexed_read", result.IndexedRead),
	}
}

func (s *TenantStore) scanCatalog(ctx context.Context, tenantID string, maxVersion int64) (IndexCatalog, bool, error) {
	catalog, err := s.GetIndexCatalogAtVersion(ctx, tenantID, maxVersion)
	if errors.Is(err, ErrNotFound) {
		return IndexCatalog{}, false, nil
	}
	if err != nil {
		return IndexCatalog{}, false, err
	}
	return catalog, catalog.Version > 0 && catalog.Version <= maxVersion, nil
}
