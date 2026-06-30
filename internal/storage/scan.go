package storage

import (
	"context"
	"errors"

	"graphdb/internal/graph"
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

func (s *TenantStore) scanCatalog(ctx context.Context, tenantID string, maxVersion int64) (IndexCatalog, bool, error) {
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return IndexCatalog{}, false, nil
	}
	if err != nil {
		return IndexCatalog{}, false, err
	}
	return catalog, catalog.Version > 0 && catalog.Version <= maxVersion, nil
}
