package httpapi

import (
	"context"
	"errors"
	"sync"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type queryReadMemoKey struct{}

type queryReadMemo struct {
	mu sync.Mutex

	tenantID    string
	manifest    storage.Manifest
	manifestErr error
	manifestSet bool
	catalog     storage.IndexCatalog
	catalogErr  error
	catalogSet  bool
}

func withQueryReadMemo(ctx context.Context) context.Context {
	if _, ok := ctx.Value(queryReadMemoKey{}).(*queryReadMemo); ok {
		return ctx
	}
	return context.WithValue(ctx, queryReadMemoKey{}, &queryReadMemo{})
}

func (s *Server) currentQueryManifest(ctx context.Context, tenantID string) (manifest storage.Manifest, err error) {
	ctx, span := startAPIPhase(ctx, "current_manifest", attribute.String("graphdb.tenant", tenantID))
	cached := false
	defer func() {
		setReadMemoSpanAttributes(span, cached, manifest.Version)
		endHTTPSpan(span, err)
	}()
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		return s.Store.CurrentManifest(ctx, tenantID)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(tenantID)
	if memo.manifestSet && memo.tenantID == tenantID {
		cached = true
		return memo.manifest, memo.manifestErr
	}
	memo.tenantID = tenantID
	memo.manifest, memo.manifestErr = s.Store.CurrentManifest(ctx, tenantID)
	memo.manifestSet = true
	return memo.manifest, memo.manifestErr
}

func (s *Server) currentQueryCatalog(ctx context.Context, tenantID string, expectedVersion int64) (catalog storage.IndexCatalog, err error) {
	ctx, span := startAPIPhase(ctx, "current_index_catalog",
		attribute.String("graphdb.tenant", tenantID),
		attribute.Int64("graphdb.index.expected_version", expectedVersion),
	)
	cached := false
	defer func() {
		setReadMemoSpanAttributes(span, cached, catalog.Version)
		if span != nil {
			span.SetAttributes(attribute.Bool("graphdb.index.catalog_available", err == nil))
		}
		spanErr := err
		if errors.Is(err, storage.ErrNotFound) {
			spanErr = nil
		}
		endHTTPSpan(span, spanErr)
	}()
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		return s.Store.GetIndexCatalogAtVersion(ctx, tenantID, expectedVersion)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(tenantID)
	if memo.catalogSet && memo.tenantID == tenantID {
		cached = true
		return memo.catalog, memo.catalogErr
	}
	memo.tenantID = tenantID
	memo.catalog, memo.catalogErr = s.Store.GetIndexCatalogAtVersion(ctx, tenantID, expectedVersion)
	memo.catalogSet = true
	return memo.catalog, memo.catalogErr
}

func setReadMemoSpanAttributes(span trace.Span, cached bool, version int64) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.Bool("graphdb.read_memo.hit", cached),
		attribute.Int64("graphdb.read.version", version),
	)
}

func (m *queryReadMemo) resetForTenant(tenantID string) {
	if m.tenantID == "" || m.tenantID == tenantID {
		return
	}
	m.manifest = storage.Manifest{}
	m.manifestErr = nil
	m.manifestSet = false
	m.catalog = storage.IndexCatalog{}
	m.catalogErr = nil
	m.catalogSet = false
}
