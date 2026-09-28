package storage

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type queryReadMemoKey struct{}

type queryReadMemo struct {
	store *TenantStore
	mu    sync.Mutex

	tenantID    string
	manifest    Manifest
	manifestErr error
	manifestSet bool
	catalog     IndexCatalog
	catalogErr  error
	catalogSet  bool
	catalogVer  int64
	reverse     ReverseIndexCatalog
	reverseErr  error
	reverseSet  bool
	reverseVer  int64
}

func WithQueryReadMemo(ctx context.Context) context.Context {
	if _, ok := ctx.Value(queryReadMemoKey{}).(*queryReadMemo); ok {
		return ctx
	}
	return context.WithValue(ctx, queryReadMemoKey{}, &queryReadMemo{})
}

func (s *TenantStore) QueryManifest(ctx context.Context, tenantID string) (manifest Manifest, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.query.execute.current_manifest", attribute.String("graphdb.tenant", tenantID))
	cached := false
	defer func() {
		setReadMemoSpanAttributes(span, cached, manifest.Version)
		endStorageSpan(span, err)
	}()
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		version, versionErr := s.CurrentVersion(ctx, tenantID)
		return Manifest{TenantID: tenantID, Version: version}, versionErr
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(s, tenantID)
	if memo.manifestSet {
		cached = true
		return memo.manifest, memo.manifestErr
	}
	version, versionErr := s.CurrentVersion(ctx, tenantID)
	memo.manifest = Manifest{TenantID: tenantID, Version: version}
	memo.manifestErr = versionErr
	memo.manifestSet = true
	return memo.manifest, memo.manifestErr
}

func (s *TenantStore) QueryCatalog(ctx context.Context, tenantID string, expectedVersion int64) (catalog IndexCatalog, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.query.execute.current_index_catalog",
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
		if errors.Is(err, ErrNotFound) {
			spanErr = nil
		}
		endStorageSpan(span, spanErr)
	}()
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		return s.GetIndexCatalogAtVersion(ctx, tenantID, expectedVersion)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(s, tenantID)
	if memo.catalogSet &&
		(memo.catalogVer == expectedVersion ||
			memo.catalogErr == nil &&
				memo.catalog.Version == expectedVersion) {
		cached = true
		return memo.catalog, memo.catalogErr
	}
	memo.catalog, memo.catalogErr = s.GetIndexCatalogAtVersion(ctx, tenantID, expectedVersion)
	memo.catalogSet = true
	memo.catalogVer = expectedVersion
	return memo.catalog, memo.catalogErr
}

func (s *TenantStore) QueryReverseCatalog(
	ctx context.Context,
	tenantID string,
	version int64,
) (catalog ReverseIndexCatalog, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.query.execute.current_reverse_index_catalog",
		attribute.String("graphdb.tenant", tenantID),
		attribute.Int64("graphdb.index.expected_version", version),
	)
	cached := false
	defer func() {
		setReadMemoSpanAttributes(span, cached, catalog.Version)
		spanErr := err
		if errors.Is(err, ErrNotFound) {
			spanErr = nil
		}
		endStorageSpan(span, spanErr)
	}()
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		return s.GetReverseIndexCatalog(ctx, tenantID, version)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(s, tenantID)
	if memo.reverseSet && memo.reverseVer == version {
		cached = true
		return memo.reverse, memo.reverseErr
	}
	memo.reverse, memo.reverseErr =
		s.GetReverseIndexCatalog(ctx, tenantID, version)
	memo.reverseSet = true
	memo.reverseVer = version
	return memo.reverse, memo.reverseErr
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

func (m *queryReadMemo) resetForTenant(store *TenantStore, tenantID string) {
	if m.store == store && m.tenantID == tenantID {
		return
	}
	m.store = store
	m.tenantID = tenantID
	m.manifest = Manifest{}
	m.manifestErr = nil
	m.manifestSet = false
	m.catalog = IndexCatalog{}
	m.catalogErr = nil
	m.catalogSet = false
	m.catalogVer = 0
	m.reverse = ReverseIndexCatalog{}
	m.reverseErr = nil
	m.reverseSet = false
	m.reverseVer = 0
}
