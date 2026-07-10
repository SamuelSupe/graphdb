package httpapi

import (
	"context"
	"sync"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
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

func (s *Server) currentQueryManifest(ctx context.Context, tenantID string) (storage.Manifest, error) {
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		return s.Store.CurrentManifest(ctx, tenantID)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(tenantID)
	if memo.manifestSet && memo.tenantID == tenantID {
		return memo.manifest, memo.manifestErr
	}
	memo.tenantID = tenantID
	memo.manifest, memo.manifestErr = s.Store.CurrentManifest(ctx, tenantID)
	memo.manifestSet = true
	return memo.manifest, memo.manifestErr
}

func (s *Server) currentQueryCatalog(ctx context.Context, tenantID string, expectedVersion int64) (storage.IndexCatalog, error) {
	memo, _ := ctx.Value(queryReadMemoKey{}).(*queryReadMemo)
	if memo == nil {
		return s.Store.GetIndexCatalogAtVersion(ctx, tenantID, expectedVersion)
	}
	memo.mu.Lock()
	defer memo.mu.Unlock()
	memo.resetForTenant(tenantID)
	if memo.catalogSet && memo.tenantID == tenantID {
		return memo.catalog, memo.catalogErr
	}
	memo.tenantID = tenantID
	memo.catalog, memo.catalogErr = s.Store.GetIndexCatalogAtVersion(ctx, tenantID, expectedVersion)
	memo.catalogSet = true
	return memo.catalog, memo.catalogErr
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
