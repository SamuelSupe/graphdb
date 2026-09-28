package storage

import (
	"context"
	"errors"
	"strings"
)

type gcViewKey struct{}

var errGCViewPinned = errors.New("gc file is pinned by a read view")

type gcView struct {
	files          *FileStore
	gate           *localViewGate
	rootGeneration uint64
}

// Retire only files already proven unreachable by the current roots. A view
// admitted before that proof may still open an old file later. New views cannot
// discover an orphan through a current manifest/catalog. Queued index work pins
// its inputs before creating or publishing files, just like foreground reads.
// Root publication during a batch invalidates the proof, including same-version
// catalog rebuilds. File replacement also removes the old retirement marker.
func (v *gcView) canDelete(key string) bool {
	if v == nil {
		return true
	}
	r := v.files.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	g := v.gate
	if g.rootGeneration != v.rootGeneration {
		return false
	}
	epoch, exists := g.retired[key]
	if !exists {
		epoch = g.clock
	}
	for active := range g.active {
		if active <= epoch {
			if !exists && len(g.retired) < 16384 {
				if g.retired == nil {
					g.retired = make(map[string]uint64)
				}
				g.retired[key] = epoch
			}
			return false
		}
	}
	delete(g.retired, key)
	return true
}

func (s *TenantStore) gcView(tenant string) *gcView {
	files := s.localFileStore()
	if files == nil {
		return nil
	}
	r := files.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	g := r.views[s.tenantObjectPrefix(tenant)]
	if g == nil {
		return nil
	}
	return &gcView{files: files, gate: g, rootGeneration: g.rootGeneration}
}

func (s *TenantStore) protectCatalogObjects(tenant string, catalog IndexCatalog) {
	files := s.localFileStore()
	if files == nil {
		return
	}
	r := files.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	g := r.views[s.tenantObjectPrefix(tenant)]
	if g == nil || len(g.retired) == 0 {
		return
	}
	for _, index := range catalog.Indexes {
		for _, object := range index.Objects {
			delete(g.retired, object.Key)
		}
	}
	for _, page := range catalog.EntityPages {
		for _, object := range page.Objects {
			delete(g.retired, object.Key)
		}
	}
	for _, shard := range catalog.EdgeShards {
		for _, object := range shard.Objects {
			delete(g.retired, object.Key)
		}
	}
}

func (r *fileRuntime) invalidateViewFile(key string) {
	for end := strings.LastIndexByte(key, '/'); end >= 0; end = strings.LastIndexByte(key[:end], '/') {
		view := r.views[key[:end+1]]
		if view == nil {
			continue
		}
		delete(view.retired, key)
		name := key[end+1:]
		if name == "manifest.parquet" || strings.HasSuffix(name, "/catalog.parquet") || strings.HasSuffix(name, "/catalog.json") || name == "metadata.parquet" {
			view.rootGeneration++
		}
	}
}

func (s *TenantStore) lockGCReadViews(ctx context.Context, tenantID string) (func(), error) {
	// GC is a lifecycle reader, but not a consumer of retired graph versions.
	return s.lockReadViews(ctx, tenantID, false)
}
