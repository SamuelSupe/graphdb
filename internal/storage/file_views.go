package storage

import (
	"context"
	"sync"
)

type localViewGate struct {
	refs, readers, waiting int
	writer                 bool
	changed                chan struct{}
}

// PinReadView protects even files that a read has not opened yet. GC, purge,
// and restore wait for existing views and prevent new ones until publication.
func (s *TenantStore) PinReadView(ctx context.Context, tenantID string) (func(), error) {
	return s.lockReadViews(ctx, tenantID, false)
}

func (s *TenantStore) lockReadViews(ctx context.Context, tenantID string, write bool) (func(), error) {
	return s.lockLocalGate(ctx, tenantID, write, false)
}

func (s *TenantStore) beginIngestAcceptance(ctx context.Context, tenantID string) (func(), error) {
	return s.lockLocalGate(ctx, tenantID, false, true)
}

func (s *TenantStore) pauseLocalIngest(ctx context.Context, tenantID string) (release func(), err error) {
	if admission, ok := ctx.Value(taskIngestAdmissionKey{}).(*taskExecutionAdmission); ok &&
		s.localFileStore() != nil && s.ingestBarrier != nil {
		// WAL backpressure may require compact. Do not hold either task slot
		// while draining, or while waiting for another direct writer to drain.
		admission.release()
		defer func() {
			if !admission.acquire(ctx) && err == nil {
				release()
				release = nil
				err = ctx.Err()
			}
		}()
	}
	release, err = s.lockLocalGate(ctx, tenantID, true, true)
	if err != nil {
		return nil, err
	}
	if s.localFileStore() != nil && s.ingestBarrier != nil {
		if err := s.ingestBarrier(ctx, tenantID); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

func (s *TenantStore) lockLocalGate(ctx context.Context, tenantID string, write, admission bool) (func(), error) {
	files := exclusiveFileStore(s.Objects)
	if files == nil || tenantID == "" {
		return func() {}, nil
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return func() {}, nil
	}
	releaseOperation, err := files.beginLifecycleOperation(ctx)
	if err != nil {
		return nil, err
	}
	r := files.runtime
	r.mu.Lock()
	gates := r.views
	if admission {
		if r.ingestAdmissions == nil {
			r.ingestAdmissions = make(map[string]*localViewGate)
		}
		gates = r.ingestAdmissions
	}
	key := s.tenantObjectPrefix(tenantID)
	g := gates[key]
	if g == nil {
		g = &localViewGate{changed: make(chan struct{})}
		gates[key] = g
	}
	g.refs++
	if write {
		g.waiting++
	}
	finishRef := func() {
		g.refs--
		if g.refs == 0 {
			delete(gates, key)
		}
		close(g.changed)
		g.changed = make(chan struct{})
	}
	for {
		if err := ctx.Err(); err != nil {
			if write {
				g.waiting--
			}
			finishRef()
			r.mu.Unlock()
			releaseOperation()
			return nil, err
		}
		if !g.writer && ((write && g.readers == 0) || (!write && g.waiting == 0)) {
			if write {
				g.waiting--
				g.writer = true
			} else {
				g.readers++
			}
			r.mu.Unlock()
			return func() {
				r.mu.Lock()
				if write {
					g.writer = false
				} else {
					g.readers--
				}
				finishRef()
				r.mu.Unlock()
				releaseOperation()
			}, nil
		}
		changed := g.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.mu.Lock()
	}
}

// ReadViewContext lets a shared cache load retain the request's view after its
// HTTP caller cancels. This prevents GC from unlinking files still being loaded.
func (s *TenantStore) ReadViewContext(ctx context.Context, tenantID string) (context.Context, func(), error) {
	if s.localFileStore() == nil || tenantID == "" {
		return ctx, func() {}, nil
	}
	key := localViewContextKey{store: s.localFileStore(), prefix: s.Prefix, tenant: tenantID}
	if view, ok := ctx.Value(key).(*localViewReference); ok {
		view.mu.Lock()
		if view.refs > 0 {
			view.refs++
			view.mu.Unlock()
			return ctx, sync.OnceFunc(view.release), nil
		}
		view.mu.Unlock()
	}
	release, err := s.PinReadView(ctx, tenantID)
	if err != nil {
		return ctx, nil, err
	}
	view := &localViewReference{refs: 1, unpin: release}
	return context.WithValue(ctx, key, view), sync.OnceFunc(view.release), nil
}

type localViewContextKey struct {
	store  *FileStore
	prefix string
	tenant string
}

type localViewReference struct {
	mu    sync.Mutex
	refs  int
	unpin func()
}

func (v *localViewReference) release() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.refs--
	if v.refs == 0 {
		v.unpin()
	}
}
