package storage

import (
	"context"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"golang.org/x/sync/semaphore"
)

const defaultMaintenanceBytes int64 = 512 << 20

type maintenanceBudgetKey struct{}

type maintenanceResources struct {
	mu              sync.Mutex
	bytes           int64
	queued          int64
	memory          *semaphore.Weighted
	builds, encodes chan struct{}
}

func maintenanceGraphBytes(g *graph.Graph) int64 {
	if g == nil {
		return 0
	}
	return writeCacheBytesForGraph(g, g.CachedLogicalSize())
}

// Estimates bound overlapping builds, not process RSS. One oversized build may
// run alone so a tenant larger than the budget can still compact and recover.
func (s *TenantStore) admitMaintenance(ctx context.Context, bytes int64) (context.Context, func(), error) {
	if s.localFileStore() == nil || ctx.Value(maintenanceBudgetKey{}) == s {
		return ctx, func() {}, nil
	}
	started := time.Now()
	defer func() { s.recordMaintenance("memory_wait", started) }()
	resources := s.maintenance
	limit := s.backgroundIndexByteLimit()
	bytes = max(1, bytes)
	resources.mu.Lock()
	if resources.memory == nil {
		resources.memory = semaphore.NewWeighted(limit)
	}
	budget := resources.memory
	resources.mu.Unlock()
	weight := min(bytes, limit)
	if err := budget.Acquire(ctx, weight); err != nil {
		return ctx, nil, err
	}
	if !acquireTaskSlot(ctx, resources.builds) {
		budget.Release(weight)
		return ctx, nil, ctx.Err()
	}
	resources.mu.Lock()
	resources.bytes += bytes
	s.recordMaintenanceMemory("active", resources.bytes)
	resources.mu.Unlock()
	return context.WithValue(ctx, maintenanceBudgetKey{}, s), sync.OnceFunc(func() {
		resources.mu.Lock()
		resources.bytes -= bytes
		s.recordMaintenanceMemory("active", resources.bytes)
		resources.mu.Unlock()
		budget.Release(weight)
		releaseTaskSlot(resources.builds)
	}), nil
}

type MaintenanceObserver interface {
	RecordMaintenancePhase(phase string, duration time.Duration)
	RecordMaintenanceMemory(pool string, bytes int64)
}

func (s *TenantStore) recordMaintenance(phase string, started time.Time) {
	if observer, ok := s.backpressureObserver.(MaintenanceObserver); ok {
		observer.RecordMaintenancePhase(phase, time.Since(started))
	}
}

func (s *TenantStore) recordMaintenanceMemory(pool string, bytes int64) {
	if observer, ok := s.backpressureObserver.(MaintenanceObserver); ok {
		observer.RecordMaintenanceMemory(pool, bytes)
	}
}

// A queued update carries its reservation into execution. Releasing it and
// reacquiring would let later work consume the memory needed to drain the queue.
type maintenanceReservation struct {
	store  *TenantStore
	bytes  int64
	active bool
	once   sync.Once
}

func (s *TenantStore) reserveIndexMemory(bytes int64) *maintenanceReservation {
	resources := s.maintenance
	limit := s.backgroundIndexByteLimit()
	if bytes <= 0 || bytes > limit {
		return nil
	}
	resources.mu.Lock()
	defer resources.mu.Unlock()
	if resources.memory == nil {
		resources.memory = semaphore.NewWeighted(limit)
	}
	if !resources.memory.TryAcquire(bytes) {
		return nil
	}
	resources.queued += bytes
	s.recordMaintenanceMemory("pending_indexes", resources.queued)
	return &maintenanceReservation{store: s, bytes: bytes}
}

func (r *maintenanceReservation) resize(bytes int64) bool {
	resources := r.store.maintenance
	resources.mu.Lock()
	defer resources.mu.Unlock()
	if bytes > r.store.backgroundIndexByteLimit() {
		return false
	}
	delta := bytes - r.bytes
	if delta > 0 && !resources.memory.TryAcquire(delta) {
		return false
	}
	if delta < 0 {
		resources.memory.Release(-delta)
	}
	r.bytes = bytes
	resources.queued += delta
	r.store.recordMaintenanceMemory("pending_indexes", resources.queued)
	return true
}

func (r *maintenanceReservation) activate(ctx context.Context) (context.Context, error) {
	resources := r.store.maintenance
	if !acquireTaskSlot(ctx, resources.builds) {
		return ctx, ctx.Err()
	}
	resources.mu.Lock()
	resources.queued -= r.bytes
	resources.bytes += r.bytes
	r.active = true
	r.store.recordMaintenanceMemory("pending_indexes", resources.queued)
	r.store.recordMaintenanceMemory("active", resources.bytes)
	resources.mu.Unlock()
	return context.WithValue(ctx, maintenanceBudgetKey{}, r.store), nil
}

func (r *maintenanceReservation) release() {
	r.once.Do(func() {
		resources := r.store.maintenance
		resources.mu.Lock()
		if r.active {
			resources.bytes -= r.bytes
		} else {
			resources.queued -= r.bytes
		}
		r.store.recordMaintenanceMemory("pending_indexes", resources.queued)
		r.store.recordMaintenanceMemory("active", resources.bytes)
		resources.mu.Unlock()
		resources.memory.Release(r.bytes)
		if r.active {
			releaseTaskSlot(resources.builds)
		}
	})
}
