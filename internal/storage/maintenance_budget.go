package storage

import (
	"context"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"golang.org/x/sync/semaphore"
)

const defaultMaintenanceBytes int64 = 512 << 20

type maintenanceBudgetKey struct{}

type maintenanceResources struct {
	mu              sync.Mutex
	bytes           int64
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
	if !acquireTaskSlot(ctx, resources.builds) {
		return ctx, nil, ctx.Err()
	}
	weight := min(bytes, limit)
	if err := budget.Acquire(ctx, weight); err != nil {
		releaseTaskSlot(resources.builds)
		return ctx, nil, err
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
