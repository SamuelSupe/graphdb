package httpapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"graphdb/internal/query"
)

type QueryAdmission struct {
	global       chan struct{}
	perTenantMax int
	queueTimeout time.Duration
	mu           sync.Mutex
	tenants      map[string]*tenantAdmission
}

type tenantAdmission struct {
	slot chan struct{}
	refs int
}

func NewQueryAdmission(maxGlobal int, maxPerTenant int, queueTimeout time.Duration) *QueryAdmission {
	admission := &QueryAdmission{perTenantMax: maxPerTenant, queueTimeout: queueTimeout, tenants: map[string]*tenantAdmission{}}
	if maxGlobal > 0 {
		admission.global = make(chan struct{}, maxGlobal)
	}
	return admission
}

func (a *QueryAdmission) Acquire(ctx context.Context, tenantID string) (func(), error) {
	if a == nil {
		return func() {}, nil
	}
	ctx, cancel := a.withQueueTimeout(ctx)
	tenant := a.retainTenant(tenantID)
	if err := acquireSlot(ctx, tenantSlot(tenant)); err != nil {
		a.releaseTenant(tenantID, tenant)
		cancel()
		return nil, err
	}
	if err := acquireSlot(ctx, a.global); err != nil {
		releaseSlot(tenantSlot(tenant))
		a.releaseTenant(tenantID, tenant)
		cancel()
		return nil, err
	}
	return func() {
		releaseSlot(a.global)
		releaseSlot(tenantSlot(tenant))
		a.releaseTenant(tenantID, tenant)
		cancel()
	}, nil
}

func (a *QueryAdmission) withQueueTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.queueTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, a.queueTimeout)
}

func (a *QueryAdmission) retainTenant(tenantID string) *tenantAdmission {
	if a.perTenantMax <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	tenant := a.tenants[tenantID]
	if tenant == nil {
		tenant = &tenantAdmission{slot: make(chan struct{}, a.perTenantMax)}
		a.tenants[tenantID] = tenant
	}
	tenant.refs++
	return tenant
}

func (a *QueryAdmission) releaseTenant(tenantID string, tenant *tenantAdmission) {
	if tenant == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.tenants[tenantID]
	if current != tenant {
		return
	}
	tenant.refs--
	if tenant.refs <= 0 && len(tenant.slot) == 0 {
		delete(a.tenants, tenantID)
	}
}

func tenantSlot(tenant *tenantAdmission) chan struct{} {
	if tenant == nil {
		return nil
	}
	return tenant.slot
}

func acquireSlot(ctx context.Context, slot chan struct{}) error {
	if slot == nil {
		return nil
	}
	select {
	case slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: query admission queue timeout", query.ErrLimitExceeded)
	}
}

func releaseSlot(slot chan struct{}) {
	if slot == nil {
		return
	}
	<-slot
}
