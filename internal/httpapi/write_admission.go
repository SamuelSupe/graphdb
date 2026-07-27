package httpapi

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type WriteAdmission struct {
	global       chan struct{}
	perTenantMax int
	queueTimeout time.Duration
	mu           sync.Mutex
	tenants      map[string]*writeTenantAdmission
}

type writeTenantAdmission struct {
	slot chan struct{}
	refs int
}

func NewWriteAdmission(maxGlobal int, maxPerTenant int, queueTimeout time.Duration) *WriteAdmission {
	admission := &WriteAdmission{perTenantMax: maxPerTenant, queueTimeout: queueTimeout, tenants: map[string]*writeTenantAdmission{}}
	if maxGlobal > 0 {
		admission.global = make(chan struct{}, maxGlobal)
	}
	return admission
}

func (a *WriteAdmission) Acquire(ctx context.Context, tenantID string) (func(), time.Duration, error) {
	start := time.Now()
	if a == nil {
		return func() {}, 0, nil
	}
	ctx, cancel := a.withQueueTimeout(ctx)
	tenant := a.retainTenant(tenantID)
	if err := acquireWriteSlot(ctx, writeTenantSlot(tenant)); err != nil {
		a.releaseTenant(tenantID, tenant)
		cancel()
		return nil, time.Since(start), err
	}
	if err := acquireWriteSlot(ctx, a.global); err != nil {
		releaseSlot(writeTenantSlot(tenant))
		a.releaseTenant(tenantID, tenant)
		cancel()
		return nil, time.Since(start), err
	}
	return func() {
		releaseSlot(a.global)
		releaseSlot(writeTenantSlot(tenant))
		a.releaseTenant(tenantID, tenant)
		cancel()
	}, time.Since(start), nil
}

func (a *WriteAdmission) withQueueTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.queueTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, a.queueTimeout)
}

func (a *WriteAdmission) retainTenant(tenantID string) *writeTenantAdmission {
	if a.perTenantMax <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	tenant := a.tenants[tenantID]
	if tenant == nil {
		tenant = &writeTenantAdmission{slot: make(chan struct{}, a.perTenantMax)}
		a.tenants[tenantID] = tenant
	}
	tenant.refs++
	return tenant
}

func (a *WriteAdmission) releaseTenant(tenantID string, tenant *writeTenantAdmission) {
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

func writeTenantSlot(tenant *writeTenantAdmission) chan struct{} {
	if tenant == nil {
		return nil
	}
	return tenant.slot
}

func acquireWriteSlot(ctx context.Context, slot chan struct{}) error {
	if slot == nil {
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("write admission queue timeout")
	}
	select {
	case slot <- struct{}{}:
		if ctx.Err() != nil {
			releaseSlot(slot)
			return fmt.Errorf("write admission queue timeout")
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("write admission queue timeout")
	}
}
