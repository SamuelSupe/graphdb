package observability

import (
	"context"
	"io"
	"sync"
	"time"
)

type Observability struct {
	Metrics            *Metrics
	Logger             *Logger
	SlowQueryThreshold time.Duration

	mu      sync.Mutex
	tenants map[string]struct{}
}

func New(out io.Writer, slowQueryThreshold time.Duration) *Observability {
	return &Observability{
		Metrics:            NewMetrics(),
		Logger:             NewLogger(out),
		SlowQueryThreshold: slowQueryThreshold,
		tenants:            map[string]struct{}{},
	}
}

func (o *Observability) RegisterTenant(tenantID string) {
	if o == nil || tenantID == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, exists := o.tenants[tenantID]; exists {
		return
	}
	if len(o.tenants) >= 4096 {
		return
	}
	o.tenants[tenantID] = struct{}{}
}

func (o *Observability) UnregisterTenant(tenantID string) {
	if o == nil || tenantID == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.tenants, tenantID)
	o.Metrics.forgetTenant(tenantID)
}

func (o *Observability) HasTenant(tenantID string) bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.tenants[tenantID]
	return ok
}

func (o *Observability) Tenants() []string {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	values := make([]string, 0, len(o.tenants))
	for tenantID := range o.tenants {
		values = append(values, tenantID)
	}
	return values
}

func (o *Observability) StartIndexHealthMonitor(ctx context.Context, interval time.Duration, check func(context.Context, string) (string, int, error)) {
	if o == nil || interval <= 0 || check == nil {
		return
	}
	go o.RunIndexHealthMonitor(ctx, interval, check)
}

func (o *Observability) RunIndexHealthMonitor(ctx context.Context, interval time.Duration, check func(context.Context, string) (string, int, error)) {
	if o == nil || interval <= 0 || check == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			o.checkKnownTenantIndexes(ctx, check)
		case <-ctx.Done():
			return
		}
	}
}

func (o *Observability) checkKnownTenantIndexes(ctx context.Context, check func(context.Context, string) (string, int, error)) {
	for _, tenantID := range o.Tenants() {
		if ctx.Err() != nil {
			return
		}
		status, issues, err := check(ctx, tenantID)
		if err != nil {
			if o.Logger != nil {
				o.Logger.Error("index_health_check_failed", map[string]any{"tenant": tenantID, "error": err.Error()})
			}
			status, issues = "error", 1
		}
		o.mu.Lock()
		if _, present := o.tenants[tenantID]; present && o.Metrics != nil {
			o.Metrics.RecordIndexHealth(tenantID, status, issues)
		}
		o.mu.Unlock()
	}
}
