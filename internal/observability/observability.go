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
	o.tenants[tenantID] = struct{}{}
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
	go func() {
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
	}()
}

func (o *Observability) checkKnownTenantIndexes(ctx context.Context, check func(context.Context, string) (string, int, error)) {
	for _, tenantID := range o.Tenants() {
		status, issues, err := check(ctx, tenantID)
		if err != nil {
			if o.Logger != nil {
				o.Logger.Error("index_health_check_failed", map[string]any{"tenant": tenantID, "error": err.Error()})
			}
			if o.Metrics != nil {
				o.Metrics.RecordIndexHealth(tenantID, "error", 1)
			}
			continue
		}
		if o.Metrics != nil {
			o.Metrics.RecordIndexHealth(tenantID, status, issues)
		}
	}
}
