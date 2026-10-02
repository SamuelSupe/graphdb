package ha

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type tenantAdmission struct {
	active  int
	paused  bool
	changed chan struct{}
}

func (c *Cluster) tenantAdmission(tenant string) *tenantAdmission {
	if c.admissions == nil {
		c.admissions = make(map[string]*tenantAdmission)
	}
	entry := c.admissions[tenant]
	if entry == nil {
		entry = &tenantAdmission{changed: make(chan struct{})}
		c.admissions[tenant] = entry
	}
	return entry
}

func (c *Cluster) admitTenantMutation(cmd command) (func(), error) {
	if cmd.Tenant == "" || (cmd.Kind != "http" && cmd.Kind != "accept" && cmd.Kind != "sharding") {
		return func() {}, nil
	}
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	entry := c.tenantAdmission(cmd.Tenant)
	if entry.paused {
		taskControl := false
		if cmd.Kind == "http" && cmd.Method == http.MethodPost {
			uri, err := url.ParseRequestURI(cmd.URI)
			if err == nil {
				taskControl = uri.Path == "/v1/tasks" || (uri.Path == "/v1/indexes/rebuild" && uri.Query().Get("async") == "true") ||
					(strings.HasPrefix(uri.Path, "/v1/tasks/") && (strings.HasSuffix(uri.Path, "/cancel") || strings.HasSuffix(uri.Path, "/retry")))
			}
		}
		if !taskControl {
			c.metrics.Event("maintenance_write_deferred")
			return nil, &storage.BackpressureError{RetryAfter: time.Second, Reasons: []storage.BackpressureReason{{Code: "maintenance_pending", Message: "tenant maintenance is preparing; retry after it finishes"}}}
		}
	}
	entry.active++
	return func() {
		c.admissionMu.Lock()
		defer c.admissionMu.Unlock()
		entry.active--
		close(entry.changed)
		entry.changed = make(chan struct{})
		if entry.active == 0 && !entry.paused {
			delete(c.admissions, cmd.Tenant)
		}
	}, nil
}

// Only the current leader admits mutations. A local pause need not enter the
// log: late proposals or a leader change still invalidate the prepared token.
// Draining accepted WAL prevents a pause from stranding acknowledged writes.
// The maintenance worker serializes pauses with maintenanceMu.
func (c *Cluster) pauseTenantWrites(ctx context.Context, tenant string) (func(), error) {
	c.admissionMu.Lock()
	entry := c.tenantAdmission(tenant)
	entry.paused = true
	c.admissionMu.Unlock()
	c.metrics.Event("maintenance_write_pause")
	resume := sync.OnceFunc(func() {
		c.admissionMu.Lock()
		defer c.admissionMu.Unlock()
		entry.paused = false
		if entry.active == 0 {
			delete(c.admissions, tenant)
		}
	})
	for {
		c.admissionMu.Lock()
		active, changed := entry.active, entry.changed
		c.admissionMu.Unlock()
		if active == 0 {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			resume()
			return nil, ctx.Err()
		}
	}
	for {
		if err := c.Node.ReadBarrier(ctx); err != nil {
			resume()
			return nil, err
		}
		queue, err := c.App.pendingSnapshot(ctx)
		if err != nil {
			resume()
			return nil, err
		}
		pending := false
		for _, request := range queue {
			pending = pending || request.tenant == tenant
		}
		if !pending {
			return resume, nil
		}
		if err := c.flushPendingTenant(ctx, tenant); err != nil {
			resume()
			return nil, err
		}
	}
}

func (c *Cluster) maintenanceWritesPaused() bool {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	for _, entry := range c.admissions {
		if entry.paused {
			return true
		}
	}
	return false
}

func (c *Cluster) pauseMaintenance(ctx context.Context, task storage.Task) (context.Context, func(), error) {
	operation, cancel := context.WithCancel(ctx)
	resume, err := c.pauseTenantWrites(operation, task.TenantID)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-operation.Done():
				return
			case <-ticker.C:
				if c.Node.LeaderID() != c.Node.ID() {
					cancel()
					return
				}
				current, err := c.App.Store.GetTask(operation, task.TenantID, task.ID)
				if err == nil && current.Status == storage.TaskStatusCanceled {
					cancel()
					return
				}
			}
		}
	}()
	return operation, sync.OnceFunc(func() { cancel(); <-done; resume() }), nil
}
