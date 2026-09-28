package httpapi

import (
	"context"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/maintenance"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type MaintenanceReport = maintenance.MaintenanceReport
type TenantMaintenanceReport = maintenance.TenantMaintenanceReport
type StorageLayoutFinding = maintenance.StorageLayoutFinding

func (s *Server) maintenanceRuntime() *maintenance.Runner {
	s.maintenanceOnce.Do(func() {
		s.maintenance = &maintenance.Runner{Store: s.Store,
			Usage: func(ctx context.Context, tenant string) (storage.TenantUsageReport, error) {
				return s.cachedTenantUsage(ctx, tenant, time.Now().UTC())
			},
			Audit: func(event, tenant string, err error, fields map[string]any) {
				if err != nil {
					s.auditError(event, tenant, err, fields)
				} else {
					s.auditInfo(event, tenant, fields)
				}
			},
		}
	})
	return s.maintenance
}

func (s *Server) StartMaintenanceLoop(ctx context.Context, interval time.Duration) {
	if s.writeAllowed() {
		s.maintenanceRuntime().Start(ctx, interval)
	}
}

func (s *Server) runMaintenanceOnce(ctx context.Context, now time.Time) MaintenanceReport {
	if !s.writeAllowed() {
		return MaintenanceReport{StartedAt: now, FinishedAt: time.Now().UTC()}
	}
	return s.maintenanceRuntime().Run(ctx, now)
}
