package storage

import (
	"context"
	"fmt"
	"time"
)

type RepairOptions struct {
	Apply    bool               `json:"apply,omitempty"`
	Progress RepairProgressFunc `json:"-"`
}

type RepairReport struct {
	TenantID         string                `json:"tenant_id"`
	ManifestVersion  int64                 `json:"manifest_version"`
	Apply            bool                  `json:"apply"`
	Status           string                `json:"status"`
	CheckedAt        time.Time             `json:"checked_at"`
	Issues           []RepairIssue         `json:"issues,omitempty"`
	Plan             []RepairPlanStep      `json:"plan,omitempty"`
	Actions          []RepairAction        `json:"actions,omitempty"`
	RemainingIssues  []RepairIssue         `json:"remaining_issues,omitempty"`
	IndexHealth      *IndexHealth          `json:"index_health,omitempty"`
	Verification     *IntegrityAuditReport `json:"verification,omitempty"`
	CurrentLayout    int                   `json:"current_layout_version"`
	SupportedLayouts []int                 `json:"supported_layout_versions"`
}

type RepairIssue struct {
	Code         string         `json:"code"`
	Severity     string         `json:"severity"`
	ResourceType string         `json:"resource_type,omitempty"`
	ResourceID   string         `json:"resource_id,omitempty"`
	Message      string         `json:"message"`
	Repairable   bool           `json:"repairable"`
	Details      map[string]any `json:"details,omitempty"`
}

type RepairAction struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type RepairPlanStep struct {
	Type       string   `json:"type"`
	Status     string   `json:"status"`
	IssueCodes []string `json:"issue_codes,omitempty"`
	Message    string   `json:"message,omitempty"`
}

type RepairProgressFunc func(ctx context.Context, action RepairAction, completed int, total int) error

func (s *TenantStore) RepairTenant(ctx context.Context, tenantID string, options RepairOptions) (RepairReport, error) {
	report, err := s.inspectTenantRepair(ctx, tenantID)
	if err != nil {
		return RepairReport{}, err
	}
	report.Apply = options.Apply
	if !options.Apply {
		return report, nil
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return report, err
	}
	working := report
	structuralRepair := false
	if reportNeedsManifestRebuild(working) {
		if err := runRepairAction(ctx, options, &report, "rebuild_manifest", func() (string, error) {
			manifest, err := s.repairManifest(ctx, tenantID)
			return fmt.Sprintf("manifest version %d rebuilt", manifest.Version), err
		}); err != nil {
			return report, err
		}
		structuralRepair = true
	}
	if reportNeedsTenantMetadataRepair(working) {
		if err := runRepairAction(ctx, options, &report, "rebuild_tenant_metadata", func() (string, error) {
			return "tenant metadata rebuilt", s.repairTenantMetadata(ctx, tenantID)
		}); err != nil {
			return report, err
		}
		structuralRepair = true
	}
	if reportNeedsTenantRegistryRepair(working) {
		if err := runRepairAction(ctx, options, &report, "rebuild_tenant_registry", func() (string, error) {
			count, err := s.repairTenantRegistry(ctx, tenantID, working.Issues)
			return fmt.Sprintf("tenant registry rebuilt with %d tenants", count), err
		}); err != nil {
			return report, err
		}
		structuralRepair = true
	}
	if structuralRepair {
		working, err = s.inspectTenantRepair(ctx, tenantID)
		if err != nil {
			return report, err
		}
		working.Apply = options.Apply
		report.Plan = mergeRepairPlans(report.Plan, repairPlan(working))
	}
	if reportNeedsCompact(working) {
		if err := runRepairAction(ctx, options, &report, "compact_snapshot", func() (string, error) {
			manifest, err := s.Compact(ctx, tenantID)
			return fmt.Sprintf("manifest version %d compacted", manifest.Version), err
		}); err != nil {
			return report, err
		}
	}
	if reportNeedsIndexRebuild(working) {
		if err := runRepairAction(ctx, options, &report, "rebuild_indexes", func() (string, error) {
			s.deleteCachedIndexCatalog(tenantID)
			catalog, err := s.RebuildIndexes(ctx, tenantID)
			return fmt.Sprintf("index catalog version %d rebuilt", catalog.Version), err
		}); err != nil {
			return report, err
		}
	}
	if reportNeedsObjectCleanup(working) {
		if err := runRepairAction(ctx, options, &report, "cleanup_obsolete_objects", func() (string, error) {
			gc, err := s.RunGC(ctx, tenantID, GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true})
			return fmt.Sprintf("gc completed, deleted %d objects", len(gc.DeletedKeys)), err
		}); err != nil {
			return report, err
		}
	}
	after, err := s.inspectTenantRepair(ctx, tenantID)
	if err != nil {
		return report, err
	}
	report.RemainingIssues = after.Issues
	report.Status = after.Status
	report.ManifestVersion = after.ManifestVersion
	report.IndexHealth = after.IndexHealth
	verification, err := s.AuditIntegrity(ctx, tenantID, IntegrityAuditOptions{Deep: true})
	if err != nil {
		return report, err
	}
	report.Verification = &verification
	if verification.Status != "ok" {
		report.Status = verification.Status
	}
	return report, nil
}

func (s *TenantStore) inspectTenantRepair(ctx context.Context, tenantID string) (RepairReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RepairReport{}, err
	}
	report := RepairReport{
		TenantID:         tenantID,
		Status:           "ready",
		CheckedAt:        time.Now().UTC(),
		CurrentLayout:    CurrentObjectLayoutVersion,
		SupportedLayouts: []int{LegacyObjectLayoutVersion, CurrentObjectLayoutVersion},
	}
	report.Issues = append(report.Issues, s.layoutIssues(ctx, tenantID)...)
	report.Issues = append(report.Issues, s.tenantLifecycleIssues(ctx, tenantID)...)
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		report.Issues = append(report.Issues, s.manifestRepairIssues(ctx, tenantID, Manifest{}, err)...)
		sortRepairIssues(report.Issues)
		report.Status = statusForIssues(report.Issues)
		report.Plan = repairPlan(report)
		return report, nil
	}
	report.ManifestVersion = manifest.Version
	report.Issues = append(report.Issues, s.manifestRepairIssues(ctx, tenantID, manifest, nil)...)
	loaded, err := s.loadWithMeta(ctx, tenantID)
	if err != nil {
		report.Issues = append(report.Issues, RepairIssue{
			Code:         "graph_load_failed",
			Severity:     "error",
			ResourceType: "graph",
			Message:      err.Error(),
			Repairable:   false,
		})
		report.Status = statusForIssues(report.Issues)
		report.Plan = repairPlan(report)
		return report, nil
	}
	report.Issues = append(report.Issues, graphConsistencyIssues(loaded.Graph)...)
	health, err := s.IndexHealth(ctx, tenantID)
	if err != nil {
		report.Issues = append(report.Issues, RepairIssue{
			Code:         "index_health_failed",
			Severity:     "error",
			ResourceType: "index",
			Message:      err.Error(),
			Repairable:   true,
		})
	} else {
		report.IndexHealth = &health
		if health.Status != "ready" {
			report.Issues = append(report.Issues, indexHealthRepairIssues(health)...)
		}
	}
	sortRepairIssues(report.Issues)
	report.Status = statusForIssues(report.Issues)
	report.Plan = repairPlan(report)
	return report, nil
}
