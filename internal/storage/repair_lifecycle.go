package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *TenantStore) tenantLifecycleIssues(ctx context.Context, tenantID string) []RepairIssue {
	issues := make([]RepairIssue, 0, 2)
	exists, existsErr := s.tenantPrefixExists(ctx, tenantID)
	if existsErr != nil {
		return append(issues, RepairIssue{
			Code:         "tenant_prefix_scan_failed",
			Severity:     "error",
			ResourceType: "tenant",
			ResourceID:   tenantID,
			Message:      existsErr.Error(),
			Repairable:   false,
		})
	}
	if !exists {
		return issues
	}
	if _, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID); err != nil {
		issues = append(issues, RepairIssue{
			Code:         "tenant_metadata_unreadable",
			Severity:     "error",
			ResourceType: "tenant_metadata",
			ResourceID:   tenantID,
			Message:      err.Error(),
			Repairable:   true,
		})
	} else if !configured {
		issues = append(issues, RepairIssue{
			Code:         "tenant_metadata_missing",
			Severity:     "warn",
			ResourceType: "tenant_metadata",
			ResourceID:   tenantID,
			Message:      "tenant metadata is missing",
			Repairable:   true,
		})
	}
	managed, ok, err := s.getTenantRegistry(ctx)
	if err != nil {
		issues = append(issues, RepairIssue{
			Code:         "tenant_registry_unreadable",
			Severity:     "error",
			ResourceType: "tenant_registry",
			ResourceID:   tenantID,
			Message:      err.Error(),
			Repairable:   true,
		})
		return issues
	}
	if !ok || !stringSliceContains(managed, tenantID) {
		issues = append(issues, RepairIssue{
			Code:         "tenant_registry_missing",
			Severity:     "warn",
			ResourceType: "tenant_registry",
			ResourceID:   tenantID,
			Message:      "tenant is missing from lifecycle registry",
			Repairable:   true,
		})
	}
	return issues
}

func (s *TenantStore) repairTenantMetadata(ctx context.Context, tenantID string) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	manifest, _, _ := s.getManifest(ctx, tenantID)
	now := time.Now().UTC()
	createdAt := manifest.UpdatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	metadata := TenantMetadata{
		TenantID:  tenantID,
		Status:    TenantStatusActive,
		CreatedAt: createdAt,
		UpdatedAt: now,
	}
	return s.putTenantMetadata(ctx, tenantID, metadata)
}

func (s *TenantStore) repairTenantRegistry(ctx context.Context, tenantID string, issues []RepairIssue) (int, error) {
	if hasRepairIssueCode(issues, "tenant_registry_unreadable") {
		tenants, err := s.RebuildTenantRegistry(ctx)
		return len(tenants), err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		if errors.Is(err, ErrNotFound) {
			_, err = s.RebuildTenantRegistry(ctx)
		}
		if err != nil {
			return 0, err
		}
	}
	tenants, _, err := s.getTenantRegistry(ctx)
	if err != nil {
		return 0, err
	}
	return len(tenants), nil
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func repairActionFailed(actionType string, err error) RepairAction {
	return RepairAction{Type: actionType, Status: "failed", Message: err.Error()}
}

func repairActionApplied(actionType string, format string, args ...any) RepairAction {
	return RepairAction{Type: actionType, Status: "applied", Message: fmt.Sprintf(format, args...)}
}
