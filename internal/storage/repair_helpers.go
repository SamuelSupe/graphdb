package storage

import (
	"sort"
	"strings"
)

func indexHealthRepairIssues(health IndexHealth) []RepairIssue {
	issues := make([]RepairIssue, 0, len(health.Issues))
	for _, issue := range health.Issues {
		issues = append(issues, RepairIssue{
			Code:         "index_health_issue",
			Severity:     "error",
			ResourceType: "index",
			Message:      issue,
			Repairable:   true,
		})
	}
	if len(issues) == 0 && health.Status != "ready" {
		issues = append(issues, RepairIssue{
			Code:         "index_health_" + health.Status,
			Severity:     "error",
			ResourceType: "index",
			Message:      "index health is " + health.Status,
			Repairable:   true,
		})
	}
	return issues
}

func reportNeedsCompact(report RepairReport) bool {
	for _, issue := range report.Issues {
		if issue.Code == "index_health_issue" {
			return true
		}
	}
	return false
}

func reportNeedsManifestRebuild(report RepairReport) bool {
	return hasAnyRepairIssueCode(report.Issues, "manifest_unreadable", "manifest_missing")
}

func reportNeedsTenantMetadataRepair(report RepairReport) bool {
	return hasAnyRepairIssueCode(report.Issues, "tenant_metadata_missing", "tenant_metadata_unreadable")
}

func reportNeedsTenantRegistryRepair(report RepairReport) bool {
	return hasAnyRepairIssueCode(report.Issues, "tenant_registry_missing", "tenant_registry_unreadable")
}

func reportNeedsIndexRebuild(report RepairReport) bool {
	for _, issue := range report.Issues {
		if strings.HasPrefix(issue.Code, "index_health") {
			return true
		}
	}
	return false
}

func reportNeedsObjectCleanup(report RepairReport) bool {
	for _, issue := range report.Issues {
		if issue.Code == "index_health_issue" && strings.Contains(issue.Message, "orphan index object") {
			return true
		}
	}
	return false
}

func hasAnyRepairIssueCode(issues []RepairIssue, codes ...string) bool {
	for _, code := range codes {
		if hasRepairIssueCode(issues, code) {
			return true
		}
	}
	return false
}

func hasRepairIssueCode(issues []RepairIssue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func statusForIssues(issues []RepairIssue) string {
	status := "ready"
	for _, issue := range issues {
		if issue.Severity == "error" {
			return "error"
		}
		if issue.Severity == "warn" {
			status = "warn"
		}
	}
	return status
}

func sortRepairIssues(issues []RepairIssue) {
	sort.Slice(issues, func(i, j int) bool {
		left := issues[i]
		right := issues[j]
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		if left.ResourceType != right.ResourceType {
			return left.ResourceType < right.ResourceType
		}
		return left.ResourceID < right.ResourceID
	})
}
