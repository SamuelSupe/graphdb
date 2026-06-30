package storage

import (
	"context"
)

func runRepairAction(ctx context.Context, options RepairOptions, report *RepairReport, actionType string, run func() (string, error)) error {
	total := len(report.Plan)
	if total <= 0 {
		total = 1
	}
	completed := len(report.Actions)
	running := RepairAction{Type: actionType, Status: "running"}
	if options.Progress != nil {
		if err := options.Progress(ctx, running, completed, total); err != nil {
			return err
		}
	}
	message, err := run()
	action := RepairAction{Type: actionType, Message: message}
	if err != nil {
		action.Status = "failed"
		action.Message = err.Error()
		report.Actions = append(report.Actions, action)
		if options.Progress != nil {
			_ = options.Progress(ctx, action, completed, total)
		}
		return err
	}
	action.Status = "applied"
	report.Actions = append(report.Actions, action)
	if options.Progress != nil {
		return options.Progress(ctx, action, completed+1, total)
	}
	return nil
}

func repairPlan(report RepairReport) []RepairPlanStep {
	steps := []RepairPlanStep{}
	add := func(actionType string, codes ...string) {
		step := RepairPlanStep{Type: actionType, Status: "planned", IssueCodes: matchingRepairCodes(report.Issues, codes...)}
		steps = append(steps, step)
	}
	if reportNeedsManifestRebuild(report) {
		add("rebuild_manifest", "manifest_unreadable", "manifest_missing")
	}
	if reportNeedsTenantMetadataRepair(report) {
		add("rebuild_tenant_metadata", "tenant_metadata_missing", "tenant_metadata_unreadable")
	}
	if reportNeedsTenantRegistryRepair(report) {
		add("rebuild_tenant_registry", "tenant_registry_missing", "tenant_registry_unreadable")
	}
	if reportNeedsCompact(report) {
		add("compact_snapshot", "index_health_issue")
	}
	if reportNeedsIndexRebuild(report) {
		add("rebuild_indexes", "index_health_issue", "index_health_failed", "index_health_missing", "index_health_stale", "index_health_error")
	}
	if reportNeedsObjectCleanup(report) {
		add("cleanup_obsolete_objects", "index_health_issue")
	}
	return steps
}

func matchingRepairCodes(issues []RepairIssue, codes ...string) []string {
	out := []string{}
	for _, issue := range issues {
		for _, code := range codes {
			if issue.Code == code {
				out = append(out, issue.Code)
				break
			}
		}
	}
	return uniqueStrings(out)
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func mergeRepairPlans(left []RepairPlanStep, right []RepairPlanStep) []RepairPlanStep {
	if len(left) == 0 {
		return right
	}
	seen := map[string]bool{}
	out := append([]RepairPlanStep(nil), left...)
	for _, step := range out {
		seen[step.Type] = true
	}
	for _, step := range right {
		if seen[step.Type] {
			continue
		}
		out = append(out, step)
		seen[step.Type] = true
	}
	return out
}
