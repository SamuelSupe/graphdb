package storage

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/query"
)

const defaultRestoreDrillQueryTimeout = 30 * time.Second

type TenantRestoreDrillReport struct {
	TenantID            string                            `json:"tenant_id"`
	TargetTenantID      string                            `json:"target_tenant_id"`
	TargetPrefix        string                            `json:"target_prefix"`
	Status              string                            `json:"status"`
	Recoverable         bool                              `json:"recoverable"`
	DryRun              bool                              `json:"dry_run,omitempty"`
	Cleanup             bool                              `json:"cleanup"`
	GeneratedBackup     bool                              `json:"generated_backup,omitempty"`
	BackupKey           string                            `json:"backup_key"`
	BackupManifestKey   string                            `json:"backup_manifest_key,omitempty"`
	BackupIntegrity     BackupIntegrityReport             `json:"backup_integrity"`
	BackupManifestStats BackupManifestStats               `json:"backup_manifest_stats,omitempty"`
	Restore             *TenantRestoreReport              `json:"restore,omitempty"`
	Integrity           *IntegrityAuditReport             `json:"integrity,omitempty"`
	SourceUsage         *TenantUsageReport                `json:"source_usage,omitempty"`
	RestoredUsage       *TenantUsageReport                `json:"restored_usage,omitempty"`
	UsageComparison     TenantRestoreDrillUsageComparison `json:"usage_comparison,omitempty"`
	QueryResults        []TenantRestoreDrillQueryResult   `json:"query_results,omitempty"`
	Proof               TenantRestoreProof                `json:"proof"`
	CleanupDeleted      int                               `json:"cleanup_deleted,omitempty"`
	CleanupError        string                            `json:"cleanup_error,omitempty"`
	StartedAt           time.Time                         `json:"started_at"`
	CompletedAt         time.Time                         `json:"completed_at"`
}

type TenantRestoreDrillUsageComparison struct {
	SourceObjectCount   int   `json:"source_object_count,omitempty"`
	RestoredObjectCount int   `json:"restored_object_count,omitempty"`
	ObjectDelta         int   `json:"object_delta,omitempty"`
	SourceBytes         int64 `json:"source_bytes,omitempty"`
	RestoredBytes       int64 `json:"restored_bytes,omitempty"`
	BytesDelta          int64 `json:"bytes_delta,omitempty"`
	BackupEntities      int   `json:"backup_entities,omitempty"`
	RestoredEntities    int   `json:"restored_entities,omitempty"`
	BackupEdges         int   `json:"backup_edges,omitempty"`
	RestoredEdges       int   `json:"restored_edges,omitempty"`
}

type TenantRestoreDrillQueryResult struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Version    int64  `json:"version,omitempty"`
	Returned   int    `json:"returned,omitempty"`
	Error      string `json:"error,omitempty"`
	Skipped    bool   `json:"skipped,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}

type TenantRestoreProof struct {
	Recoverable bool                      `json:"recoverable"`
	Checks      []TenantRestoreProofCheck `json:"checks"`
	Message     string                    `json:"message"`
}

type TenantRestoreProofCheck struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Required bool   `json:"required"`
	Message  string `json:"message,omitempty"`
}

func (s *TenantStore) tenantRestoreDrillTask(ctx context.Context, task Task) (TenantRestoreDrillReport, error) {
	total := taskProgressTotal(task.Type)
	targetTenantID := restoreDrillTargetTenantID(task)
	if err := ValidateTenantID(targetTenantID); err != nil {
		return TenantRestoreDrillReport{}, err
	}
	targetPrefix := restoreDrillTargetPrefix(s.Prefix, task)
	targetStore := s.restoreDrillTargetStore(targetPrefix)
	dryRun := boolTaskParam(task.Params, "dry_run")
	cleanup := restoreDrillCleanup(task.Params)
	if !dryRun && cleanPrefix(targetPrefix) == s.Prefix && targetTenantID == task.TenantID {
		return TenantRestoreDrillReport{}, fmt.Errorf("restore drill target must not be the source tenant")
	}
	report := TenantRestoreDrillReport{
		TenantID:       task.TenantID,
		TargetTenantID: targetTenantID,
		TargetPrefix:   cleanPrefix(targetPrefix),
		DryRun:         dryRun,
		Cleanup:        cleanup,
		StartedAt:      time.Now().UTC(),
	}
	if err := s.updateTaskProgress(ctx, task, "restore_drill_backup", 1, total, map[string]any{"phase": "restore_drill_backup", "target_tenant_id": targetTenantID, "target_prefix": report.TargetPrefix}); err != nil {
		return report, err
	}
	input, backupKey, generated, stats, err := s.restoreDrillBackupInput(ctx, task)
	if err != nil {
		return report, err
	}
	report.GeneratedBackup = generated
	report.BackupKey = backupKey
	report.BackupManifestKey = input.ManifestKey
	report.BackupIntegrity = input.Integrity
	report.BackupManifestStats = stats
	sourceUsage, err := s.TenantUsage(ctx, task.TenantID)
	if err != nil {
		return report, err
	}
	report.SourceUsage = &sourceUsage
	if dryRun {
		report.Proof = restoreDrillDryRunProof(report)
		report.finish()
		_ = s.updateTaskProgress(ctx, task, "restore_drill_dry_run_done", total, total, map[string]any{"phase": "restore_drill_dry_run_done", "backup_key": report.BackupKey, "backup_manifest_key": report.BackupManifestKey, "recoverable": report.Recoverable})
		return report, nil
	}
	if err := s.updateTaskProgress(ctx, task, "restore_drill_restore", 2, total, map[string]any{"phase": "restore_drill_restore", "backup_key": report.BackupKey}); err != nil {
		return report, err
	}
	restoreTask := Task{
		ID:        task.ID + "-restore",
		TenantID:  targetTenantID,
		Type:      TaskTypeTenantRestore,
		Status:    TaskStatusRunning,
		Phase:     "restore_drill_restore",
		OwnerID:   s.InstanceID,
		Params:    map[string]any{"backup_key": report.BackupKey, "overwrite": true},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	restoreReport, err := targetStore.restoreTenantBackupInputTask(ctx, restoreTask, report.BackupKey, input)
	if err != nil {
		return report, err
	}
	targetStore.finishSyntheticRestoreTask(targetTenantID, restoreTask.ID)
	report.Restore = &restoreReport
	if err := s.updateTaskProgress(ctx, task, "restore_drill_audit", 3, total, map[string]any{"phase": "restore_drill_audit", "version": restoreReport.Version}); err != nil {
		return report, err
	}
	audit, err := targetStore.AuditIntegrity(ctx, targetTenantID, IntegrityAuditOptions{Deep: true})
	if err != nil {
		return report, err
	}
	report.Integrity = &audit
	if err := s.updateTaskProgress(ctx, task, "restore_drill_queries", 4, total, map[string]any{"phase": "restore_drill_queries", "audit_status": audit.Status}); err != nil {
		return report, err
	}
	restoredGraph, _, err := targetStore.Load(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	report.QueryResults = s.runRestoreDrillQueries(ctx, task, restoredGraph)
	if err := s.updateTaskProgress(ctx, task, "restore_drill_usage", 5, total, map[string]any{"phase": "restore_drill_usage", "queries": len(report.QueryResults)}); err != nil {
		return report, err
	}
	restoredUsage, err := targetStore.TenantUsage(ctx, targetTenantID)
	if err != nil {
		return report, err
	}
	report.RestoredUsage = &restoredUsage
	report.UsageComparison = compareRestoreDrillUsage(sourceUsage, restoredUsage, input.Record.Snapshot, restoredGraph.Snapshot())
	report.Proof = buildRestoreDrillProof(report)
	if cleanup {
		if err := s.updateTaskProgress(ctx, task, "restore_drill_cleanup", 6, total, map[string]any{"phase": "restore_drill_cleanup"}); err != nil {
			return report, err
		}
		purge, err := targetStore.PurgeTenant(ctx, targetTenantID, true)
		if err != nil {
			report.CleanupError = err.Error()
			report.Proof.Checks = append(report.Proof.Checks, TenantRestoreProofCheck{Name: "cleanup", Status: "error", Required: true, Message: err.Error()})
		} else {
			report.CleanupDeleted = purge.Deleted
			report.Proof.Checks = append(report.Proof.Checks, TenantRestoreProofCheck{Name: "cleanup", Status: "ok", Required: true, Message: fmt.Sprintf("deleted %d drill objects", purge.Deleted)})
		}
		report.Proof.finish()
	}
	report.finish()
	_ = s.updateTaskProgress(ctx, task, "restore_drill_done", total, total, map[string]any{"phase": "restore_drill_done", "recoverable": report.Recoverable, "status": report.Status})
	return report, nil
}

func (s *TenantStore) restoreDrillBackupInput(ctx context.Context, task Task) (tenantBackupInput, string, bool, BackupManifestStats, error) {
	backupKey := stringTaskParam(task.Params, "backup_key")
	if backupKey != "" {
		input, err := s.loadTenantBackupInput(ctx, backupKey)
		stats := BackupManifestStats{Entities: len(input.Record.Snapshot.Entities), Edges: len(input.Record.Snapshot.Edges)}
		if input.ManifestKey != "" {
			if manifest, manifestErr := s.loadBackupManifest(ctx, input.ManifestKey); manifestErr == nil {
				stats = manifest.Stats
			}
		}
		return input, backupKey, false, stats, err
	}
	input, resultKey, stats, err := s.createRestoreDrillBackup(ctx, task.TenantID, task.ID+"-backup")
	return input, resultKey, true, stats, err
}

func (s *TenantStore) createRestoreDrillBackup(ctx context.Context, tenantID string, backupID string) (tenantBackupInput, string, BackupManifestStats, error) {
	g, manifest, err := s.Load(ctx, tenantID)
	if err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	}
	metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	}
	if !configured {
		metadata = legacyTenantMetadata(tenantID)
	}
	record := TenantBackupRecord{TenantID: tenantID, Version: manifest.Version, CreatedAt: time.Now().UTC(), Metadata: metadata, Snapshot: g.Snapshot()}
	if config, ok, err := s.GetTenantConfig(ctx, tenantID); err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	} else if ok {
		record.Config = &config
	}
	if policy, ok, err := s.GetSourcePolicy(ctx, tenantID); err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	} else if ok {
		record.SourcePolicy = &policy
	}
	resultKey := s.taskResultKey(tenantID, backupID)
	if err := s.putTaskResult(ctx, tenantID, backupID, taskResult(record)); err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	}
	backupManifest, err := s.buildBackupManifest(ctx, tenantID, backupID, record, resultKey, manifest)
	if err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	}
	manifestKey, err := s.putBackupManifest(ctx, tenantID, backupID, backupManifest)
	if err != nil {
		return tenantBackupInput{}, "", BackupManifestStats{}, err
	}
	integrity := s.validateBackupManifest(ctx, backupManifest)
	if integrity.Status != "ok" {
		return tenantBackupInput{}, "", BackupManifestStats{}, fmt.Errorf("backup integrity failed: %s", strings.Join(integrity.Issues, "; "))
	}
	return tenantBackupInput{Record: record, ManifestKey: manifestKey, Integrity: integrity}, resultKey, backupManifest.Stats, nil
}

func (s *TenantStore) restoreDrillTargetStore(targetPrefix string) *TenantStore {
	target := NewTenantStore(s.Objects, targetPrefix)
	target.InstanceID = s.InstanceID
	target.LeaseTTL = s.LeaseTTL
	target.MaxRetries = s.MaxRetries
	target.IndexFormat = s.IndexFormat
	target.Backpressure = s.Backpressure
	target.BackpressureObserver = s.BackpressureObserver
	return target
}

func (s *TenantStore) finishSyntheticRestoreTask(tenantID string, taskID string) {
	task, err := s.GetTask(context.Background(), tenantID, taskID)
	if err != nil {
		return
	}
	now := time.Now().UTC()
	task.Status = TaskStatusSucceeded
	task.Phase = "done"
	task.UpdatedAt = now
	task.FinishedAt = now
	if task.Checkpoint == nil {
		task.Checkpoint = map[string]any{}
	}
	task.Checkpoint["completed"] = true
	s.trySaveTask(context.Background(), task)
}

func restoreDrillTargetTenantID(task Task) string {
	if value := stringTaskParam(task.Params, "target_tenant_id"); value != "" {
		return value
	}
	suffix := task.ID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return task.TenantID + "-restore-drill-" + suffix
}

func restoreDrillTargetPrefix(sourcePrefix string, task Task) string {
	if value := stringTaskParam(task.Params, "target_prefix"); value != "" {
		return value
	}
	return path.Join(sourcePrefix, "restore-drills", task.ID)
}

func restoreDrillCleanup(params map[string]any) bool {
	if _, ok := params["cleanup"]; !ok {
		return true
	}
	return boolTaskParam(params, "cleanup")
}

func (s *TenantStore) runRestoreDrillQueries(ctx context.Context, task Task, g *graph.Graph) []TenantRestoreDrillQueryResult {
	names := stringSliceTaskParam(task.Params, "query_templates")
	saved := make([]SavedQuery, 0)
	results := []TenantRestoreDrillQueryResult{}
	if len(names) == 0 {
		items, err := s.ListSavedQueries(ctx, task.TenantID)
		if err != nil {
			return []TenantRestoreDrillQueryResult{{Name: "*", Status: "error", Error: err.Error()}}
		}
		saved = items
	} else {
		for _, name := range names {
			item, err := s.GetSavedQuery(ctx, task.TenantID, name)
			if err != nil {
				results = append(results, TenantRestoreDrillQueryResult{Name: name, Status: "error", Error: err.Error()})
				continue
			}
			saved = append(saved, item)
		}
	}
	if len(saved) == 0 && len(results) == 0 {
		return []TenantRestoreDrillQueryResult{{Name: "*", Status: "skipped", Skipped: true, Error: "no saved query templates configured"}}
	}
	timeout := time.Duration(int64TaskParam(task.Params, "query_timeout_ms")) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultRestoreDrillQueryTimeout
	}
	for _, item := range saved {
		start := time.Now()
		queryCtx, cancel := context.WithTimeout(ctx, timeout)
		response, err := query.ExecuteContext(queryCtx, g, item.Request)
		cancel()
		result := TenantRestoreDrillQueryResult{Name: item.Name, DurationMS: time.Since(start).Milliseconds()}
		if err != nil {
			result.Status = "error"
			result.Error = err.Error()
		} else {
			result.Status = "ok"
			result.Version = response.Version
			result.Returned = len(response.Results)
		}
		results = append(results, result)
	}
	return results
}

func compareRestoreDrillUsage(source TenantUsageReport, restored TenantUsageReport, backup graph.Snapshot, restoredSnapshot graph.Snapshot) TenantRestoreDrillUsageComparison {
	return TenantRestoreDrillUsageComparison{
		SourceObjectCount:   source.ObjectCount,
		RestoredObjectCount: restored.ObjectCount,
		ObjectDelta:         restored.ObjectCount - source.ObjectCount,
		SourceBytes:         source.TotalBytes,
		RestoredBytes:       restored.TotalBytes,
		BytesDelta:          restored.TotalBytes - source.TotalBytes,
		BackupEntities:      len(backup.Entities),
		RestoredEntities:    len(restoredSnapshot.Entities),
		BackupEdges:         len(backup.Edges),
		RestoredEdges:       len(restoredSnapshot.Edges),
	}
}

func buildRestoreDrillProof(report TenantRestoreDrillReport) TenantRestoreProof {
	proof := TenantRestoreProof{}
	proof.add("backup_manifest_integrity", report.BackupIntegrity.Status == "ok", true, report.BackupIntegrity.Status)
	if report.Restore == nil {
		proof.add("restore_executed", false, true, "restore did not run")
	} else {
		proof.add("restore_integrity", report.Restore.RestoreIntegrity.Status == "ok", true, report.Restore.RestoreIntegrity.Status)
	}
	if report.Integrity == nil {
		proof.add("integrity_audit", false, true, "audit did not run")
	} else {
		proof.add("integrity_audit", report.Integrity.Status == "ok", true, report.Integrity.Status)
	}
	proof.add("entity_count", report.UsageComparison.BackupEntities == report.UsageComparison.RestoredEntities, true, fmt.Sprintf("backup=%d restored=%d", report.UsageComparison.BackupEntities, report.UsageComparison.RestoredEntities))
	proof.add("edge_count", report.UsageComparison.BackupEdges == report.UsageComparison.RestoredEdges, true, fmt.Sprintf("backup=%d restored=%d", report.UsageComparison.BackupEdges, report.UsageComparison.RestoredEdges))
	proof.add("restored_usage", report.UsageComparison.RestoredObjectCount > 0 && report.UsageComparison.RestoredBytes > 0, true, fmt.Sprintf("objects=%d bytes=%d", report.UsageComparison.RestoredObjectCount, report.UsageComparison.RestoredBytes))
	queryOK := true
	queryMessage := "all saved query templates passed"
	if len(report.QueryResults) == 0 {
		queryOK = false
		queryMessage = "query templates did not run"
	} else if len(report.QueryResults) == 1 && report.QueryResults[0].Skipped {
		queryMessage = report.QueryResults[0].Error
		proof.Checks = append(proof.Checks, TenantRestoreProofCheck{Name: "query_templates", Status: "warn", Required: false, Message: queryMessage})
		proof.finish()
		return proof
	} else {
		for _, result := range report.QueryResults {
			if result.Status != "ok" {
				queryOK = false
				queryMessage = result.Name + ": " + result.Error
				break
			}
		}
	}
	proof.add("query_templates", queryOK, true, queryMessage)
	proof.finish()
	return proof
}

func restoreDrillDryRunProof(report TenantRestoreDrillReport) TenantRestoreProof {
	proof := TenantRestoreProof{}
	proof.add("backup_manifest_integrity", report.BackupIntegrity.Status == "ok", true, report.BackupIntegrity.Status)
	proof.Checks = append(proof.Checks, TenantRestoreProofCheck{Name: "restore_executed", Status: "skipped", Required: true, Message: "dry run does not restore into the target prefix"})
	proof.finish()
	return proof
}

func (p *TenantRestoreProof) add(name string, ok bool, required bool, message string) {
	status := "ok"
	if !ok {
		status = "error"
	}
	p.Checks = append(p.Checks, TenantRestoreProofCheck{Name: name, Status: status, Required: required, Message: message})
}

func (p *TenantRestoreProof) finish() {
	p.Recoverable = true
	for _, check := range p.Checks {
		if check.Required && check.Status != "ok" {
			p.Recoverable = false
			p.Message = "restore drill proof failed"
			return
		}
	}
	p.Message = "restore drill proof passed"
}

func (r *TenantRestoreDrillReport) finish() {
	r.CompletedAt = time.Now().UTC()
	r.Recoverable = r.Proof.Recoverable
	if r.DryRun {
		r.Status = "dry_run"
		return
	}
	if r.Recoverable {
		r.Status = "passed"
	} else {
		r.Status = "failed"
	}
}

func stringSliceTaskParam(params map[string]any, key string) []string {
	value, ok := params[key]
	if !ok {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		return trimStringSlice(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return trimStringSlice(strings.Split(typed, ","))
	default:
		text := strings.TrimSpace(fmt.Sprint(typed))
		if text == "" {
			return nil
		}
		return []string{text}
	}
}

func trimStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			out = append(out, text)
		}
	}
	return out
}
