package httpapi

import (
	"context"
	"errors"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type maintenanceState struct {
	mu       sync.Mutex
	lastGC   map[string]time.Time
	gcCursor map[string]string
}

const (
	defaultMaintenanceGCMaxDeletes = 256
	autoCompactObjectMinTail       = 16
	ingestMaintenanceIdleWindow    = time.Minute
)

type MaintenanceReport struct {
	StartedAt      time.Time                 `json:"started_at"`
	FinishedAt     time.Time                 `json:"finished_at"`
	TenantsChecked int                       `json:"tenants_checked"`
	Compacted      int                       `json:"compacted"`
	GCRuns         int                       `json:"gc_runs"`
	IndexRebuilds  int                       `json:"index_rebuilds"`
	Errors         []MaintenanceError        `json:"errors,omitempty"`
	Tenants        []TenantMaintenanceReport `json:"tenants,omitempty"`
}

type TenantMaintenanceReport struct {
	TenantID        string                 `json:"tenant_id"`
	Manifest        int64                  `json:"manifest"`
	CommitTail      int                    `json:"commit_tail"`
	Compacted       bool                   `json:"compacted,omitempty"`
	CompactReason   string                 `json:"compact_reason,omitempty"`
	GCRun           bool                   `json:"gc_run,omitempty"`
	IndexStatus     string                 `json:"index_status,omitempty"`
	IndexTaskID     string                 `json:"index_task_id,omitempty"`
	IndexTaskReused bool                   `json:"index_task_reused,omitempty"`
	Skipped         string                 `json:"skipped,omitempty"`
	StorageFindings []StorageLayoutFinding `json:"storage_findings,omitempty"`
}

type StorageLayoutFinding struct {
	Code      string `json:"code"`
	Target    string `json:"target"`
	Current   int64  `json:"current"`
	Threshold int64  `json:"threshold"`
	Action    string `json:"action"`
}

type MaintenanceError struct {
	TenantID string `json:"tenant_id,omitempty"`
	Action   string `json:"action"`
	Message  string `json:"message"`
}

func (s *Server) StartMaintenanceLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 || !s.writeAllowed() || s.Store == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.runMaintenanceOnce(ctx, time.Now().UTC())
			}
		}
	}()
}

func (s *Server) runMaintenanceOnce(ctx context.Context, now time.Time) (report MaintenanceReport) {
	report.StartedAt = now
	defer func() {
		report.FinishedAt = time.Now().UTC()
	}()
	if !s.writeAllowed() || s.Store == nil {
		return report
	}
	tenants, err := s.Store.ListManagedTenants(ctx)
	if err != nil {
		report.addError("", "list_tenants", err)
		return report
	}
	for _, tenantID := range tenants {
		if err := ctx.Err(); err != nil {
			report.addError(tenantID, "context", err)
			break
		}
		tenantReport := s.maintainTenant(ctx, tenantID, now, &report)
		report.TenantsChecked++
		report.Tenants = append(report.Tenants, tenantReport)
	}
	return report
}

func (s *Server) maintainTenant(ctx context.Context, tenantID string, now time.Time, report *MaintenanceReport) TenantMaintenanceReport {
	manifest, err := s.Store.CurrentManifest(ctx, tenantID)
	if err != nil {
		report.addError(tenantID, "manifest", err)
		return TenantMaintenanceReport{TenantID: tenantID, Skipped: "manifest_error"}
	}
	tenantReport := TenantMaintenanceReport{TenantID: tenantID, Manifest: manifest.Version, CommitTail: storage.ManifestCommitTailLength(manifest)}
	if uninitializedManifest(manifest) {
		tenantReport.Skipped = "uninitialized"
		return tenantReport
	}
	config, configured, err := s.Store.GetTenantConfig(ctx, tenantID)
	if err != nil {
		report.addError(tenantID, "tenant_config", err)
		tenantReport.Skipped = "tenant_config_error"
		return tenantReport
	}
	if !configured {
		config = storage.DefaultTenantConfig()
	} else {
		config = storage.TenantConfigWithDefaults(config)
	}
	if s.IngestService != nil && s.IngestService.HasRecentTenantActivity(tenantID, now.Add(-ingestMaintenanceIdleWindow)) {
		tenantReport.Skipped = "ingest_active"
		return tenantReport
	}
	manifest = s.maybeAutoCompact(ctx, tenantID, manifest, config.Maintenance, report, &tenantReport)
	s.maybeRunGC(ctx, tenantID, now, config.Maintenance, report, &tenantReport)
	tenantReport.StorageFindings = s.storageLayoutFindings(ctx, tenantID, config.Maintenance, report)
	s.maybeRebuildIndexes(ctx, tenantID, config.Indexes, report, &tenantReport)
	tenantReport.Manifest = manifest.Version
	tenantReport.CommitTail = storage.ManifestCommitTailLength(manifest)
	return tenantReport
}

func (s *Server) maybeAutoCompact(ctx context.Context, tenantID string, manifest storage.Manifest, config storage.TenantMaintenanceConfig, report *MaintenanceReport, tenantReport *TenantMaintenanceReport) storage.Manifest {
	if !boolValue(config.AutoCompact) {
		return manifest
	}
	decision := s.autoCompactDecision(ctx, tenantID, manifest, config, report)
	if !decision.Compact {
		return manifest
	}
	next, err := s.Store.Compact(ctx, tenantID)
	if err != nil {
		report.addError(tenantID, "compact", err)
		s.auditError("maintenance_compact_failed", tenantID, err, map[string]any{"reason": decision.Reason, "current": decision.Current, "threshold": decision.Threshold})
		return manifest
	}
	s.invalidate(tenantID)
	report.Compacted++
	tenantReport.Compacted = true
	tenantReport.CompactReason = decision.Reason
	s.auditInfo("maintenance_compact_applied", tenantID, map[string]any{"version": next.Version, "reason": decision.Reason, "current": decision.Current, "threshold": decision.Threshold})
	return next
}

type autoCompactDecision struct {
	Compact   bool
	Reason    string
	Current   int64
	Threshold int64
}

func (s *Server) autoCompactDecision(ctx context.Context, tenantID string, manifest storage.Manifest, config storage.TenantMaintenanceConfig, report *MaintenanceReport) autoCompactDecision {
	tailThreshold := intValue(config.CompactCommitTailThreshold, 1000)
	tailLength := storage.ManifestCommitTailLength(manifest)
	if tailThreshold > 0 && tailLength >= tailThreshold {
		return autoCompactDecision{Compact: true, Reason: "commit_tail", Current: int64(tailLength), Threshold: int64(tailThreshold)}
	}
	// Compaction cannot reduce historical object inventory once the current
	// version is already snapshotted. Retention/GC owns those old objects.
	if tailLength == 0 && manifest.Version == manifest.SnapshotVersion && manifest.SnapshotKey != "" && manifest.SnapshotCatalogKey != "" {
		return autoCompactDecision{}
	}
	// Historical objects remain after compaction until retention removes them.
	// Require a meaningful amount of new tail data before object-count/byte
	// heuristics can trigger another compaction. The explicit tail threshold
	// above still wins when operators configure a lower threshold.
	if manifest.SnapshotKey != "" && tailLength < autoCompactObjectMinTail {
		return autoCompactDecision{}
	}
	objectThreshold := intValue(config.CompactObjectCountThreshold, 0)
	bytesThreshold := int64Value(config.CompactBytesThreshold, 0)
	smallObjectThreshold := intValue(config.SmallFileObjectThreshold, 0)
	smallBytesThreshold := int64Value(config.SmallFileBytesThreshold, 0)
	if objectThreshold <= 0 && bytesThreshold <= 0 && (smallObjectThreshold <= 0 || smallBytesThreshold <= 0) {
		return autoCompactDecision{}
	}
	usage, err := s.cachedTenantUsage(ctx, tenantID, time.Now().UTC())
	if err != nil {
		report.addError(tenantID, "tenant_usage", err)
		s.auditError("maintenance_usage_failed", tenantID, err, map[string]any{})
		return autoCompactDecision{}
	}
	if objectThreshold > 0 && usage.ObjectCount >= objectThreshold {
		return autoCompactDecision{Compact: true, Reason: "object_count", Current: int64(usage.ObjectCount), Threshold: int64(objectThreshold)}
	}
	if bytesThreshold > 0 && usage.TotalBytes >= bytesThreshold {
		return autoCompactDecision{Compact: true, Reason: "total_bytes", Current: usage.TotalBytes, Threshold: bytesThreshold}
	}
	if smallObjectThreshold > 0 && smallBytesThreshold > 0 && usage.ObjectCount >= smallObjectThreshold {
		averageBytes := int64(0)
		if usage.ObjectCount > 0 {
			averageBytes = usage.TotalBytes / int64(usage.ObjectCount)
		}
		if averageBytes <= smallBytesThreshold {
			return autoCompactDecision{Compact: true, Reason: "small_files", Current: int64(usage.ObjectCount), Threshold: int64(smallObjectThreshold)}
		}
	}
	return autoCompactDecision{}
}

func (s *Server) maybeRunGC(ctx context.Context, tenantID string, now time.Time, config storage.TenantMaintenanceConfig, report *MaintenanceReport, tenantReport *TenantMaintenanceReport) {
	if config.GCIntervalSeconds == nil || *config.GCIntervalSeconds <= 0 {
		return
	}
	interval := time.Duration(*config.GCIntervalSeconds) * time.Second
	if !s.gcDue(tenantID, now, interval) {
		return
	}
	keepSnapshots := intValue(config.KeepSnapshots, 1)
	gcReport, err := s.Store.RunGC(ctx, tenantID, storage.GCOptions{
		KeepSnapshots:       keepSnapshots,
		DeadLetterMaxAge:    time.Duration(int64Value(config.DeadLetterMaxAgeSeconds, 0)) * time.Second,
		TaskMaxAge:          time.Duration(int64Value(config.TaskMaxAgeSeconds, 0)) * time.Second,
		CleanupIndexOrphans: boolPtrValue(config.CleanupIndexOrphans, true),
		CheckpointCursor:    s.gcCursor(tenantID),
		MaxDeletes:          defaultMaintenanceGCMaxDeletes,
	})
	if err != nil {
		report.addError(tenantID, "gc", err)
		s.auditError("maintenance_gc_failed", tenantID, err, map[string]any{"keep_snapshots": keepSnapshots})
		return
	}
	s.markGC(tenantID, now, gcReport.Checkpoint)
	report.GCRuns++
	tenantReport.GCRun = true
	s.auditInfo("maintenance_gc_completed", tenantID, map[string]any{
		"manifest_version":    gcReport.ManifestVersion,
		"deleted_snapshots":   gcReport.DeletedSnapshots,
		"deleted_deadletters": gcReport.DeletedDeadLetters,
		"deleted_tasks":       gcReport.DeletedTasks,
		"deleted_commits":     gcReport.CommitCleanup.Deleted,
	})
}

func (s *Server) maybeRebuildIndexes(ctx context.Context, tenantID string, config storage.TenantIndexConfig, report *MaintenanceReport, tenantReport *TenantMaintenanceReport) {
	autoRebuild := boolValue(config.AutoRebuild)
	rebuildOnStale := boolValue(config.RebuildOnStale)
	if !autoRebuild && !rebuildOnStale {
		return
	}
	health, err := s.Store.IndexHealthWithOptions(ctx, tenantID, storage.IndexHealthOptions{})
	if err != nil {
		report.addError(tenantID, "index_health", err)
		s.auditError("maintenance_index_health_failed", tenantID, err, map[string]any{})
		return
	}
	tenantReport.IndexStatus = health.Status
	if !shouldRebuildIndex(health.Status, autoRebuild, rebuildOnStale) {
		return
	}
	task, err := s.Store.StartIndexRebuild(ctx, tenantID)
	if err != nil {
		report.addError(tenantID, "index_rebuild", err)
		s.auditError("maintenance_index_rebuild_failed", tenantID, err, map[string]any{"status": health.Status})
		return
	}
	report.IndexRebuilds++
	tenantReport.IndexTaskID = task.ID
	tenantReport.IndexTaskReused = task.Phase != "queued"
	s.auditInfo("maintenance_index_rebuild_started", tenantID, map[string]any{"task_id": task.ID, "status": health.Status})
}

func (s *Server) storageLayoutFindings(ctx context.Context, tenantID string, config storage.TenantMaintenanceConfig, report *MaintenanceReport) []StorageLayoutFinding {
	if intValue(config.EntityPageSplitThreshold, 0) <= 0 &&
		intValue(config.EntityPageMergeThreshold, 0) <= 0 &&
		intValue(config.EdgeShardSplitThreshold, 0) <= 0 &&
		intValue(config.EdgeShardMergeThreshold, 0) <= 0 {
		return nil
	}
	catalog, err := s.Store.GetIndexCatalog(ctx, tenantID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		report.addError(tenantID, "storage_layout", err)
		s.auditError("maintenance_storage_layout_failed", tenantID, err, map[string]any{})
		return nil
	}
	findings := make([]StorageLayoutFinding, 0)
	pageSplit := intValue(config.EntityPageSplitThreshold, 0)
	pageMerge := intValue(config.EntityPageMergeThreshold, 0)
	for _, page := range catalog.EntityPages {
		target := "entity_page:" + page.Shard
		if pageSplit > 0 && page.EntityCount >= pageSplit {
			findings = append(findings, StorageLayoutFinding{Code: "entity_page_split_needed", Target: target, Current: int64(page.EntityCount), Threshold: int64(pageSplit), Action: "split_entity_page"})
		}
		if pageMerge > 0 && page.EntityCount > 0 && page.EntityCount <= pageMerge {
			findings = append(findings, StorageLayoutFinding{Code: "entity_page_merge_candidate", Target: target, Current: int64(page.EntityCount), Threshold: int64(pageMerge), Action: "merge_entity_page"})
		}
	}
	edgeSplit := intValue(config.EdgeShardSplitThreshold, 0)
	edgeMerge := intValue(config.EdgeShardMergeThreshold, 0)
	for _, shard := range catalog.EdgeShards {
		target := "edge_shard:" + shard.RelationType + "/" + shard.Shard
		if edgeSplit > 0 && shard.EdgeCount >= edgeSplit {
			findings = append(findings, StorageLayoutFinding{Code: "edge_shard_split_needed", Target: target, Current: int64(shard.EdgeCount), Threshold: int64(edgeSplit), Action: "split_edge_shard"})
		}
		if edgeMerge > 0 && shard.EdgeCount > 0 && shard.EdgeCount <= edgeMerge {
			findings = append(findings, StorageLayoutFinding{Code: "edge_shard_merge_candidate", Target: target, Current: int64(shard.EdgeCount), Threshold: int64(edgeMerge), Action: "merge_edge_shard"})
		}
	}
	return findings
}

func (s *Server) gcDue(tenantID string, now time.Time, interval time.Duration) bool {
	state := s.maintenanceRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lastGC == nil {
		state.lastGC = map[string]time.Time{}
	}
	if state.gcCursor == nil {
		state.gcCursor = map[string]string{}
	}
	if state.gcCursor[tenantID] != "" {
		return true
	}
	last, ok := state.lastGC[tenantID]
	return !ok || !now.Before(last.Add(interval))
}

func (s *Server) gcCursor(tenantID string) string {
	state := s.maintenanceRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.gcCursor == nil {
		state.gcCursor = map[string]string{}
	}
	return state.gcCursor[tenantID]
}

func (s *Server) markGC(tenantID string, now time.Time, checkpoint storage.GCCheckpoint) {
	state := s.maintenanceRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lastGC == nil {
		state.lastGC = map[string]time.Time{}
	}
	if state.gcCursor == nil {
		state.gcCursor = map[string]string{}
	}
	if checkpoint.Paused && checkpoint.NextCursor != "" {
		state.gcCursor[tenantID] = checkpoint.NextCursor
		return
	}
	delete(state.gcCursor, tenantID)
	state.lastGC[tenantID] = now
}

func (s *Server) maintenanceRuntime() *maintenanceState {
	s.maintenanceOnce.Do(func() {
		if s.maintenance == nil {
			s.maintenance = &maintenanceState{}
		}
		s.maintenance.mu.Lock()
		defer s.maintenance.mu.Unlock()
		if s.maintenance.lastGC == nil {
			s.maintenance.lastGC = map[string]time.Time{}
		}
		if s.maintenance.gcCursor == nil {
			s.maintenance.gcCursor = map[string]string{}
		}
	})
	return s.maintenance
}

func (r *MaintenanceReport) addError(tenantID string, action string, err error) {
	r.Errors = append(r.Errors, MaintenanceError{TenantID: tenantID, Action: action, Message: err.Error()})
}

func shouldRebuildIndex(status string, autoRebuild bool, rebuildOnStale bool) bool {
	switch status {
	case "stale":
		return autoRebuild || rebuildOnStale
	case "missing", "error":
		return autoRebuild
	default:
		return false
	}
}

func uninitializedManifest(manifest storage.Manifest) bool {
	return manifest.Version == 0 && manifest.SnapshotKey == "" && storage.ManifestCommitTailLength(manifest) == 0 && manifest.UpdatedAt.IsZero()
}

func boolValue(value *bool) bool {
	return value != nil && *value
}

func intValue(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func int64Value(value *int64, fallback int64) int64 {
	if value == nil {
		return fallback
	}
	return *value
}

func boolPtrValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
