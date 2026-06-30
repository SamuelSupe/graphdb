package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"graphdb/internal/storage"
)

type GCRequest struct {
	KeepSnapshots           int    `json:"keep_snapshots,omitempty"`
	DeadLetterMaxAgeSeconds int64  `json:"deadletter_max_age_seconds,omitempty"`
	TaskMaxAgeSeconds       int64  `json:"task_max_age_seconds,omitempty"`
	CleanupIndexOrphans     bool   `json:"cleanup_index_orphans,omitempty"`
	Cursor                  string `json:"cursor,omitempty"`
	MaxDeletes              int    `json:"max_deletes,omitempty"`
	DryRun                  bool   `json:"dry_run,omitempty"`
}

type RepairRequest struct {
	Apply bool `json:"apply,omitempty"`
}

type ReaderFreshnessReport struct {
	TenantID              string               `json:"tenant_id"`
	Status                string               `json:"status"`
	Fresh                 bool                 `json:"fresh"`
	Consistent            bool                 `json:"consistent"`
	CheckedAt             time.Time            `json:"checked_at"`
	ManifestVersion       int64                `json:"manifest_version"`
	VisibleVersion        int64                `json:"visible_version"`
	Lag                   int64                `json:"lag"`
	WriterManifestVersion int64                `json:"writer_manifest_version"`
	ReaderManifestVersion int64                `json:"reader_manifest_version"`
	VersionLag            int64                `json:"version_lag"`
	LagMS                 int64                `json:"lag_ms"`
	WriterUpdatedAt       time.Time            `json:"writer_updated_at,omitempty"`
	ReaderUpdatedAt       time.Time            `json:"reader_updated_at,omitempty"`
	Cache                 ReaderFreshnessCache `json:"cache"`
	CommitTail            CommitTailFreshness  `json:"commit_tail"`
}

type ReaderFleetReadinessReport struct {
	TenantID       string                  `json:"tenant_id"`
	CheckedAt      time.Time               `json:"checked_at"`
	TargetVersion  int64                   `json:"target_version"`
	MinReady       int                     `json:"min_ready"`
	MaxStalenessMS int64                   `json:"max_staleness_ms"`
	Ready          bool                    `json:"ready"`
	TotalReaders   int                     `json:"total_readers"`
	ReadyReaders   int                     `json:"ready_readers"`
	StaleReaders   int                     `json:"stale_readers"`
	Readers        []ReaderFleetNodeStatus `json:"readers"`
}

type ReaderTrafficGateReport struct {
	TenantID       string                `json:"tenant_id"`
	CheckedAt      time.Time             `json:"checked_at"`
	ServeTraffic   bool                  `json:"serve_traffic"`
	Status         string                `json:"status"`
	Reason         string                `json:"reason,omitempty"`
	Message        string                `json:"message,omitempty"`
	TargetVersion  int64                 `json:"target_version"`
	MaxStalenessMS int64                 `json:"max_staleness_ms"`
	AllowStale     bool                  `json:"allow_stale,omitempty"`
	RefreshAttempt bool                  `json:"refresh_attempt,omitempty"`
	RefreshSuccess bool                  `json:"refresh_success,omitempty"`
	Freshness      ReaderFreshnessReport `json:"freshness"`
}

type ReaderFleetNodeStatus struct {
	storage.ReaderHeartbeat
	Ready  bool   `json:"ready"`
	AgeMS  int64  `json:"age_ms"`
	Reason string `json:"reason,omitempty"`
}

type ReaderFreshnessCache struct {
	Configured bool      `json:"configured"`
	Cached     bool      `json:"cached"`
	Loading    bool      `json:"loading"`
	CachedAt   time.Time `json:"cached_at,omitempty"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
	LastAccess time.Time `json:"last_access,omitempty"`
	TTLMS      int64     `json:"ttl_ms,omitempty"`
}

type CommitTailFreshness struct {
	WriterLength          int    `json:"writer_length"`
	ReaderLength          int    `json:"reader_length"`
	SnapshotVersion       int64  `json:"snapshot_version"`
	ReaderSnapshotVersion int64  `json:"reader_snapshot_version"`
	ReplayStatus          string `json:"replay_status"`
}

func (s *Server) writerLease(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	lease, err := s.Store.GetWriterLease(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (s *Server) readerLag(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	report, err := s.readerFreshnessReport(r, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.recordReaderHeartbeat(r, report)
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) readerFleetReadiness(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	report, err := s.readerFreshnessReport(r, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.recordReaderHeartbeat(r, report); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetVersion := report.ManifestVersion
	strictVersion := false
	if raw := r.URL.Query().Get("min_version"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "min_version must be a non-negative integer")
			return
		}
		targetVersion = parsed
		strictVersion = true
	}
	minReady := queryInt(r, "min_ready", 1)
	if minReady < 1 {
		writeError(w, http.StatusBadRequest, "min_ready must be >= 1")
		return
	}
	maxStalenessMS := int64(queryInt(r, "max_staleness_ms", 30000))
	if maxStalenessMS < 0 {
		writeError(w, http.StatusBadRequest, "max_staleness_ms must be non-negative")
		return
	}
	readiness, err := s.readerFleetReadinessReport(r, tenantID, targetVersion, minReady, maxStalenessMS, strictVersion)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, readiness)
}

func (s *Server) readerTrafficGate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	report, err := s.readerTrafficGateReport(r, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.recordReaderHeartbeat(r, report.Freshness); err != nil {
		report.ServeTraffic = false
		report.Status = "draining"
		report.Reason = "heartbeat_write_failed"
		s.writeTrafficGate(w, http.StatusServiceUnavailable, report)
		return
	}
	if !report.ServeTraffic {
		s.writeTrafficGate(w, http.StatusServiceUnavailable, report)
		return
	}
	s.writeTrafficGate(w, http.StatusOK, report)
}

func (s *Server) readerTrafficGateReport(r *http.Request, tenantID string) (ReaderTrafficGateReport, error) {
	freshness, err := s.readerFreshnessReport(r, tenantID)
	if err != nil {
		return ReaderTrafficGateReport{}, err
	}
	targetVersion := freshness.ManifestVersion
	hasMinVersion := false
	if raw := r.URL.Query().Get("min_version"); raw != "" {
		hasMinVersion = true
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			return ReaderTrafficGateReport{}, fmt.Errorf("min_version must be a non-negative integer")
		}
		targetVersion = parsed
	}
	maxStalenessMS := int64(queryInt(r, "max_staleness_ms", 30000))
	if maxStalenessMS < 0 {
		return ReaderTrafficGateReport{}, fmt.Errorf("max_staleness_ms must be non-negative")
	}
	allowStale := r.URL.Query().Get("allow_stale") == "true"
	refresh := r.URL.Query().Get("refresh") != "false"
	refreshAttempt := false
	refreshSuccess := false
	if refresh && !allowStale && s.Cache != nil && freshness.VisibleVersion < targetVersion {
		refreshAttempt = true
		if _, _, err := s.Cache.LoadAtLeast(r.Context(), tenantID, targetVersion); err != nil {
			return ReaderTrafficGateReport{
				TenantID:       tenantID,
				CheckedAt:      freshness.CheckedAt,
				ServeTraffic:   false,
				Status:         "draining",
				Reason:         "refresh_failed",
				Message:        err.Error(),
				TargetVersion:  targetVersion,
				MaxStalenessMS: maxStalenessMS,
				AllowStale:     allowStale,
				RefreshAttempt: true,
				RefreshSuccess: false,
				Freshness:      freshness,
			}, nil
		}
		refreshSuccess = true
		freshness, err = s.readerFreshnessReport(r, tenantID)
		if err != nil {
			return ReaderTrafficGateReport{}, err
		}
		if !hasMinVersion {
			targetVersion = freshness.ManifestVersion
		}
	}
	heartbeat := storage.ReaderHeartbeat{
		ReaderID:        s.Store.InstanceID,
		InstanceID:      s.Store.InstanceID,
		TenantID:        tenantID,
		Mode:            s.Mode,
		Status:          freshness.Status,
		Fresh:           freshness.Fresh,
		Consistent:      freshness.Consistent,
		ManifestVersion: freshness.ManifestVersion,
		SnapshotVersion: freshness.CommitTail.ReaderSnapshotVersion,
		VisibleVersion:  freshness.VisibleVersion,
		VersionLag:      freshness.VersionLag,
		LagMS:           freshness.LagMS,
		LastSeenAt:      freshness.CheckedAt,
	}
	ready, reason := readerNodeReady(heartbeat, targetVersion, maxStalenessMS, 0, !allowStale)
	status := "ready"
	if !ready {
		status = "draining"
	}
	return ReaderTrafficGateReport{
		TenantID:       tenantID,
		CheckedAt:      freshness.CheckedAt,
		ServeTraffic:   ready,
		Status:         status,
		Reason:         reason,
		TargetVersion:  targetVersion,
		MaxStalenessMS: maxStalenessMS,
		AllowStale:     allowStale,
		RefreshAttempt: refreshAttempt,
		RefreshSuccess: refreshSuccess,
		Freshness:      freshness,
	}, nil
}

func (s *Server) writeTrafficGate(w http.ResponseWriter, status int, report ReaderTrafficGateReport) {
	w.Header().Set("Cache-Control", "no-store")
	if report.ServeTraffic {
		w.Header().Set("X-GraphDB-Reader-Traffic", "ready")
	} else {
		w.Header().Set("X-GraphDB-Reader-Traffic", "draining")
	}
	writeJSON(w, status, report)
}

func (s *Server) readerFleetReadinessReport(r *http.Request, tenantID string, targetVersion int64, minReady int, maxStalenessMS int64, strictVersion bool) (ReaderFleetReadinessReport, error) {
	now := time.Now().UTC()
	heartbeats, err := s.Store.ListReaderHeartbeats(r.Context(), tenantID)
	if err != nil {
		return ReaderFleetReadinessReport{}, err
	}
	report := ReaderFleetReadinessReport{
		TenantID:       tenantID,
		CheckedAt:      now,
		TargetVersion:  targetVersion,
		MinReady:       minReady,
		MaxStalenessMS: maxStalenessMS,
		Readers:        make([]ReaderFleetNodeStatus, 0, len(heartbeats)),
	}
	for _, heartbeat := range heartbeats {
		node := ReaderFleetNodeStatus{ReaderHeartbeat: heartbeat}
		if !heartbeat.LastSeenAt.IsZero() {
			node.AgeMS = now.Sub(heartbeat.LastSeenAt).Milliseconds()
			if node.AgeMS < 0 {
				node.AgeMS = 0
			}
		}
		node.Ready, node.Reason = readerNodeReady(heartbeat, targetVersion, maxStalenessMS, node.AgeMS, strictVersion)
		if node.Ready {
			report.ReadyReaders++
		} else {
			report.StaleReaders++
		}
		report.Readers = append(report.Readers, node)
	}
	report.TotalReaders = len(report.Readers)
	report.Ready = report.ReadyReaders >= minReady
	return report, nil
}

func readerNodeReady(heartbeat storage.ReaderHeartbeat, targetVersion int64, maxStalenessMS int64, ageMS int64, strictVersion bool) (bool, string) {
	if heartbeat.LastSeenAt.IsZero() {
		return false, "missing_last_seen"
	}
	if maxStalenessMS > 0 && ageMS > maxStalenessMS {
		return false, "heartbeat_stale"
	}
	if heartbeat.VisibleVersion <= 0 {
		return false, "not_loaded"
	}
	if heartbeat.VisibleVersion < targetVersion {
		if strictVersion || maxStalenessMS <= 0 {
			return false, "version_lag"
		}
		if heartbeat.LagMS > maxStalenessMS {
			return false, "staleness_lag"
		}
	}
	return true, ""
}

func (s *Server) recordReaderHeartbeat(r *http.Request, report ReaderFreshnessReport) error {
	if s.Store == nil {
		return nil
	}
	_, err := s.Store.PutReaderHeartbeat(r.Context(), report.TenantID, storage.ReaderHeartbeat{
		ReaderID:        s.Store.InstanceID,
		InstanceID:      s.Store.InstanceID,
		TenantID:        report.TenantID,
		Mode:            s.Mode,
		Status:          report.Status,
		Fresh:           report.Fresh,
		Consistent:      report.Consistent,
		ManifestVersion: report.ManifestVersion,
		SnapshotVersion: report.CommitTail.ReaderSnapshotVersion,
		VisibleVersion:  report.VisibleVersion,
		VersionLag:      report.VersionLag,
		LagMS:           report.LagMS,
		LastSeenAt:      report.CheckedAt,
	})
	return err
}

func (s *Server) readerFreshnessReport(r *http.Request, tenantID string) (ReaderFreshnessReport, error) {
	manifest, err := s.Store.CurrentManifest(r.Context(), tenantID)
	if err != nil {
		return ReaderFreshnessReport{}, err
	}
	now := time.Now().UTC()
	report := ReaderFreshnessReport{
		TenantID:              tenantID,
		Status:                "fresh",
		Fresh:                 true,
		Consistent:            true,
		CheckedAt:             now,
		ManifestVersion:       manifest.Version,
		VisibleVersion:        manifest.Version,
		WriterManifestVersion: manifest.Version,
		ReaderManifestVersion: manifest.Version,
		WriterUpdatedAt:       manifest.UpdatedAt,
		ReaderUpdatedAt:       manifest.UpdatedAt,
		CommitTail: CommitTailFreshness{
			WriterLength:          storage.ManifestCommitTailLength(manifest),
			ReaderLength:          storage.ManifestCommitTailLength(manifest),
			SnapshotVersion:       manifest.SnapshotVersion,
			ReaderSnapshotVersion: manifest.SnapshotVersion,
			ReplayStatus:          "direct",
		},
	}
	if s.Cache != nil {
		status := s.Cache.Status(tenantID)
		report.Cache = ReaderFreshnessCache{
			Configured: true,
			Cached:     status.Cached,
			Loading:    status.Loading,
			CachedAt:   status.CachedAt,
			ExpiresAt:  status.ExpiresAt,
			LastAccess: status.LastAccess,
			TTLMS:      status.TTL.Milliseconds(),
		}
		report.VisibleVersion = 0
		report.ReaderManifestVersion = 0
		report.ReaderUpdatedAt = time.Time{}
		report.CommitTail.ReaderLength = 0
		report.CommitTail.ReaderSnapshotVersion = 0
		if status.Cached {
			report.VisibleVersion = status.Version
			report.ReaderManifestVersion = status.Version
			report.ReaderUpdatedAt = status.Manifest.UpdatedAt
			report.CommitTail.ReaderLength = storage.ManifestCommitTailLength(status.Manifest)
			report.CommitTail.ReaderSnapshotVersion = status.Manifest.SnapshotVersion
		}
		report.Status, report.CommitTail.ReplayStatus = cacheFreshnessStatus(manifest.Version, status)
	} else {
		report.Cache.Configured = false
	}
	s.applyIndexCatalogVisibility(r, tenantID, manifest, &report)
	report.VersionLag = manifest.Version - report.VisibleVersion
	if report.VersionLag < 0 {
		report.VersionLag = 0
	}
	report.Lag = report.VersionLag
	report.Fresh = report.VersionLag == 0 && report.Status == "fresh"
	report.Consistent = report.VersionLag == 0
	if report.VersionLag > 0 && !report.WriterUpdatedAt.IsZero() {
		if !report.ReaderUpdatedAt.IsZero() {
			report.LagMS = report.WriterUpdatedAt.Sub(report.ReaderUpdatedAt).Milliseconds()
		} else {
			report.LagMS = now.Sub(report.WriterUpdatedAt).Milliseconds()
		}
		if report.LagMS < 0 {
			report.LagMS = 0
		}
	}
	return report, nil
}

func (s *Server) applyIndexCatalogVisibility(r *http.Request, tenantID string, manifest storage.Manifest, report *ReaderFreshnessReport) {
	catalog, err := s.Store.GetIndexCatalog(r.Context(), tenantID)
	if err != nil || catalog.Version <= report.VisibleVersion {
		return
	}
	report.VisibleVersion = catalog.Version
	report.ReaderManifestVersion = catalog.Version
	report.ReaderUpdatedAt = catalog.UpdatedAt
	report.CommitTail.ReaderSnapshotVersion = catalog.Version
	if catalog.Version == manifest.Version {
		report.Status = "fresh"
		report.CommitTail.ReplayStatus = "indexed"
		return
	}
	report.Status = "stale"
	report.CommitTail.ReplayStatus = "indexed_stale"
}

func cacheFreshnessStatus(writerVersion int64, status storage.ReaderCacheStatus) (string, string) {
	switch {
	case status.Loading && !status.Cached:
		return "loading", "loading"
	case !status.Cached:
		return "cold", "not_loaded"
	case status.Version == writerVersion:
		return "fresh", "replayed"
	case status.Loading:
		return "refreshing", "loading"
	case status.Version < writerVersion:
		return "stale", "pending"
	default:
		return "ahead", "ahead"
	}
}

func (s *Server) recoverTenant(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "recovery is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	report, err := s.Store.RecoverTenant(r.Context(), tenantID)
	if err != nil {
		s.auditError("tenant_recovery_failed", tenantID, err, map[string]any{})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.invalidate(tenantID)
	s.auditInfo("tenant_recovery_completed", tenantID, map[string]any{
		"recovered": report.Recovered, "blocked": report.Blocked, "end_version": report.EndVersion,
	})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) integrityAudit(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	deep := r.URL.Query().Get("deep") != "false"
	report, err := s.Store.AuditIntegrity(r.Context(), tenantID, storage.IntegrityAuditOptions{Deep: deep})
	if err != nil {
		s.auditError("integrity_audit_failed", tenantID, err, map[string]any{"deep": deep})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("integrity_audit_completed", tenantID, map[string]any{
		"status": report.Status, "issues": len(report.Issues), "objects": report.Objects, "bytes": report.Bytes, "deep": deep,
	})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) repairTenant(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request RepairRequest
	if r.ContentLength != 0 && !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	if request.Apply && !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "repair apply is disabled in reader mode")
		return
	}
	report, err := s.Store.RepairTenant(r.Context(), tenantID, storage.RepairOptions{Apply: request.Apply})
	if err != nil {
		s.auditError("tenant_repair_failed", tenantID, err, map[string]any{"apply": request.Apply})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.Apply {
		s.invalidate(tenantID)
	}
	s.auditInfo("tenant_repair_completed", tenantID, map[string]any{
		"apply": request.Apply, "status": report.Status, "issues": len(report.Issues), "remaining_issues": len(report.RemainingIssues), "actions": len(report.Actions),
	})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) cleanupCommits(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "commit cleanup is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	report, err := s.Store.CleanupCommits(r.Context(), tenantID)
	if err != nil {
		s.auditError("commit_cleanup_failed", tenantID, err, map[string]any{})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("commit_cleanup_completed", tenantID, map[string]any{
		"deleted": report.Deleted, "kept_future": report.KeptFuture, "invalid_keys": len(report.InvalidKeys),
	})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) runGC(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "gc is disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request GCRequest
	if r.ContentLength != 0 && !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	if request.KeepSnapshots < 0 || request.DeadLetterMaxAgeSeconds < 0 || request.TaskMaxAgeSeconds < 0 || request.MaxDeletes < 0 {
		writeError(w, http.StatusBadRequest, "keep_snapshots, deadletter_max_age_seconds, task_max_age_seconds, and max_deletes must be non-negative")
		return
	}
	report, err := s.Store.RunGC(r.Context(), tenantID, storage.GCOptions{
		KeepSnapshots:       request.KeepSnapshots,
		DeadLetterMaxAge:    time.Duration(request.DeadLetterMaxAgeSeconds) * time.Second,
		TaskMaxAge:          time.Duration(request.TaskMaxAgeSeconds) * time.Second,
		CleanupIndexOrphans: request.CleanupIndexOrphans,
		CheckpointCursor:    request.Cursor,
		MaxDeletes:          request.MaxDeletes,
		DryRun:              request.DryRun,
	})
	if err != nil {
		s.auditError("gc_failed", tenantID, err, map[string]any{})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("gc_completed", tenantID, map[string]any{
		"manifest_version":      report.ManifestVersion,
		"deleted_snapshots":     report.DeletedSnapshots,
		"deleted_deadletters":   report.DeletedDeadLetters,
		"deleted_commits":       report.CommitCleanup.Deleted,
		"index_cleanup_attempt": report.IndexCleanupAttempt,
	})
	writeJSON(w, http.StatusOK, report)
}

func queryInt(r *http.Request, name string, fallback int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return -1
	}
	return value
}
