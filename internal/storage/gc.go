package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultGCReaderMaxAge = 5 * time.Minute
const coordinatorCandidateGracePeriod = time.Hour

type GCOptions struct {
	KeepSnapshots           int
	DeadLetterMaxAge        time.Duration
	TaskMaxAge              time.Duration
	CleanupIndexOrphans     bool
	ReaderMaxAge            time.Duration
	ReaderScanLimit         int
	CheckpointCursor        string
	MaxDeletes              int
	DryRun                  bool
	SkipEntityRecordCleanup bool
}

type GCReport struct {
	TenantID                     string        `json:"tenant_id"`
	ManifestVersion              int64         `json:"manifest_version"`
	DeletedSnapshots             int           `json:"deleted_snapshots"`
	DeletedDeadLetters           int           `json:"deleted_deadletters"`
	DeletedTasks                 int           `json:"deleted_tasks"`
	DeletedTaskResults           int           `json:"deleted_task_results"`
	DeletedImportSources         int           `json:"deleted_import_sources"`
	DeletedIndexTasks            int           `json:"deleted_index_tasks"`
	DeletedEntityRecords         int           `json:"deleted_entity_records"`
	DeletedCoordinatorManifests  int           `json:"deleted_coordinator_manifests"`
	DeletedWriteContexts         int           `json:"deleted_write_contexts"`
	ReaderWatermarkVersion       int64         `json:"reader_watermark_version,omitempty"`
	ReaderWatermarkReaders       int           `json:"reader_watermark_readers,omitempty"`
	ReaderWatermarkIgnored       int           `json:"reader_watermark_ignored,omitempty"`
	IndexCleanupAttempt          bool          `json:"index_cleanup_attempt"`
	CommitCleanupSkippedReason   string        `json:"commit_cleanup_skipped_reason,omitempty"`
	IndexCleanupSkippedReason    string        `json:"index_cleanup_skipped_reason,omitempty"`
	SnapshotCleanupSkippedReason string        `json:"snapshot_cleanup_skipped_reason,omitempty"`
	IndexCleanupError            string        `json:"index_cleanup_error,omitempty"`
	CommitCleanup                CleanupReport `json:"commit_cleanup"`
	DeletedKeys                  []string      `json:"deleted_keys,omitempty"`
	Checkpoint                   GCCheckpoint  `json:"checkpoint,omitempty"`
}

type GCCheckpoint struct {
	Cursor          string   `json:"cursor,omitempty"`
	NextCursor      string   `json:"next_cursor,omitempty"`
	LastKey         string   `json:"last_key,omitempty"`
	ScannedPrefixes []string `json:"scanned_prefixes,omitempty"`
	ScannedKeys     int      `json:"scanned_keys,omitempty"`
	SkippedByCursor int      `json:"skipped_by_cursor,omitempty"`
	DeletedKeys     []string `json:"deleted_keys,omitempty"`
	PlannedKeys     []string `json:"planned_keys,omitempty"`
	FailedKeys      []string `json:"failed_keys,omitempty"`
	Deleted         int      `json:"deleted,omitempty"`
	Planned         int      `json:"planned,omitempty"`
	MaxDeletes      int      `json:"max_deletes,omitempty"`
	DryRun          bool     `json:"dry_run,omitempty"`
	Paused          bool     `json:"paused,omitempty"`
	Completed       bool     `json:"completed"`
}

type gcReaderProtection struct {
	SnapshotVersions []int64
	VisibleVersions  []int64
	Watermark        int64
	Active           int
	Ignored          int
}

func (s *TenantStore) RunGC(ctx context.Context, tenantID string, options GCOptions) (GCReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return GCReport{}, err
	}
	if s.coordinated() {
		operationCtx, stop, err := s.startCoordinatorOperationLease(
			ctx, tenantID, TaskTypeGC,
		)
		if err != nil {
			return GCReport{}, err
		}
		defer stop()
		ctx = operationCtx
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return GCReport{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return GCReport{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return GCReport{}, err
	}
	if err := s.validateGCSnapshotCursor(tenantID, manifest, options.CheckpointCursor); err != nil {
		return GCReport{}, err
	}
	report := GCReport{TenantID: tenantID, ManifestVersion: manifest.Version}
	checkpoint := newGCCheckpointRunner(options)
	finish := func(err error) (GCReport, error) {
		sort.Strings(report.DeletedKeys)
		report.Checkpoint = checkpoint.result()
		if gcPaused(err) {
			return report, nil
		}
		return report, err
	}
	protection, err := s.gcReaderProtection(ctx, tenantID, options.ReaderMaxAge, options.ReaderScanLimit, time.Now().UTC())
	if err != nil {
		return finish(err)
	}
	report.ReaderWatermarkVersion = protection.Watermark
	report.ReaderWatermarkReaders = protection.Active
	report.ReaderWatermarkIgnored = protection.Ignored
	coordinatorMirrorPending := false
	var coordinatorRoots CoordinatorReachability
	if s.coordinated() {
		coordinatorRoots, err = s.Coordinator.Reachability(ctx, tenantID)
		if err != nil {
			return finish(err)
		}
		coordinatorMirrorPending = coordinatorRoots.PendingLegacy > 0
	}
	if coordinatorMirrorPending {
		report.CommitCleanupSkippedReason = "PostgreSQL legacy manifest outbox still has reachable manifests"
	} else if protection.activeReaderBehind(manifest.Version) {
		report.CommitCleanupSkippedReason = fmt.Sprintf("active reader watermark %d is behind manifest version %d", protection.Watermark, manifest.Version)
	} else {
		commitReport, err := s.cleanupCommitsLocked(ctx, tenantID, manifest, checkpoint)
		report.CommitCleanup = commitReport
		report.DeletedKeys = append(report.DeletedKeys, commitReport.DeletedKeys...)
		if err != nil {
			return finish(err)
		}
	}
	if s.coordinated() {
		manifests, contexts, keys, err := s.cleanupCoordinatorCandidatesLocked(
			ctx, tenantID, coordinatorRoots, checkpoint,
		)
		report.DeletedCoordinatorManifests = manifests
		report.DeletedWriteContexts = contexts
		report.DeletedKeys = append(report.DeletedKeys, keys...)
		if err != nil {
			return finish(err)
		}
	}
	var deleted int
	var keys []string
	if options.TaskMaxAge > 0 {
		cutoff := time.Now().UTC().Add(-options.TaskMaxAge)
		taskReport := taskCleanupReport{}
		if err := s.cleanupIndexTasksLocked(ctx, tenantID, cutoff, &taskReport, checkpoint); err != nil {
			return finish(err)
		}
		report.DeletedIndexTasks = taskReport.DeletedIndexTasks
		report.DeletedKeys = append(report.DeletedKeys, taskReport.DeletedKeys...)
	}
	if options.DeadLetterMaxAge > 0 {
		deleted, keys, err = s.cleanupDeadLettersLocked(ctx, tenantID, options.DeadLetterMaxAge, checkpoint)
		report.DeletedDeadLetters = deleted
		report.DeletedKeys = append(report.DeletedKeys, keys...)
		if err != nil {
			return finish(err)
		}
	}
	if coordinatorMirrorPending {
		report.SnapshotCleanupSkippedReason = "PostgreSQL legacy manifest outbox still has reachable snapshots"
	} else {
		deleted, keys, err = s.cleanupSnapshotsLocked(ctx, tenantID, manifest, options.KeepSnapshots, protection, checkpoint)
		report.DeletedSnapshots = deleted
		report.DeletedKeys = append(report.DeletedKeys, keys...)
		if err != nil {
			return finish(err)
		}
	}
	if options.TaskMaxAge > 0 {
		cutoff := time.Now().UTC().Add(-options.TaskMaxAge)
		taskReport := taskCleanupReport{}
		err := s.cleanupUnifiedTasksLocked(ctx, tenantID, cutoff, &taskReport, checkpoint)
		report.DeletedTasks = taskReport.DeletedTasks
		report.DeletedTaskResults = taskReport.DeletedTaskResults
		report.DeletedImportSources = taskReport.DeletedImportSources
		report.DeletedKeys = append(report.DeletedKeys, taskReport.DeletedKeys...)
		if err != nil {
			return finish(err)
		}
	}
	if options.CleanupIndexOrphans {
		if options.DryRun || options.CheckpointCursor != "" || options.MaxDeletes > 0 {
			report.IndexCleanupSkippedReason = "checkpoint or dry-run mode skips index orphan cleanup"
		} else if protection.activeReaderBehind(manifest.Version) {
			report.IndexCleanupSkippedReason = fmt.Sprintf("active reader watermark %d is behind manifest version %d", protection.Watermark, manifest.Version)
		} else {
			report.IndexCleanupAttempt = true
			if err := s.cleanupIndexOrphansLocked(ctx, tenantID); err != nil {
				report.IndexCleanupError = err.Error()
			}
			if !options.SkipEntityRecordCleanup {
				deletedRecords, recordKeys, err := s.cleanupEntityRecordsLocked(ctx, tenantID)
				report.DeletedEntityRecords = deletedRecords
				report.DeletedKeys = append(report.DeletedKeys, recordKeys...)
				if err != nil && report.IndexCleanupError == "" {
					report.IndexCleanupError = err.Error()
				}
			}
		}
	}
	return finish(nil)
}

func (s *TenantStore) cleanupCoordinatorCandidatesLocked(
	ctx context.Context,
	tenantID string,
	roots CoordinatorReachability,
	checkpoint *gcCheckpointRunner,
) (int, int, []string, error) {
	cutoff := time.Now().UTC().Add(-coordinatorCandidateGracePeriod)
	manifestPrefix := s.coordinatorManifestPrefix(tenantID)
	contextPrefix := s.coordinatorWriteContextPrefix(tenantID)
	manifestCount, manifestKeys, err := s.cleanupCoordinatorCandidatePrefix(
		ctx,
		manifestPrefix,
		roots.ManifestKeys,
		cutoff,
		checkpoint,
		roots.Head.Revision,
		coordinatorManifestRevisionFromKey,
		coordinatorManifestUpdatedAt,
	)
	if err != nil {
		return manifestCount, 0, manifestKeys, err
	}
	contextCount, contextKeys, err := s.cleanupCoordinatorCandidatePrefix(
		ctx,
		contextPrefix,
		roots.WriteContextKeys,
		cutoff,
		checkpoint,
		roots.Head.WriteContextRevision,
		coordinatorWriteContextRevisionFromKey,
		coordinatorWriteContextUpdatedAt,
	)
	return manifestCount, contextCount, append(manifestKeys, contextKeys...), err
}

func (s *TenantStore) cleanupCoordinatorCandidatePrefix(
	ctx context.Context,
	prefix string,
	roots map[string]struct{},
	cutoff time.Time,
	checkpoint *gcCheckpointRunner,
	maxResolvedRevision int64,
	revisionFromKey func(string) (int64, bool),
	updatedAt func(context.Context, []byte) (time.Time, error),
) (int, []string, error) {
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil || skip {
		return 0, nil, err
	}
	deleted := 0
	keys := make([]string, 0)
	for _, object := range objects {
		if _, reachable := roots[object.Key]; reachable {
			continue
		}
		revision, ok := revisionFromKey(object.Key)
		if !ok || revision > maxResolvedRevision {
			continue
		}
		data, err := s.Objects.Get(ctx, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return deleted, keys, err
		}
		updated, err := updatedAt(ctx, data)
		if err != nil || updated.IsZero() || updated.After(cutoff) {
			continue
		}
		removed, err := checkpoint.deleteKey(ctx, s.Objects, object.Key)
		if err != nil {
			return deleted, keys, err
		}
		if removed {
			deleted++
			keys = append(keys, object.Key)
		}
	}
	if err := checkpoint.pauseAfterPage(next); err != nil {
		return deleted, keys, err
	}
	return deleted, keys, nil
}

func coordinatorManifestRevisionFromKey(key string) (int64, bool) {
	name := strings.TrimSuffix(path.Base(key), ".parquet")
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		return 0, false
	}
	revision, err := strconv.ParseInt(parts[1], 10, 64)
	return revision, err == nil && revision > 0
}

func coordinatorWriteContextRevisionFromKey(key string) (int64, bool) {
	parts := strings.Split(path.Base(key), "-")
	if len(parts) != 2 {
		return 0, false
	}
	revision, err := strconv.ParseInt(parts[0], 10, 64)
	return revision, err == nil && revision > 0
}

func coordinatorManifestUpdatedAt(ctx context.Context, data []byte) (time.Time, error) {
	if !isParquetBytes(data) {
		return time.Time{}, fmt.Errorf("coordinator manifest is not parquet")
	}
	manifest, err := decodeParquetManifest(ctx, data)
	return manifest.UpdatedAt, err
}

func coordinatorWriteContextUpdatedAt(_ context.Context, data []byte) (time.Time, error) {
	var snapshot WriteContextSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return time.Time{}, err
	}
	return snapshot.UpdatedAt, nil
}

func (s *TenantStore) validateGCSnapshotCursor(tenantID string, manifest Manifest, cursor string) error {
	version, ok := shardedSnapshotVersionFromCursor(s.snapshotPrefix(tenantID), cursor)
	if !ok || manifest.SnapshotKey == "" || version != manifest.SnapshotVersion {
		return nil
	}
	return fmt.Errorf("gc checkpoint cursor references current snapshot version %d", version)
}

func (p gcReaderProtection) activeReaderBehind(manifestVersion int64) bool {
	return p.Active > 0 && p.Watermark > 0 && p.Watermark < manifestVersion
}

func (s *TenantStore) gcReaderProtection(ctx context.Context, tenantID string, maxAge time.Duration, scanLimit int, now time.Time) (gcReaderProtection, error) {
	if maxAge < 0 {
		return gcReaderProtection{}, nil
	}
	if maxAge == 0 {
		maxAge = defaultGCReaderMaxAge
	}
	if scanLimit <= 0 {
		scanLimit = readerHeartbeatScanLimit
	}
	heartbeats, err := s.ListReaderHeartbeatsWithOptions(ctx, tenantID, ReaderHeartbeatListOptions{
		MaxAge:        maxAge,
		Limit:         readerHeartbeatListLimit,
		ScanLimit:     scanLimit,
		DeleteExpired: true,
	})
	if err != nil {
		return gcReaderProtection{}, err
	}
	out := gcReaderProtection{}
	seenVisible := map[int64]struct{}{}
	seenSnapshots := map[int64]struct{}{}
	for _, heartbeat := range heartbeats {
		if heartbeat.VisibleVersion <= 0 || heartbeat.LastSeenAt.IsZero() {
			out.Ignored++
			continue
		}
		if maxAge > 0 && now.Sub(heartbeat.LastSeenAt) > maxAge {
			out.Ignored++
			continue
		}
		if _, ok := seenVisible[heartbeat.VisibleVersion]; !ok {
			seenVisible[heartbeat.VisibleVersion] = struct{}{}
			out.VisibleVersions = append(out.VisibleVersions, heartbeat.VisibleVersion)
		}
		if heartbeat.SnapshotVersion > 0 {
			if _, ok := seenSnapshots[heartbeat.SnapshotVersion]; !ok {
				seenSnapshots[heartbeat.SnapshotVersion] = struct{}{}
				out.SnapshotVersions = append(out.SnapshotVersions, heartbeat.SnapshotVersion)
			}
		}
		out.Active++
		if out.Watermark == 0 || heartbeat.VisibleVersion < out.Watermark {
			out.Watermark = heartbeat.VisibleVersion
		}
	}
	sort.Slice(out.SnapshotVersions, func(i, j int) bool { return out.SnapshotVersions[i] < out.SnapshotVersions[j] })
	sort.Slice(out.VisibleVersions, func(i, j int) bool { return out.VisibleVersions[i] < out.VisibleVersions[j] })
	return out, nil
}

func (s *TenantStore) cleanupCommitsLocked(ctx context.Context, tenantID string, manifest Manifest, checkpoint *gcCheckpointRunner) (CleanupReport, error) {
	report := CleanupReport{TenantID: tenantID, ManifestVersion: manifest.Version}
	referenced := referencedCommits(manifest)
	report.ReferencedKeys = len(referenced)
	checkpoint.addPrefix(s.commitPrefix(tenantID))
	commitPrefix := s.commitPrefix(tenantID)
	cursor := checkpoint.options.CheckpointCursor
	if cursor != "" && !strings.HasPrefix(cursor, commitPrefix) && cursor > commitPrefix {
		return report, nil
	}
	pageLimit := checkpoint.scanPageLimit()
	scan, err := s.loadCommitObjectsPage(ctx, tenantID, referenced, cursor, pageLimit)
	if err != nil {
		return report, err
	}
	report.InvalidKeys = scan.InvalidKeys
	for _, item := range scan.Items {
		if s.coordinated() && !item.Commit.CreatedAt.IsZero() &&
			item.Commit.CreatedAt.After(time.Now().UTC().Add(-coordinatorCandidateGracePeriod)) {
			report.KeptFuture++
			report.FutureKeys = append(report.FutureKeys, item.Key)
			continue
		}
		if item.Commit.Version > manifest.Version {
			report.KeptFuture++
			report.FutureKeys = append(report.FutureKeys, item.Key)
			continue
		}
		deleted, err := checkpoint.deleteKey(ctx, s.Objects, item.Key)
		if err != nil {
			return report, err
		}
		if deleted {
			report.Deleted++
			report.DeletedKeys = append(report.DeletedKeys, item.Key)
		}
	}
	if checkpoint.checkpoint.Paused {
		return report, errGCPaused
	}
	if scan.Truncated && !checkpoint.checkpoint.Paused {
		checkpoint.pauseAt(scan.NextCursor)
		return report, errGCPaused
	}
	if err := s.cleanupCommitSegmentsLocked(ctx, tenantID, manifest, referenced, &report, checkpoint); err != nil {
		return report, err
	}
	return report, nil
}

func (s *TenantStore) cleanupCommitSegmentsLocked(ctx context.Context, tenantID string, manifest Manifest, referenced map[string]struct{}, report *CleanupReport, checkpoint *gcCheckpointRunner) error {
	prefix := s.commitSegmentPrefix(tenantID)
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	for _, object := range objects {
		if _, ok := referenced[object.Key]; ok {
			continue
		}
		items, err := s.loadCommitSegment(ctx, tenantID, CommitSegmentRef{Key: object.Key})
		if err != nil {
			report.InvalidKeys = append(report.InvalidKeys, object.Key)
			continue
		}
		last := items[len(items)-1].Commit.Version
		if last > manifest.Version {
			report.KeptFuture++
			report.FutureKeys = append(report.FutureKeys, object.Key)
			continue
		}
		deleted, err := checkpoint.deleteKey(ctx, s.Objects, object.Key)
		if err != nil {
			return err
		}
		if deleted {
			report.Deleted++
			report.DeletedKeys = append(report.DeletedKeys, object.Key)
		}
	}
	return checkpoint.pauseAfterPage(next)
}

func (s *TenantStore) cleanupSnapshotsLocked(ctx context.Context, tenantID string, manifest Manifest, keepSnapshots int, protection gcReaderProtection, checkpoint *gcCheckpointRunner) (int, []string, error) {
	if keepSnapshots < 1 {
		keepSnapshots = 1
	}
	// A reader can require the closest snapshot at or before its visible
	// version. Conservatively defer snapshot retention while any reader is
	// active instead of materializing every snapshot to rediscover that base.
	if protection.Active > 0 {
		return 0, nil, nil
	}
	type snapshotObject struct {
		Key     string
		Version int64
	}
	prefix := s.snapshotPrefix(tenantID)
	checkpoint.addPrefix(prefix)
	deleted := 0
	keys := make([]string, 0)
	pageCursor := ""
	if version, ok := shardedSnapshotVersionFromCursor(prefix, checkpoint.options.CheckpointCursor); ok {
		shardedKeys, err := s.deleteShardedSnapshotVersionObjectsLocked(ctx, tenantID, version, checkpoint)
		keys = append(keys, shardedKeys...)
		if err != nil {
			return deleted, keys, err
		}
		removed, err := checkpoint.deleteKeyIgnoringCursor(ctx, s.Objects, s.snapshotKey(tenantID, version))
		if err != nil {
			return deleted, keys, err
		}
		if removed {
			deleted++
			keys = append(keys, s.snapshotKey(tenantID, version))
		}
		if checkpoint.checkpoint.Paused {
			return deleted, keys, errGCPaused
		}
		pageCursor = s.snapshotKey(tenantID, version)
	} else {
		var skip bool
		pageCursor, skip = checkpoint.pageCursor(prefix)
		if skip {
			return 0, nil, nil
		}
	}
	pageLimit := checkpoint.scanPageLimit()
	if pageLimit > 0 {
		pageLimit += keepSnapshots
	}
	objects, next, err := listObjectPage(ctx, s.Objects, prefix, pageCursor, pageLimit)
	if err != nil {
		return deleted, keys, err
	}
	items := make([]snapshotObject, 0, min(len(objects), pageLimit))
	reachedSharded := false
	for _, object := range objects {
		if strings.HasPrefix(object.Key, prefix+"sharded/") {
			reachedSharded = true
			break
		}
		version, ok := snapshotIdentityFromKey(object.Key)
		if !ok {
			continue
		}
		items = append(items, snapshotObject{Key: object.Key, Version: version})
	}
	safeCount := max(0, len(items)-keepSnapshots)
	for _, item := range items[:safeCount] {
		if item.Key == manifest.SnapshotKey {
			continue
		}
		shardedKeys, err := s.deleteShardedSnapshotVersionObjectsLocked(ctx, tenantID, item.Version, checkpoint)
		if err != nil {
			return deleted, keys, err
		}
		keys = append(keys, shardedKeys...)
		removedSnapshot, err := checkpoint.deleteKey(ctx, s.Objects, item.Key)
		if err != nil {
			return deleted, keys, err
		}
		if removedSnapshot {
			deleted++
			keys = append(keys, item.Key)
		}
	}
	if checkpoint.checkpoint.Paused {
		return deleted, keys, errGCPaused
	}
	if next != "" && !reachedSharded {
		resume := pageCursor
		if safeCount > 0 {
			resume = items[safeCount-1].Key
		}
		checkpoint.pauseAt(resume)
		return deleted, keys, errGCPaused
	}
	return deleted, keys, nil
}

func (s *TenantStore) deleteShardedSnapshotVersionObjectsLocked(ctx context.Context, tenantID string, version int64, checkpoint *gcCheckpointRunner) ([]string, error) {
	prefix := s.snapshotVersionPrefix(tenantID, version) + "/"
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil {
		return nil, err
	}
	if skip {
		return nil, nil
	}
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		deleted, err := checkpoint.deleteKey(ctx, s.Objects, object.Key)
		if err != nil {
			return keys, err
		}
		if deleted {
			keys = append(keys, object.Key)
		}
	}
	if err := checkpoint.pauseAfterPage(next); err != nil {
		return keys, err
	}
	return keys, nil
}

func shardedSnapshotVersionFromCursor(snapshotPrefix string, cursor string) (int64, bool) {
	marker := snapshotPrefix + "sharded/v"
	if !strings.HasPrefix(cursor, marker) {
		return 0, false
	}
	rest := strings.TrimPrefix(cursor, marker)
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		rest = rest[:slash]
	}
	version, err := strconv.ParseInt(rest, 10, 64)
	return version, err == nil && version >= 0
}

func (s *TenantStore) cleanupDeadLettersLocked(ctx context.Context, tenantID string, maxAge time.Duration, checkpoint *gcCheckpointRunner) (int, []string, error) {
	prefix := s.tenantObjectPrefix(tenantID) + "ingest/"
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil {
		return 0, nil, err
	}
	if skip {
		return 0, nil, nil
	}
	cutoff := time.Now().UTC().Add(-maxAge)
	deleted := 0
	keys := make([]string, 0)
	for _, object := range objects {
		if !strings.Contains(object.Key, "/deadletters/") || !strings.HasSuffix(object.Key, ".parquet") {
			continue
		}
		var letter DeadLetter
		data, err := s.Objects.Get(ctx, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return deleted, keys, err
		}
		if !isParquetBytes(data) {
			continue
		}
		letter, err = decodeParquetDeadLetter(ctx, data)
		if err != nil {
			continue
		}
		updated := letter.UpdatedAt
		if updated.IsZero() {
			updated = letter.CreatedAt
		}
		if updated.IsZero() || updated.After(cutoff) {
			continue
		}
		removed, err := checkpoint.deleteKey(ctx, s.Objects, object.Key)
		if err != nil {
			return deleted, keys, err
		}
		if removed {
			deleted++
			keys = append(keys, object.Key)
		}
	}
	if err := checkpoint.pauseAfterPage(next); err != nil {
		return deleted, keys, err
	}
	return deleted, keys, nil
}

func (s *TenantStore) cleanupIndexOrphansLocked(ctx context.Context, tenantID string) error {
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil {
		if err := s.cleanupObsoleteIndexObjects(ctx, tenantID, IndexCatalog{}, catalog); err != nil {
			return err
		}
	}
	return s.cleanupReverseIndexOrphans(ctx, tenantID)
}

func (s *TenantStore) cleanupEntityRecordsLocked(ctx context.Context, tenantID string) (int, []string, error) {
	deleted := 0
	keys := make([]string, 0)
	err := scanObjectPrefix(
		ctx,
		s.Objects,
		s.entityRecordPrefix(tenantID),
		func(objects []ObjectInfo) error {
			for _, object := range objects {
				if _, ok, err := s.entityIDFromRecordKey(
					tenantID, object.Key,
				); err != nil {
					return err
				} else if !ok {
					continue
				}
				if err := s.deleteListedObject(ctx, object); err != nil {
					return err
				}
				deleted++
				keys = append(keys, object.Key)
			}
			return nil
		},
	)
	if err != nil {
		return deleted, keys, err
	}
	return deleted, keys, nil
}

type taskCleanupReport struct {
	DeletedTasks         int
	DeletedTaskResults   int
	DeletedImportSources int
	DeletedIndexTasks    int
	DeletedKeys          []string
}

func (s *TenantStore) cleanupUnifiedTasksLocked(ctx context.Context, tenantID string, cutoff time.Time, report *taskCleanupReport, checkpoint *gcCheckpointRunner) error {
	prefix := s.taskPrefix(tenantID)
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	for _, object := range objects {
		rest := strings.TrimPrefix(object.Key, prefix)
		if strings.Contains(rest, "/") {
			continue
		}
		taskID, ok := taskIDFromKey(object.Key)
		if !ok {
			continue
		}
		data, err := s.Objects.Get(ctx, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if !isParquetBytes(data) {
			continue
		}
		task, err := decodeParquetTask(ctx, data)
		if err != nil {
			continue
		}
		if task.TenantID != tenantID ||
			task.ID != taskID ||
			(taskStillActive(task) &&
				!s.taskOwnerStopped(ctx, task, time.Now().UTC())) ||
			!taskExpired(
				task.StartedAt,
				task.UpdatedAt,
				task.FinishedAt,
				cutoff,
			) {
			continue
		}
		if checkpoint.options.DryRun {
			if err := s.planExpiredTaskGroup(
				ctx, tenantID, task, object.Key, checkpoint,
			); err != nil {
				return err
			}
			if checkpoint.checkpoint.Paused {
				return errGCPaused
			}
			continue
		}
		if task.Type == TaskTypeBulkImport {
			sourceKey := stringTaskParam(task.Params, "source_key")
			if sourceKey != "" {
				if err := s.validateImportSourceKey(tenantID, sourceKey); err != nil {
					return err
				}
				deleted, err := checkpoint.deleteExistingKeyIgnoringCursor(
					ctx, s.Objects, sourceKey,
				)
				if gcPaused(err) || checkpoint.checkpoint.Paused {
					checkpoint.pauseBeforeObject(object.Key)
					return errGCPaused
				}
				if err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
				if deleted {
					report.DeletedImportSources++
					report.DeletedKeys = append(report.DeletedKeys, sourceKey)
				}
			}
		}
		if task.ResultKey != "" {
			if err := s.validateTenantObjectKey(tenantID, task.ResultKey); err != nil {
				return err
			}
			deleted, err := checkpoint.deleteExistingKeyIgnoringCursor(
				ctx, s.Objects, task.ResultKey,
			)
			if gcPaused(err) || checkpoint.checkpoint.Paused {
				checkpoint.pauseBeforeObject(object.Key)
				return errGCPaused
			}
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			if deleted {
				report.DeletedTaskResults++
				report.DeletedKeys = append(report.DeletedKeys, task.ResultKey)
			}
		}
		deleted, err := checkpoint.deleteKeyIgnoringCursor(
			ctx, s.Objects, object.Key,
		)
		if err != nil {
			return err
		}
		if deleted {
			report.DeletedTasks++
			report.DeletedKeys = append(report.DeletedKeys, object.Key)
		}
		if checkpoint.checkpoint.Paused {
			return errGCPaused
		}
	}
	return checkpoint.pauseAfterPage(next)
}

func (s *TenantStore) cleanupIndexTasksLocked(ctx context.Context, tenantID string, cutoff time.Time, report *taskCleanupReport, checkpoint *gcCheckpointRunner) error {
	prefix := s.indexTaskPrefix(tenantID)
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	for _, object := range objects {
		taskID, ok := indexTaskIDFromKey(object.Key)
		if !ok {
			continue
		}
		task, _, err := s.getIndexTaskObjectWithMeta(
			ctx,
			tenantID,
			taskID,
		)
		if errors.Is(err, ErrNotFound) || errors.Is(err, errInvalidIndexTask) {
			continue
		}
		if err != nil {
			return err
		}
		if indexTaskStillActive(task) {
			active, err := s.indexTaskActive(
				ctx,
				tenantID,
				task,
				time.Now().UTC(),
			)
			if err != nil {
				return err
			}
			if active {
				continue
			}
		}
		if !taskExpired(
			task.StartedAt,
			task.UpdatedAt,
			task.FinishedAt,
			cutoff,
		) {
			continue
		}
		deleted, err := checkpoint.deleteKey(ctx, s.Objects, object.Key)
		if err != nil {
			return err
		}
		if deleted {
			report.DeletedIndexTasks++
			report.DeletedKeys = append(report.DeletedKeys, object.Key)
		}
	}
	return checkpoint.pauseAfterPage(next)
}

func taskStillActive(task Task) bool {
	return task.Status == "queued" || task.Status == "running"
}

func indexTaskStillActive(task IndexTask) bool {
	return task.Status == "queued" || task.Status == "running"
}

func taskExpired(startedAt time.Time, updatedAt time.Time, finishedAt time.Time, cutoff time.Time) bool {
	at := finishedAt
	if at.IsZero() {
		at = updatedAt
	}
	if at.IsZero() {
		at = startedAt
	}
	return !at.IsZero() && at.Before(cutoff)
}
