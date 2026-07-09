package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const defaultGCReaderMaxAge = 5 * time.Minute

type GCOptions struct {
	KeepSnapshots           int
	DeadLetterMaxAge        time.Duration
	TaskMaxAge              time.Duration
	CleanupIndexOrphans     bool
	ReaderMaxAge            time.Duration
	CheckpointCursor        string
	MaxDeletes              int
	DryRun                  bool
	SkipEntityRecordCleanup bool
}

type GCReport struct {
	TenantID                   string        `json:"tenant_id"`
	ManifestVersion            int64         `json:"manifest_version"`
	DeletedSnapshots           int           `json:"deleted_snapshots"`
	DeletedDeadLetters         int           `json:"deleted_deadletters"`
	DeletedTasks               int           `json:"deleted_tasks"`
	DeletedTaskResults         int           `json:"deleted_task_results"`
	DeletedIndexTasks          int           `json:"deleted_index_tasks"`
	DeletedEntityRecords       int           `json:"deleted_entity_records"`
	ReaderWatermarkVersion     int64         `json:"reader_watermark_version,omitempty"`
	ReaderWatermarkReaders     int           `json:"reader_watermark_readers,omitempty"`
	ReaderWatermarkIgnored     int           `json:"reader_watermark_ignored,omitempty"`
	IndexCleanupAttempt        bool          `json:"index_cleanup_attempt"`
	CommitCleanupSkippedReason string        `json:"commit_cleanup_skipped_reason,omitempty"`
	IndexCleanupSkippedReason  string        `json:"index_cleanup_skipped_reason,omitempty"`
	IndexCleanupError          string        `json:"index_cleanup_error,omitempty"`
	CommitCleanup              CleanupReport `json:"commit_cleanup"`
	DeletedKeys                []string      `json:"deleted_keys,omitempty"`
	Checkpoint                 GCCheckpoint  `json:"checkpoint,omitempty"`
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
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return GCReport{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
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
	protection, err := s.gcReaderProtection(ctx, tenantID, options.ReaderMaxAge, time.Now().UTC())
	if err != nil {
		return finish(err)
	}
	report.ReaderWatermarkVersion = protection.Watermark
	report.ReaderWatermarkReaders = protection.Active
	report.ReaderWatermarkIgnored = protection.Ignored
	if protection.activeReaderBehind(manifest.Version) {
		report.CommitCleanupSkippedReason = fmt.Sprintf("active reader watermark %d is behind manifest version %d", protection.Watermark, manifest.Version)
	} else {
		commitReport, err := s.cleanupCommitsLocked(ctx, tenantID, manifest, checkpoint)
		report.CommitCleanup = commitReport
		report.DeletedKeys = append(report.DeletedKeys, commitReport.DeletedKeys...)
		if err != nil {
			return finish(err)
		}
	}
	deleted, keys, err := s.cleanupSnapshotsLocked(ctx, tenantID, manifest, options.KeepSnapshots, protection, checkpoint)
	report.DeletedSnapshots = deleted
	report.DeletedKeys = append(report.DeletedKeys, keys...)
	if err != nil {
		return finish(err)
	}
	if options.DeadLetterMaxAge > 0 {
		deleted, keys, err = s.cleanupDeadLettersLocked(ctx, tenantID, options.DeadLetterMaxAge, checkpoint)
		report.DeletedDeadLetters = deleted
		report.DeletedKeys = append(report.DeletedKeys, keys...)
		if err != nil {
			return finish(err)
		}
	}
	if options.TaskMaxAge > 0 {
		taskReport, err := s.cleanupTasksLocked(ctx, tenantID, options.TaskMaxAge, checkpoint)
		report.DeletedTasks = taskReport.DeletedTasks
		report.DeletedTaskResults = taskReport.DeletedTaskResults
		report.DeletedIndexTasks = taskReport.DeletedIndexTasks
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

func (p gcReaderProtection) activeReaderBehind(manifestVersion int64) bool {
	return p.Active > 0 && p.Watermark > 0 && p.Watermark < manifestVersion
}

func (s *TenantStore) gcReaderProtection(ctx context.Context, tenantID string, maxAge time.Duration, now time.Time) (gcReaderProtection, error) {
	if maxAge < 0 {
		return gcReaderProtection{}, nil
	}
	if maxAge == 0 {
		maxAge = defaultGCReaderMaxAge
	}
	heartbeats, err := s.ListReaderHeartbeats(ctx, tenantID)
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
	scan, err := s.loadCommitObjects(ctx, tenantID, referenced)
	if err != nil {
		return report, err
	}
	report.InvalidKeys = scan.InvalidKeys
	for _, item := range scan.Items {
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
	if err := s.cleanupCommitSegmentsLocked(ctx, tenantID, manifest, referenced, &report, checkpoint); err != nil {
		return report, err
	}
	return report, nil
}

func (s *TenantStore) cleanupCommitSegmentsLocked(ctx context.Context, tenantID string, manifest Manifest, referenced map[string]struct{}, report *CleanupReport, checkpoint *gcCheckpointRunner) error {
	checkpoint.addPrefix(s.commitSegmentPrefix(tenantID))
	objects, err := s.Objects.List(ctx, s.commitSegmentPrefix(tenantID))
	if err != nil {
		return err
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
	return nil
}

func (s *TenantStore) cleanupSnapshotsLocked(ctx context.Context, tenantID string, manifest Manifest, keepSnapshots int, protection gcReaderProtection, checkpoint *gcCheckpointRunner) (int, []string, error) {
	if keepSnapshots < 1 {
		keepSnapshots = 1
	}
	checkpoint.addPrefix(s.snapshotPrefix(tenantID))
	objects, err := s.Objects.List(ctx, s.snapshotPrefix(tenantID))
	if err != nil {
		return 0, nil, err
	}
	type snapshotObject struct {
		Key     string
		Version int64
	}
	items := make([]snapshotObject, 0, len(objects))
	for _, object := range objects {
		version, ok := snapshotIdentityFromKey(object.Key)
		if !ok {
			continue
		}
		items = append(items, snapshotObject{Key: object.Key, Version: version})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Version == items[j].Version {
			return items[i].Key > items[j].Key
		}
		return items[i].Version > items[j].Version
	})
	keep := map[string]struct{}{}
	if manifest.SnapshotKey != "" {
		keep[manifest.SnapshotKey] = struct{}{}
	}
	for i, item := range items {
		if i < keepSnapshots {
			keep[item.Key] = struct{}{}
		}
	}
	snapshotVersionProtected := map[int64]struct{}{}
	for _, snapshotVersion := range protection.SnapshotVersions {
		snapshotVersionProtected[snapshotVersion] = struct{}{}
	}
	for _, item := range items {
		if _, ok := snapshotVersionProtected[item.Version]; ok {
			keep[item.Key] = struct{}{}
		}
	}
	for _, visibleVersion := range protection.VisibleVersions {
		if _, ok := snapshotVersionProtected[visibleVersion]; ok {
			continue
		}
		for _, item := range items {
			if item.Version <= visibleVersion {
				keep[item.Key] = struct{}{}
				break
			}
		}
	}
	deleted := 0
	keys := make([]string, 0)
	for _, item := range items {
		if _, ok := keep[item.Key]; ok {
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
	return deleted, keys, nil
}

func (s *TenantStore) deleteShardedSnapshotVersionObjectsLocked(ctx context.Context, tenantID string, version int64, checkpoint *gcCheckpointRunner) ([]string, error) {
	prefix := s.snapshotVersionPrefix(tenantID, version) + "/"
	checkpoint.addPrefix(prefix)
	objects, err := s.Objects.List(ctx, prefix)
	if err != nil {
		return nil, err
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
	return keys, nil
}

func (s *TenantStore) cleanupDeadLettersLocked(ctx context.Context, tenantID string, maxAge time.Duration, checkpoint *gcCheckpointRunner) (int, []string, error) {
	prefix := s.tenantObjectPrefix(tenantID) + "ingest/"
	checkpoint.addPrefix(prefix)
	objects, err := s.Objects.List(ctx, prefix)
	if err != nil {
		return 0, nil, err
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
	return deleted, keys, nil
}

func (s *TenantStore) cleanupIndexOrphansLocked(ctx context.Context, tenantID string) error {
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.cleanupObsoleteIndexObjects(ctx, tenantID, IndexCatalog{}, catalog)
}

func (s *TenantStore) cleanupEntityRecordsLocked(ctx context.Context, tenantID string) (int, []string, error) {
	objects, err := s.Objects.List(ctx, s.entityRecordPrefix(tenantID))
	if err != nil {
		return 0, nil, err
	}
	deleted := 0
	keys := make([]string, 0)
	for _, object := range objects {
		if _, ok, err := s.entityIDFromRecordKey(tenantID, object.Key); err != nil {
			return deleted, keys, err
		} else if !ok {
			continue
		}
		if err := s.deleteListedObject(ctx, object); err != nil {
			return deleted, keys, err
		}
		deleted++
		keys = append(keys, object.Key)
	}
	return deleted, keys, nil
}

type taskCleanupReport struct {
	DeletedTasks       int
	DeletedTaskResults int
	DeletedIndexTasks  int
	DeletedKeys        []string
}

func (s *TenantStore) cleanupTasksLocked(ctx context.Context, tenantID string, maxAge time.Duration, checkpoint *gcCheckpointRunner) (taskCleanupReport, error) {
	cutoff := time.Now().UTC().Add(-maxAge)
	report := taskCleanupReport{}
	if err := s.cleanupUnifiedTasksLocked(ctx, tenantID, cutoff, &report, checkpoint); err != nil {
		return report, err
	}
	if err := s.cleanupIndexTasksLocked(ctx, tenantID, cutoff, &report, checkpoint); err != nil {
		return report, err
	}
	return report, nil
}

func (s *TenantStore) cleanupUnifiedTasksLocked(ctx context.Context, tenantID string, cutoff time.Time, report *taskCleanupReport, checkpoint *gcCheckpointRunner) error {
	checkpoint.addPrefix(s.taskPrefix(tenantID))
	objects, err := s.Objects.List(ctx, s.taskPrefix(tenantID))
	if err != nil {
		return err
	}
	prefix := s.taskPrefix(tenantID)
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
		if task.TenantID != tenantID || task.ID != taskID || taskStillActive(task) || !taskExpired(task.StartedAt, task.UpdatedAt, task.FinishedAt, cutoff) {
			continue
		}
		if task.ResultKey != "" {
			if err := s.validateTenantObjectKey(tenantID, task.ResultKey); err != nil {
				return err
			}
			deleted, err := checkpoint.deleteKey(ctx, s.Objects, task.ResultKey)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			if deleted {
				report.DeletedTaskResults++
				report.DeletedKeys = append(report.DeletedKeys, task.ResultKey)
			}
		}
		deleted, err := checkpoint.deleteKey(ctx, s.Objects, object.Key)
		if err != nil {
			return err
		}
		if deleted {
			report.DeletedTasks++
			report.DeletedKeys = append(report.DeletedKeys, object.Key)
		}
	}
	return nil
}

func (s *TenantStore) cleanupIndexTasksLocked(ctx context.Context, tenantID string, cutoff time.Time, report *taskCleanupReport, checkpoint *gcCheckpointRunner) error {
	checkpoint.addPrefix(s.indexTaskPrefix(tenantID))
	objects, err := s.Objects.List(ctx, s.indexTaskPrefix(tenantID))
	if err != nil {
		return err
	}
	for _, object := range objects {
		taskID, ok := indexTaskIDFromKey(object.Key)
		if !ok {
			continue
		}
		task, err := s.GetIndexTask(ctx, tenantID, taskID)
		if errors.Is(err, ErrNotFound) || errors.Is(err, errInvalidIndexTask) {
			continue
		}
		if err != nil {
			return err
		}
		if indexTaskStillActive(task) || !taskExpired(task.StartedAt, task.UpdatedAt, task.FinishedAt, cutoff) {
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
	return nil
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
