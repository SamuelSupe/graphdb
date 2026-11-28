package storage

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const gcBatchDeletes = 4096

func (s *TenantStore) RunGC(ctx context.Context, tenantID string, options GCOptions) (report GCReport, err error) {
	if s.localFileStore() == nil {
		return s.runGCBatch(ctx, tenantID, options)
	}
	cache := &gcAdjacencyCache{}
	options.retainedAdjacency = cache
	prefix := s.tenantObjectPrefix(tenantID)
	stop := s.localFileStore().OnChange(func(key string) {
		if !strings.HasPrefix(key, prefix) {
			return
		}
		name := strings.TrimPrefix(key, prefix)
		if name == "manifest.parquet" || name == "metadata.parquet" || strings.HasSuffix(name, "/catalog.parquet") || strings.HasSuffix(name, "/catalog.json") {
			// Historical removal cannot introduce a reference. GC itself removes
			// catalogs; treating those deletions as publication defeats the mark cache.
			if strings.HasPrefix(name, "indexes/parquet/versions/") || strings.HasPrefix(name, "snapshots/sharded/") {
				if filename, err := s.localFileStore().path(key); err == nil {
					if _, err = os.Lstat(filename); os.IsNotExist(err) {
						return
					}
				}
			}
			cache.generation.Add(1)
		}
	})
	defer stop()
	// Each batch reacquires the read-view and tenant locks, then reloads the
	// manifest and catalogs. A listing is only a candidate list, never a root.
	maxDeletes := max(0, options.MaxDeletes)
	options.MaxDeletes = gcBatchDeletes
	if maxDeletes > 0 {
		options.MaxDeletes = min(options.MaxDeletes, maxDeletes)
	}
	options.listings = make(map[string]gcListing)
	options.replicated = IsReplicatedContext(ctx)
	if !options.replicated {
		options.listBefore = time.Now()
	}
	ctx = WithFileListPageCache(ctx)
	var admission *taskExecutionAdmission
	if parent, ok := s.taskAdmissionFromContext(ctx); ok {
		parent.release()
		admission = &taskExecutionAdmission{store: s, execution: parent.execution}
		defer func() {
			admission.release()
			if !parent.acquire(ctx) && err == nil {
				err = ctx.Err()
			}
		}()
	}
	started := time.Now()
	for {
		if admission != nil && !admission.acquire(ctx) {
			return report, ctx.Err()
		}
		batch, err := s.runGCBatch(ctx, tenantID, options)
		if admission != nil {
			admission.release()
		}
		mergeGCReport(&report, batch)
		report.Checkpoint.MaxDeletes = maxDeletes
		used := report.Checkpoint.Deleted + report.Checkpoint.Planned
		if err != nil || !batch.Checkpoint.Paused || batch.IndexCleanupError != "" || (maxDeletes > 0 && used >= maxDeletes) {
			sort.Strings(report.DeletedKeys)
			return report, err
		}
		next := batch.Checkpoint.NextCursor
		if !gcCursorAdvanced(options.CheckpointCursor, next) {
			return report, fmt.Errorf("gc checkpoint did not advance from %q", options.CheckpointCursor)
		}
		if options.RunBudget > 0 && time.Since(started) >= options.RunBudget {
			sort.Strings(report.DeletedKeys)
			return report, nil
		}
		options.CheckpointCursor = next
		if maxDeletes > 0 {
			options.MaxDeletes = min(gcBatchDeletes, maxDeletes-used)
		}
		// Give waiting foreground operations a turn before requesting exclusivity.
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return report, ctx.Err()
		case <-timer.C:
		}
	}
}

func gcCursorAdvanced(prev, next string) bool {
	if next == "" || next == prev {
		return false
	}
	if prev == "" {
		return true
	}
	if prevPrefix, prevVer, ok := splitShardedSnapshotCursor(prev); ok {
		if nextPrefix, nextVer, nextSharded := splitShardedSnapshotCursor(next); nextSharded && nextPrefix == prevPrefix {
			return nextVer > prevVer || (nextVer == prevVer && next > prev)
		}
		if strings.HasPrefix(next, prevPrefix) {
			if nextVer, ok := snapshotIdentityFromKey(next); ok {
				return nextVer >= prevVer
			}
			// Transition from sharded/v<N>/... into ~sharded-orphans/ is forward.
			return next > prev
		}
		return next > prevPrefix
	}
	return next > prev
}

func splitShardedSnapshotCursor(cursor string) (snapshotPrefix string, version int64, ok bool) {
	idx := strings.Index(cursor, "/snapshots/sharded/v")
	if idx < 0 {
		return "", 0, false
	}
	snapshotPrefix = cursor[:idx+len("/snapshots/")]
	version, ok = shardedSnapshotVersionFromCursor(snapshotPrefix, cursor)
	return snapshotPrefix, version, ok
}

const gcReportKeySampleLimit = 10000

func appendBoundedKeys(dst []string, src []string) []string {
	if len(src) == 0 || len(dst) >= gcReportKeySampleLimit {
		return dst
	}
	remaining := gcReportKeySampleLimit - len(dst)
	if len(src) > remaining {
		src = src[:remaining]
	}
	return append(dst, src...)
}

func mergeGCReport(total *GCReport, batch GCReport) {
	if total.TenantID == "" {
		*total = batch
		if len(total.DeletedKeys) > gcReportKeySampleLimit {
			total.DeletedKeys = total.DeletedKeys[:gcReportKeySampleLimit]
		}
		if len(total.CommitCleanup.DeletedKeys) > gcReportKeySampleLimit {
			total.CommitCleanup.DeletedKeys = total.CommitCleanup.DeletedKeys[:gcReportKeySampleLimit]
		}
		if len(total.Checkpoint.DeletedKeys) > gcReportKeySampleLimit {
			total.Checkpoint.DeletedKeys = total.Checkpoint.DeletedKeys[:gcReportKeySampleLimit]
		}
		if len(total.Checkpoint.PlannedKeys) > gcReportKeySampleLimit {
			total.Checkpoint.PlannedKeys = total.Checkpoint.PlannedKeys[:gcReportKeySampleLimit]
		}
		return
	}
	total.ManifestVersion = batch.ManifestVersion
	total.DeletedSnapshots += batch.DeletedSnapshots
	total.DeletedDeadLetters += batch.DeletedDeadLetters
	total.DeletedTasks += batch.DeletedTasks
	total.DeletedTaskResults += batch.DeletedTaskResults
	total.DeletedImportSources += batch.DeletedImportSources
	total.DeletedIndexTasks += batch.DeletedIndexTasks
	total.DeletedEntityRecords += batch.DeletedEntityRecords
	total.ReaderWatermarkVersion = batch.ReaderWatermarkVersion
	total.ReaderWatermarkReaders = batch.ReaderWatermarkReaders
	total.ReaderWatermarkIgnored = batch.ReaderWatermarkIgnored
	total.IndexCleanupAttempt = total.IndexCleanupAttempt || batch.IndexCleanupAttempt
	if batch.CommitCleanupSkippedReason != "" {
		total.CommitCleanupSkippedReason = batch.CommitCleanupSkippedReason
	}
	if batch.IndexCleanupSkippedReason != "" {
		total.IndexCleanupSkippedReason = batch.IndexCleanupSkippedReason
	}
	if batch.SnapshotCleanupSkippedReason != "" {
		total.SnapshotCleanupSkippedReason = batch.SnapshotCleanupSkippedReason
	}
	total.IndexCleanupError = batch.IndexCleanupError
	total.DeletedKeys = appendBoundedKeys(total.DeletedKeys, batch.DeletedKeys)
	c, b := &total.CommitCleanup, batch.CommitCleanup
	c.ManifestVersion = b.ManifestVersion
	c.ReferencedKeys = b.ReferencedKeys
	c.Deleted += b.Deleted
	c.KeptFuture += b.KeptFuture
	c.DeletedKeys = appendBoundedKeys(c.DeletedKeys, b.DeletedKeys)
	c.FutureKeys = appendBoundedKeys(c.FutureKeys, b.FutureKeys)
	c.InvalidKeys = appendBoundedKeys(c.InvalidKeys, b.InvalidKeys)
	cp, bp := &total.Checkpoint, batch.Checkpoint
	cp.NextCursor, cp.LastKey = bp.NextCursor, bp.LastKey
	cp.ScannedKeys += bp.ScannedKeys
	cp.DeferredFiles += bp.DeferredFiles
	cp.SkippedByCursor += bp.SkippedByCursor
	cp.Deleted += bp.Deleted
	cp.Planned += bp.Planned
	cp.DeletedKeys = appendBoundedKeys(cp.DeletedKeys, bp.DeletedKeys)
	cp.PlannedKeys = appendBoundedKeys(cp.PlannedKeys, bp.PlannedKeys)
	cp.FailedKeys = appendBoundedKeys(cp.FailedKeys, bp.FailedKeys)
	for _, prefix := range bp.ScannedPrefixes {
		found := false
		for _, seen := range cp.ScannedPrefixes {
			if seen == prefix {
				found = true
				break
			}
		}
		if !found {
			cp.ScannedPrefixes = append(cp.ScannedPrefixes, prefix)
		}
	}
	cp.Paused, cp.Completed = bp.Paused, bp.Completed
}
