package storage

import (
	"context"
	"fmt"
	"sort"
	"time"
)

const gcBatchDeletes = 256

func (s *TenantStore) RunGC(ctx context.Context, tenantID string, options GCOptions) (GCReport, error) {
	if s.localFileStore() == nil {
		return s.runGCBatch(ctx, tenantID, options)
	}
	// Each batch reacquires the read-view and tenant locks, then reloads the
	// manifest and catalogs. A listing is only a candidate list, never a root.
	maxDeletes := max(0, options.MaxDeletes)
	options.MaxDeletes = gcBatchDeletes
	if maxDeletes > 0 {
		options.MaxDeletes = min(options.MaxDeletes, maxDeletes)
	}
	options.listings = make(map[string][]ObjectInfo)
	var admission *taskExecutionAdmission
	if parent, ok := ctx.Value(taskIngestAdmissionKey{}).(*taskExecutionAdmission); ok {
		parent.release()
		admission = &taskExecutionAdmission{execution: parent.execution}
		defer admission.release()
	}
	var report GCReport
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
		if next == "" || next <= options.CheckpointCursor {
			return report, fmt.Errorf("gc checkpoint did not advance from %q", options.CheckpointCursor)
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

func mergeGCReport(total *GCReport, batch GCReport) {
	if total.TenantID == "" {
		*total = batch
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
	total.DeletedKeys = append(total.DeletedKeys, batch.DeletedKeys...)
	c, b := &total.CommitCleanup, batch.CommitCleanup
	c.ManifestVersion = b.ManifestVersion
	c.ReferencedKeys = b.ReferencedKeys
	c.Deleted += b.Deleted
	c.KeptFuture += b.KeptFuture
	c.DeletedKeys = append(c.DeletedKeys, b.DeletedKeys...)
	c.FutureKeys = append(c.FutureKeys, b.FutureKeys...)
	c.InvalidKeys = append(c.InvalidKeys, b.InvalidKeys...)
	cp, bp := &total.Checkpoint, batch.Checkpoint
	cp.NextCursor, cp.LastKey = bp.NextCursor, bp.LastKey
	cp.ScannedKeys += bp.ScannedKeys
	cp.SkippedByCursor += bp.SkippedByCursor
	cp.Deleted += bp.Deleted
	cp.Planned += bp.Planned
	cp.DeletedKeys = append(cp.DeletedKeys, bp.DeletedKeys...)
	cp.PlannedKeys = append(cp.PlannedKeys, bp.PlannedKeys...)
	cp.FailedKeys = append(cp.FailedKeys, bp.FailedKeys...)
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
