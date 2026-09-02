package storage

import (
	"context"
	"errors"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

func (s *TenantStore) CheckWriteBackpressure(ctx context.Context, tenantID string) (err error) {
	return s.checkWriteBackpressure(ctx, tenantID, true)
}

func (s *TenantStore) checkWriteBackpressure(ctx context.Context, tenantID string, authoritative bool) (err error) {
	return s.checkWriteBackpressureWithOptions(ctx, tenantID, authoritative, writeBackpressureCheckOptions{})
}

type writeBackpressureCheckOptions struct {
	ignoreCASConflicts bool
}

func (s *TenantStore) checkAcceptedWALBackpressure(ctx context.Context, tenantID string, authoritative bool) error {
	return s.checkWriteBackpressureWithOptions(ctx, tenantID, authoritative, writeBackpressureCheckOptions{
		ignoreCASConflicts: s.coordinated(),
	})
}

func (s *TenantStore) checkWriteBackpressureWithOptions(
	ctx context.Context,
	tenantID string,
	authoritative bool,
	options writeBackpressureCheckOptions,
) (err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.write_backpressure.check", tenantTraceAttr(tenantID))
	defer func() {
		endStorageSpan(span, err)
	}()
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if s.Backpressure == nil {
		span.SetAttributes(attribute.Bool("graphdb.write_backpressure.enabled", false))
		return nil
	}
	span.SetAttributes(attribute.Bool("graphdb.write_backpressure.enabled", true))
	config, err := s.effectiveBackpressureConfig(ctx, tenantID)
	if err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return newCheckedBackpressureError([]BackpressureReason{reason}, s.Backpressure.Config().RetryAfter, options)
		}
		return err
	}
	reasons := filterCheckedBackpressureReasons(s.Backpressure.ReasonsWithConfig(tenantID, config), options)
	span.SetAttributes(
		attribute.Bool("graphdb.write_backpressure.authoritative", authoritative),
		attribute.Int("graphdb.write_backpressure.reasons_initial", len(reasons)),
		attribute.Int("graphdb.write_backpressure.max_commit_tail", config.MaxCommitTail),
		attribute.Int64("graphdb.write_backpressure.retry_after_ms", config.RetryAfter.Milliseconds()),
	)
	if !authoritative {
		span.SetAttributes(attribute.Int("graphdb.write_backpressure.reasons_final", len(reasons)))
		return newCheckedBackpressureError(reasons, config.RetryAfter, options)
	}
	manifest, err := s.currentManifestForWriteAdmission(ctx, tenantID)
	if err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return newCheckedBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter, options)
		}
		return err
	}
	tailLength := manifestCommitTailLength(manifest)
	span.SetAttributes(
		attribute.Int("graphdb.write_backpressure.commit_tail_length", tailLength),
		attribute.Int64("graphdb.write_backpressure.current_manifest_version", manifest.Version),
	)
	s.Backpressure.RecordCommitTail(tenantID, tailLength)
	if s.backpressureObserver != nil {
		s.backpressureObserver.RecordCommitTail(tenantID, tailLength)
	}
	if config.MaxCommitTail > 0 && tailLength > config.MaxCommitTail {
		reasons = append(reasons, BackpressureReason{
			Code:      "commit_tail_too_long",
			Current:   float64(tailLength),
			Threshold: float64(config.MaxCommitTail),
			Message:   "compact required",
		})
	}
	indexTask, indexTaskRunning, err := s.findRunningTask(
		ctx, tenantID, TaskTypeIndexRebuild,
	)
	if err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return newCheckedBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter, options)
		}
		return err
	}
	var legacyIndexTask IndexTask
	legacyIndexTaskRunning := false
	if !indexTaskRunning {
		legacyIndexTask, legacyIndexTaskRunning, _, err = s.findRunningIndexRebuildTask(ctx, tenantID)
		if err != nil {
			if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
				return newCheckedBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter, options)
			}
			return err
		}
	}
	if indexTaskRunning || legacyIndexTaskRunning {
		taskID := indexTask.ID
		if taskID == "" {
			taskID = legacyIndexTask.ID
		}
		span.SetAttributes(
			attribute.Bool("graphdb.write_backpressure.index_rebuild_running", true),
			attribute.String("graphdb.write_backpressure.index_rebuild_task_id", taskID),
		)
		reasons = append(reasons, BackpressureReason{
			Code:    "index_rebuild_running",
			Current: 1,
			Message: "index rebuild is running",
		})
	} else {
		span.SetAttributes(attribute.Bool("graphdb.write_backpressure.index_rebuild_running", false))
	}
	if task, ok, err := s.findRunningTask(ctx, tenantID, TaskTypeGC); err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return newCheckedBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter, options)
		}
		return err
	} else if ok {
		span.SetAttributes(
			attribute.Bool("graphdb.write_backpressure.gc_running", true),
			attribute.String("graphdb.write_backpressure.gc_task_id", task.ID),
		)
		reasons = append(reasons, BackpressureReason{
			Code:    "gc_running",
			Current: 1,
			Message: "gc is running",
		})
	} else {
		span.SetAttributes(attribute.Bool("graphdb.write_backpressure.gc_running", false))
	}
	reasons = filterCheckedBackpressureReasons(
		appendBackpressureReasons(reasons, s.Backpressure.ReasonsWithConfig(tenantID, config)...),
		options,
	)
	span.SetAttributes(attribute.Int("graphdb.write_backpressure.reasons_final", len(reasons)))
	return newCheckedBackpressureError(reasons, config.RetryAfter, options)
}

func newCheckedBackpressureError(
	reasons []BackpressureReason,
	retryAfter time.Duration,
	options writeBackpressureCheckOptions,
) error {
	return newBackpressureError(filterCheckedBackpressureReasons(reasons, options), retryAfter)
}

func filterCheckedBackpressureReasons(
	reasons []BackpressureReason,
	options writeBackpressureCheckOptions,
) []BackpressureReason {
	if !options.ignoreCASConflicts {
		return reasons
	}
	filtered := reasons[:0]
	for _, reason := range reasons {
		if reason.Code != "manifest_cas_conflicts_high" {
			filtered = append(filtered, reason)
		}
	}
	return filtered
}

func (s *TenantStore) currentManifestForWriteAdmission(ctx context.Context, tenantID string) (manifest Manifest, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.write_backpressure.current_manifest", tenantTraceAttr(tenantID))
	defer func() {
		if err == nil {
			span.SetAttributes(manifestTraceAttrs("graphdb.write_backpressure.current_manifest", manifest)...)
		}
		endStorageSpan(span, err)
	}()
	if loaded, ok := s.getWriteCache(tenantID); ok {
		if !s.coordinated() {
			if _, _, leaseOK := s.getCachedWriterLease(
				tenantID, time.Now().UTC(),
			); !leaseOK {
				span.SetAttributes(
					attribute.Bool("graphdb.write_cache.found", true),
					attribute.Bool("graphdb.write_cache.hit", false),
				)
				return s.currentManifestWithoutWriteCache(ctx, tenantID)
			}
		} else {
			head, exists, headErr := s.Coordinator.Head(ctx, tenantID)
			if headErr != nil {
				return Manifest{}, headErr
			}
			if !exists {
				span.SetAttributes(
					attribute.Bool("graphdb.write_cache.found", true),
					attribute.Bool("graphdb.write_cache.hit", false),
				)
				return s.currentManifestWithoutWriteCache(ctx, tenantID)
			}
			if !writeCacheMatchesCoordinatorHead(loaded, head) {
				manifest, _, err = s.getCoordinatedManifestAtHead(
					ctx, tenantID, head,
				)
				span.SetAttributes(
					attribute.Bool("graphdb.write_cache.found", true),
					attribute.Bool("graphdb.write_cache.hit", false),
					attribute.Int64("graphdb.write_cache.current_manifest_version", manifest.Version),
				)
				return manifest, err
			}
		}
		span.SetAttributes(
			attribute.Bool("graphdb.write_cache.found", true),
			attribute.Bool("graphdb.write_cache.hit", true),
			attribute.Int64("graphdb.write_cache.current_manifest_version", loaded.Manifest.Version),
		)
		return loaded.Manifest, nil
	}
	span.SetAttributes(
		attribute.Bool("graphdb.write_cache.found", false),
		attribute.Bool("graphdb.write_cache.hit", false),
	)
	return s.currentManifestWithoutWriteCache(ctx, tenantID)
}

func (s *TenantStore) currentManifestWithoutWriteCache(
	ctx context.Context,
	tenantID string,
) (Manifest, error) {
	manifest, _, err := s.getManifest(ctx, tenantID)
	return manifest, err
}

func writeCacheMatchesCoordinatorHead(
	loaded loadedGraph,
	head CoordinationHead,
) bool {
	return loaded.Graph != nil &&
		loaded.Graph.Version == head.GraphVersion &&
		manifestMetaMatchesCoordinatorHead(loaded.Manifest, loaded.Meta, head)
}

func objectStoreUnavailableBackpressureReason(err error) (BackpressureReason, bool) {
	if !errors.Is(err, ErrObjectStoreUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
		return BackpressureReason{}, false
	}
	return BackpressureReason{
		Code:    "object_store_unavailable",
		Current: 1,
		Message: "object store is unavailable for write admission checks",
	}, true
}

func (s *TenantStore) objectStoreBackpressureError(err error) error {
	if s == nil || s.Backpressure == nil {
		return nil
	}
	reason, ok := objectStoreUnavailableBackpressureReason(err)
	if !ok {
		return nil
	}
	return newBackpressureError([]BackpressureReason{reason}, s.Backpressure.Config().RetryAfter)
}

func (s *TenantStore) checkQuotaAfterApply(ctx context.Context, tenantID string, previous *graph.Graph, next *graph.Graph) error {
	if s.Backpressure == nil || next == nil {
		return nil
	}
	config, err := s.effectiveBackpressureConfig(ctx, tenantID)
	if err != nil {
		return err
	}
	reasons := make([]BackpressureReason, 0, 2)
	if config.MaxEntitiesPerTenant > 0 && len(next.Entities) > config.MaxEntitiesPerTenant {
		reasons = append(reasons, BackpressureReason{
			Code:      "tenant_entity_quota_exceeded",
			Current:   float64(len(next.Entities)),
			Threshold: float64(config.MaxEntitiesPerTenant),
			Message:   "tenant entity quota exceeded",
		})
	}
	if config.MaxEdgesPerTenant > 0 && len(next.Edges) > config.MaxEdgesPerTenant {
		reasons = append(reasons, BackpressureReason{
			Code:      "tenant_edge_quota_exceeded",
			Current:   float64(len(next.Edges)),
			Threshold: float64(config.MaxEdgesPerTenant),
			Message:   "tenant edge quota exceeded",
		})
	}
	if len(reasons) == 0 || quotaDecreased(previous, next, config) {
		return nil
	}
	return newBackpressureError(reasons, config.RetryAfter)
}

func quotaDecreased(previous *graph.Graph, next *graph.Graph, config BackpressureConfig) bool {
	if previous == nil || next == nil {
		return false
	}
	entityOK := config.MaxEntitiesPerTenant <= 0 || len(next.Entities) <= config.MaxEntitiesPerTenant || len(next.Entities) < len(previous.Entities)
	edgeOK := config.MaxEdgesPerTenant <= 0 || len(next.Edges) <= config.MaxEdgesPerTenant || len(next.Edges) < len(previous.Edges)
	return entityOK && edgeOK
}

func (s *TenantStore) recordManifestCASConflict(tenantID string) {
	if s.Backpressure != nil {
		s.Backpressure.RecordManifestCASConflict(tenantID)
	}
	if s.backpressureObserver != nil {
		s.backpressureObserver.RecordManifestCASConflict(tenantID)
	}
}

func appendBackpressureReasons(base []BackpressureReason, extra ...BackpressureReason) []BackpressureReason {
	seen := map[string]struct{}{}
	for _, reason := range base {
		seen[reason.Code] = struct{}{}
	}
	for _, reason := range extra {
		if _, ok := seen[reason.Code]; ok {
			continue
		}
		seen[reason.Code] = struct{}{}
		base = append(base, reason)
	}
	return base
}

func (s *TenantStore) findRunningTask(ctx context.Context, tenantID string, taskType string) (task Task, found bool, err error) {
	spanName := "graphdb.storage.write_backpressure.find_running_task"
	if taskType == TaskTypeGC {
		spanName = "graphdb.storage.write_backpressure.find_running_gc_task"
	}
	ctx, span := startStorageSpan(ctx, spanName,
		tenantTraceAttr(tenantID),
		attribute.String("graphdb.task.type", taskType),
		attribute.String("graphdb.task.status", "running"),
	)
	defer func() {
		span.SetAttributes(attribute.Bool("graphdb.task.running_found", found))
		if found {
			span.SetAttributes(attribute.String("graphdb.task.id", task.ID))
		}
		endStorageSpan(span, err)
	}()
	if taskType == TaskTypeGC {
		return s.findRunningGCTask(ctx, tenantID)
	}
	if taskType == TaskTypeIndexRebuild {
		s.taskMu.Lock()
		active, ok := s.taskActive[taskActiveKey(tenantID, taskType)]
		s.taskMu.Unlock()
		if ok {
			return active, true, nil
		}
		if s.coordinated() {
			active, ok := s.findCoordinatorQueuedTask(ctx, Task{
				TenantID: tenantID,
				Type:     taskType,
			})
			return active, ok, nil
		}
	}
	return Task{}, false, nil
}
