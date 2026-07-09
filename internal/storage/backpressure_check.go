package storage

import (
	"context"
	"errors"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

func (s *TenantStore) CheckWriteBackpressure(ctx context.Context, tenantID string) (err error) {
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
			return newBackpressureError([]BackpressureReason{reason}, s.Backpressure.Config().RetryAfter)
		}
		return err
	}
	reasons := s.Backpressure.ReasonsWithConfig(tenantID, config)
	span.SetAttributes(
		attribute.Int("graphdb.write_backpressure.reasons_initial", len(reasons)),
		attribute.Int("graphdb.write_backpressure.max_commit_tail", config.MaxCommitTail),
		attribute.Int64("graphdb.write_backpressure.retry_after_ms", config.RetryAfter.Milliseconds()),
	)
	manifest, err := s.currentManifestForWriteAdmission(ctx, tenantID)
	if err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return newBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter)
		}
		return err
	}
	tailLength := manifestCommitTailLength(manifest)
	span.SetAttributes(
		attribute.Int("graphdb.write_backpressure.commit_tail_length", tailLength),
		attribute.Int64("graphdb.write_backpressure.current_manifest_version", manifest.Version),
	)
	s.Backpressure.RecordCommitTail(tenantID, tailLength)
	if s.BackpressureObserver != nil {
		s.BackpressureObserver.RecordCommitTail(tenantID, tailLength)
	}
	if config.MaxCommitTail > 0 && tailLength > config.MaxCommitTail {
		reasons = append(reasons, BackpressureReason{
			Code:      "commit_tail_too_long",
			Current:   float64(tailLength),
			Threshold: float64(config.MaxCommitTail),
			Message:   "compact required",
		})
	}
	if task, ok, err := s.findRunningIndexRebuildTask(ctx, tenantID); err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return newBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter)
		}
		return err
	} else if ok {
		span.SetAttributes(
			attribute.Bool("graphdb.write_backpressure.index_rebuild_running", true),
			attribute.String("graphdb.write_backpressure.index_rebuild_task_id", task.ID),
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
			return newBackpressureError(appendBackpressureReasons(reasons, reason), config.RetryAfter)
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
	reasons = appendBackpressureReasons(reasons, s.Backpressure.ReasonsWithConfig(tenantID, config)...)
	span.SetAttributes(attribute.Int("graphdb.write_backpressure.reasons_final", len(reasons)))
	return newBackpressureError(reasons, config.RetryAfter)
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
	manifest, _, err = s.getManifest(ctx, tenantID)
	return manifest, err
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
	if s.BackpressureObserver != nil {
		s.BackpressureObserver.RecordManifestCASConflict(tenantID)
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
	tasks, err := s.ListTasks(ctx, tenantID, TaskListOptions{Type: taskType, Status: "running", Limit: 1})
	if err != nil {
		return Task{}, false, err
	}
	span.SetAttributes(attribute.Int("graphdb.task.matched", len(tasks)))
	if len(tasks) == 0 {
		return Task{}, false, nil
	}
	return tasks[0], true, nil
}
