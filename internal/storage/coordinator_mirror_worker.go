package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
)

const legacyManifestLeaseTTL = 30 * time.Second
const coordinatorMaintenanceBatchSize = 32

func (s *TenantStore) StartCoordinatorMaintenance(ctx context.Context, interval time.Duration) {
	if !s.coordinated() {
		return
	}
	s.StartCoordinatorCleanup(ctx)
	if interval <= 0 || interval > 5*time.Second {
		interval = time.Second
	}
	startCoordinatorMaintenanceLoop(ctx, interval, func(loopCtx context.Context) {
		_, _ = s.syncLegacyManifests(loopCtx, coordinatorMaintenanceBatchSize)
	})
	startCoordinatorMaintenanceLoop(ctx, interval, func(loopCtx context.Context) {
		_, _ = s.syncDerivedTasks(loopCtx, coordinatorMaintenanceBatchSize)
	})
}

func startCoordinatorMaintenanceLoop(
	ctx context.Context,
	interval time.Duration,
	run func(context.Context),
) {
	go func() {
		run(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run(ctx)
			}
		}
	}()
}

func (s *TenantStore) SyncDerivedTasks(ctx context.Context) (int, error) {
	return s.syncDerivedTasks(ctx, 0)
}

func (s *TenantStore) syncDerivedTasks(ctx context.Context, limit int) (int, error) {
	if !s.coordinated() {
		return 0, nil
	}
	processed := 0
	for {
		if limit > 0 && processed >= limit {
			return processed, nil
		}
		ownerToken, err := newCommitID()
		if err != nil {
			return processed, err
		}
		job, ok, err := s.Coordinator.ClaimDerivedTask(
			ctx, s.InstanceID+"-"+ownerToken, s.leaseTTL(),
		)
		if err != nil {
			return processed, err
		}
		if !ok {
			return processed, nil
		}
		jobCtx, stopLease := s.startDerivedTaskLease(ctx, job)
		version, err := s.runDerivedTask(jobCtx, job)
		if leaseErr := stopLease(); err == nil {
			err = leaseErr
		}
		if err != nil {
			_ = s.Coordinator.FailDerivedTask(ctx, job, err)
			return processed, err
		}
		if err := s.Coordinator.CompleteDerivedTask(ctx, job, version); err != nil {
			return processed, err
		}
		processed++
	}
}

func (s *TenantStore) runDerivedTask(ctx context.Context, job DerivedTaskJob) (int64, error) {
	switch job.TaskType {
	case derivedTaskIndexes:
		head, exists, err := s.Coordinator.Head(ctx, job.TenantID)
		if err != nil {
			return 0, err
		}
		if !exists || head.Status != TenantStatusActive {
			return max(job.TargetVersion, head.GraphVersion), nil
		}
		catalog, err := s.RebuildIndexes(ctx, job.TenantID)
		if err != nil {
			return 0, err
		}
		return catalog.Version, nil
	default:
		return 0, fmt.Errorf("unsupported derived task type %q", job.TaskType)
	}
}

func (s *TenantStore) SyncLegacyManifests(ctx context.Context) (int, error) {
	return s.syncLegacyManifests(ctx, 0)
}

func (s *TenantStore) syncLegacyManifests(ctx context.Context, limit int) (int, error) {
	if !s.coordinated() {
		return 0, nil
	}
	synced := 0
	for {
		if limit > 0 && synced >= limit {
			return synced, nil
		}
		ownerToken, err := newCommitID()
		if err != nil {
			return synced, err
		}
		job, ok, err := s.Coordinator.ClaimLegacyManifest(ctx, s.InstanceID+"-"+ownerToken, legacyManifestLeaseTTL)
		if err != nil {
			return synced, err
		}
		if !ok {
			return synced, nil
		}
		if err := s.syncLegacyManifestJob(ctx, job); err != nil {
			_ = s.Coordinator.FailLegacyManifest(ctx, job, err)
			return synced, err
		}
		if err := s.Coordinator.CompleteLegacyManifest(ctx, job); err != nil {
			return synced, err
		}
		synced++
	}
}

func (s *TenantStore) syncLegacyManifestJob(ctx context.Context, job LegacyManifestJob) error {
	if err := s.ensureLegacyManifestGeneration(ctx, job); err != nil {
		return err
	}
	data, err := s.Objects.Get(ctx, job.ManifestKey)
	if err != nil {
		return err
	}
	if hash := objectContentHash(data); hash != job.ManifestHash {
		return fmt.Errorf("legacy mirror manifest hash mismatch: got %s want %s", hash, job.ManifestHash)
	}
	key := s.manifestKey(job.TenantID)
	current, meta, err := s.Objects.GetWithMeta(ctx, key)
	condition := PutCondition{}
	switch {
	case errors.Is(err, ErrNotFound):
		condition.IfNoneMatch = true
	case err != nil:
		return err
	case bytes.Equal(current, data):
		return s.ensureLegacyManifestGeneration(ctx, job)
	default:
		if !isParquetBytes(current) {
			return fmt.Errorf("legacy manifest %q is not parquet", key)
		}
		manifest, decodeErr := decodeParquetManifest(ctx, current)
		if decodeErr != nil {
			return decodeErr
		}
		if manifest.Version > job.GraphVersion {
			return fmt.Errorf("%w: legacy manifest version %d is ahead of mirror job version %d", ErrConflict, manifest.Version, job.GraphVersion)
		}
		condition.IfMatch = meta.ETag
	}
	next, err := s.Objects.PutConditional(ctx, key, data, condition)
	if err != nil {
		return err
	}
	if err := s.ensureLegacyManifestGeneration(ctx, job); err != nil {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rollbackErr := s.Objects.DeleteConditional(
			rollbackCtx, key, PutCondition{IfMatch: next.ETag},
		)
		if rollbackErr != nil &&
			!errors.Is(rollbackErr, ErrConflict) &&
			!errors.Is(rollbackErr, ErrNotFound) {
			return errors.Join(err, fmt.Errorf("rollback stale legacy manifest %q: %w", key, rollbackErr))
		}
		return err
	}
	return nil
}

func (s *TenantStore) ensureLegacyManifestGeneration(
	ctx context.Context,
	job LegacyManifestJob,
) error {
	head, exists, err := s.Coordinator.Head(ctx, job.TenantID)
	if err != nil {
		return err
	}
	if !exists || head.Generation != job.Generation || head.Status == TenantStatusDeleted {
		return fmt.Errorf("%w: tenant %q generation changed while mirroring manifest", ErrConflict, job.TenantID)
	}
	return nil
}
