package storage

import (
	"context"
	"time"
)

func coordinatorQueuedIndexTaskLeaseType() string {
	return coordinatorQueuedTaskLeasePrefix + TaskTypeIndexRebuild
}

func (s *TenantStore) claimCoordinatorQueuedIndexTask(
	ctx context.Context,
	task IndexTask,
	cancel context.CancelFunc,
) (func(), IndexTask, bool, error) {
	var active IndexTask
	stop, reused, err := s.claimCoordinatorLease(
		ctx,
		task.TenantID,
		coordinatorQueuedIndexTaskLeaseType(),
		s.InstanceID+"/"+task.ID,
		cancel,
		func(findCtx context.Context) bool {
			var ok bool
			active, ok = s.findCoordinatorQueuedIndexTask(findCtx, task)
			return ok
		},
	)
	return stop, active, reused, err
}

func (s *TenantStore) publishQueuedIndexTask(
	ctx context.Context,
	task IndexTask,
) error {
	data, err := marshalParquetIndexTask(ctx, task)
	if err != nil {
		return err
	}
	if err := s.putTenantGenerationObject(
		ctx,
		task.TenantID,
		s.indexRebuildRunningTaskKey(task.TenantID),
		data,
	); err != nil {
		return err
	}
	if err := s.putTenantGenerationObject(
		ctx,
		task.TenantID,
		s.indexTaskKey(task.TenantID, task.ID),
		data,
	); err != nil {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			5*time.Second,
		)
		defer cancel()
		_ = s.clearIndexRebuildRunningMarker(
			cleanupCtx,
			task.TenantID,
			task.ID,
		)
		return err
	}
	return nil
}

func (s *TenantStore) findCoordinatorQueuedIndexTask(
	ctx context.Context,
	requested IndexTask,
) (IndexTask, bool) {
	reader, ok := s.Coordinator.(CoordinatorTaskLeaseReader)
	if !ok {
		return IndexTask{}, false
	}
	lease, active, err := reader.TaskLease(
		ctx,
		requested.TenantID,
		coordinatorQueuedIndexTaskLeaseType(),
	)
	if err != nil || !active {
		return IndexTask{}, false
	}
	taskID := taskIDFromCoordinatorLeaseOwner(lease.OwnerToken)
	if taskID == "" {
		return IndexTask{}, false
	}
	task, err := s.GetIndexTask(ctx, requested.TenantID, taskID)
	if err != nil ||
		task.Type != "rebuild" ||
		!indexTaskStillActive(task) ||
		lease.OwnerToken != task.OwnerID+"/"+task.ID {
		return IndexTask{}, false
	}
	marker, err := s.getIndexRebuildRunningMarker(
		ctx,
		requested.TenantID,
	)
	if err != nil ||
		marker.ID != task.ID ||
		marker.OwnerID != task.OwnerID ||
		!indexTaskStillActive(marker) {
		return IndexTask{}, false
	}
	return task, true
}
