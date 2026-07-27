package storage

import (
	"context"
	"strings"
)

const coordinatorQueuedTaskLeasePrefix = "task_queue/"

func coordinatorQueuedTaskLeaseType(task Task) string {
	return coordinatorQueuedTaskLeasePrefix + task.Type
}

func (s *TenantStore) claimCoordinatorQueuedTask(
	ctx context.Context,
	task Task,
	cancel context.CancelFunc,
) (func(), Task, bool, error) {
	var active Task
	stop, reused, err := s.claimCoordinatorLease(
		ctx,
		task.TenantID,
		coordinatorQueuedTaskLeaseType(task),
		s.InstanceID+"/"+task.ID,
		cancel,
		func(findCtx context.Context) bool {
			var ok bool
			active, ok = s.findCoordinatorQueuedTask(findCtx, task)
			return ok
		},
	)
	return stop, active, reused, err
}

func (s *TenantStore) findCoordinatorQueuedTask(
	ctx context.Context,
	requested Task,
) (Task, bool) {
	reader, ok := s.Coordinator.(CoordinatorTaskLeaseReader)
	if !ok {
		return Task{}, false
	}
	lease, active, err := reader.TaskLease(
		ctx,
		requested.TenantID,
		coordinatorQueuedTaskLeaseType(requested),
	)
	if err != nil || !active {
		return Task{}, false
	}
	taskID := taskIDFromCoordinatorLeaseOwner(lease.OwnerToken)
	if taskID == "" {
		return Task{}, false
	}
	task, err := s.GetTask(ctx, requested.TenantID, taskID)
	if err != nil ||
		task.Type != requested.Type ||
		!taskStillActive(task) ||
		lease.OwnerToken != task.OwnerID+"/"+task.ID {
		return Task{}, false
	}
	return task, true
}

func taskIDFromCoordinatorLeaseOwner(owner string) string {
	index := strings.LastIndexByte(owner, '/')
	if index < 0 || index+1 >= len(owner) {
		return ""
	}
	return owner[index+1:]
}
