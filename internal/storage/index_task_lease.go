package storage

import "context"

func (s *TenantStore) startIndexRebuildTaskLease(
	ctx context.Context,
	task IndexTask,
) (context.Context, func(), error) {
	if !s.coordinated() {
		return ctx, func() {}, nil
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	boundCtx, stop, err := s.startCoordinatorTaskLease(
		leaseCtx,
		Task{
			ID:       task.ID,
			TenantID: task.TenantID,
			Type:     TaskTypeIndexRebuild,
			OwnerID:  task.OwnerID,
		},
		cancel,
	)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return boundCtx, func() {
		stop()
		cancel()
	}, nil
}
