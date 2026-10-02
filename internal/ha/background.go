package ha

import (
	"context"
	"errors"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (c *Cluster) RunBackground(ctx context.Context) {
	interval := c.FlushInterval
	if interval <= 0 {
		interval = time.Second
	}
	nextFlush := time.Now().Add(interval)
	ticker := time.NewTicker(min(interval, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operationCtx, cancel := context.WithTimeout(ctx, time.Minute)
			if c.Node.ReadBarrier(operationCtx) == nil {
				if time.Now().After(nextFlush) {
					_ = c.flushPending(operationCtx)
					nextFlush = time.Now().Add(interval)
				}
				if c.config.Protocol < 2 {
					_ = c.runQueuedTask(operationCtx)
				}
			}
			cancel()
		}
	}
}

func (c *Cluster) runTaskBackground(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operation, cancel := context.WithTimeout(ctx, 10*time.Minute)
			if c.Node.ReadBarrier(operation) == nil {
				_ = c.runQueuedTask(operation)
			}
			cancel()
		}
	}
}

func (c *Cluster) runQueuedTask(ctx context.Context) (err error) {
	finish := c.metrics.Start("maintenance_poll")
	defer func() { finish(err) }()
	c.App.mu.RLock()
	tenants, err := c.App.Store.ListManagedTenants(ctx)
	if err != nil {
		c.App.mu.RUnlock()
		return err
	}
	var queued *storage.Task
	for _, tenant := range tenants {
		tasks, err := c.App.Store.ListTasks(ctx, tenant, storage.TaskListOptions{Status: storage.TaskStatusQueued})
		if err != nil {
			c.App.mu.RUnlock()
			return err
		}
		if len(tasks) > 0 {
			queued = &tasks[0]
			break
		}
	}
	var restore []byte
	var generation int64
	var staged restoreManifest
	ready := false
	capture := false
	if queued != nil {
		c.metrics.Event("maintenance_selected")
		if err := c.App.Store.CheckTaskDiskSpace(ctx, *queued); err != nil {
			c.App.mu.RUnlock()
			return err
		}
	}
	if queued != nil && c.config.Protocol >= 2 && storage.PreparedMaintenanceTask(*queued) {
		generation, err := c.App.Store.ReplicationTenantGeneration(ctx, queued.TenantID)
		var source *storage.ReplicatedMaintenanceSource
		if err == nil {
			finish := c.metrics.Start("maintenance_capture")
			source, err = c.App.Store.CaptureReplicatedMaintenance(ctx, *queued)
			finish(err)
		}
		c.App.mu.RUnlock()
		if err != nil {
			return err
		}
		defer source.Close()
		finish := c.metrics.Start("maintenance_prepare")
		input, err := source.Build(ctx, c.App.MaxSnapshotBytes)
		finish(err)
		if err != nil {
			return err
		}
		defer input.Close()
		return c.replicateMaintenance(ctx, *queued, input, generation)
	}
	if queued != nil && restoreTask(*queued) {
		generation, err = c.App.Store.ReplicationTenantGeneration(ctx, queued.TenantID)
		if err == nil {
			staged, ready, err = c.App.stagedRestoreReady(ctx, *queued)
		}
	}
	if queued != nil && queued.Type == storage.TaskTypeTenantBackup && queued.Params["destination"] == "object" {
		captured, captureErr := c.App.Store.ReplicatedObjectBackupCaptured(ctx, *queued)
		if captureErr != nil {
			c.App.mu.RUnlock()
			return captureErr
		}
		capture = !captured
	}
	if queued != nil && !capture && !ready && err == nil {
		finish := c.metrics.Start("maintenance_prepare")
		restore, err = c.App.Store.PrepareReplicatedTask(ctx, *queued)
		finish(err)
	}
	c.App.mu.RUnlock()
	if queued == nil {
		return err
	}
	if err == nil && restoreTask(*queued) {
		if ready {
			return c.publishRestore(ctx, *queued, staged)
		}
		return c.replicateRestore(ctx, *queued, restore, generation)
	}
	prepareErr := err
	var pressure *storage.BackpressureError
	if errors.As(prepareErr, &pressure) {
		return prepareErr
	}
	cmd, commandErr := newCommand("task")
	if commandErr != nil {
		return commandErr
	}
	if capture {
		cmd.Kind = "capture_backup"
	}
	cmd.Tenant = queued.TenantID
	cmd.IDs = []string{queued.ID}
	cmd.Restore = restore
	cmd.ExpectedGeneration = generation
	if prepareErr != nil {
		cmd.Error = prepareErr.Error()
	}
	_, err = c.propose(ctx, cmd)
	return err
}
