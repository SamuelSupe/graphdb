package ha

import (
	"context"
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
				_ = c.runQueuedTask(operationCtx)
			}
			cancel()
		}
	}
}

func (c *Cluster) runQueuedTask(ctx context.Context) error {
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
		restore, err = c.App.Store.PrepareReplicatedTask(ctx, *queued)
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
