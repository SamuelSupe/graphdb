package ha

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type maintenanceRetry struct {
	conflicts int
	delay     time.Duration
	next      time.Time
}

func (c *Cluster) RunBackground(ctx context.Context) {
	interval := c.FlushInterval
	if interval <= 0 {
		interval = time.Second
	}
	nextFlush := time.Now().Add(interval)
	ticker := time.NewTicker(min(interval, time.Second))
	defer ticker.Stop()

	for {
		tick := false
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick = true
		case <-c.flushWake:
		}
		operationCtx, cancel := context.WithTimeout(ctx, time.Minute)
		if c.Node.ReadBarrier(operationCtx) == nil {
			all := time.Now().After(nextFlush)
			_ = c.flushReady(operationCtx, all)
			if all {
				nextFlush = time.Now().Add(interval)
			}
			if tick && c.config.Protocol < 2 {
				_ = c.runQueuedTask(operationCtx)
			}
		}
		cancel()
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

// runScheduledMaintenance runs leader-scoped scheduled compaction and GC checks
// in HA mode by submitting idempotent replicated tasks through Raft.
func (c *Cluster) runScheduledMaintenance(ctx context.Context) {
	ticker := time.NewTicker(c.MaintenanceInterval)
	defer ticker.Stop()
	lastGC := make(map[string]time.Time)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(ctx, min(c.MaintenanceInterval, 2*time.Minute))
			if c.Node.ReadBarrier(opCtx) == nil {
				c.scheduleHAMaintenanceOnce(opCtx, time.Now().UTC(), lastGC)
			}
			cancel()
		}
	}
}

func (c *Cluster) scheduleHAMaintenanceOnce(ctx context.Context, now time.Time, lastGC map[string]time.Time) {
	if c.App.Handler == nil {
		return
	}
	c.App.mu.RLock()
	tenants, err := c.App.Store.ListManagedTenants(ctx)
	disk, diskErr := c.App.Store.DiskSpace(ctx)
	c.App.mu.RUnlock()
	if err != nil {
		return
	}
	inodePressure := diskErr == nil && disk.InodesKnown && disk.MinimumFreeInodes > 0 && disk.AvailableInodes/2 < disk.MinimumFreeInodes
	for _, tenant := range tenants {
		if ctx.Err() != nil {
			return
		}
		var (
			routeEpoch     uint64
			manifest       storage.Manifest
			cfg            storage.TenantConfig
			hasCompactTask bool
			hasGCTask      bool
			skip           bool
		)
		c.App.mu.RLock()
		if c.config.ShardID != "" {
			if _, gErr := c.App.partitionStore().GetGraph(ctx, tenant); gErr == nil {
				skip = true // Partitioned graphs have their own GC/compaction coordinator.
			} else if owner, oErr := c.App.ownership(ctx, tenant); oErr != nil || owner.Epoch == 0 || owner.State != "active" {
				skip = true
			} else {
				routeEpoch = owner.Epoch
			}
		}
		if !skip {
			if wErr := c.App.Store.EnsureTenantWritable(ctx, tenant); wErr != nil {
				skip = true
			}
		}
		if !skip {
			m, mErr := c.App.Store.CurrentManifest(ctx, tenant)
			if mErr != nil || m.Version == 0 {
				skip = true
			} else {
				manifest = m
			}
		}
		if !skip {
			loadedCfg, configured, cErr := c.App.Store.GetTenantConfig(ctx, tenant)
			if cErr != nil {
				skip = true
			} else if !configured {
				cfg = storage.DefaultTenantConfig()
			} else {
				cfg = storage.TenantConfigWithDefaults(loadedCfg)
			}
		}
		if !skip {
			if queued, qErr := c.App.Store.QueuedMaintenanceTasks(ctx, tenant); qErr == nil {
				for _, task := range queued {
					if task.Type == storage.TaskTypeCompact {
						hasCompactTask = true
					}
					if task.Type == storage.TaskTypeGC {
						hasGCTask = true
					}
				}
			}
		}
		c.App.mu.RUnlock()
		if skip {
			continue
		}
		autoCompact := cfg.Maintenance.AutoCompact == nil || *cfg.Maintenance.AutoCompact
		compactTail := 0
		if cfg.Maintenance.CompactCommitTailThreshold != nil {
			compactTail = *cfg.Maintenance.CompactCommitTailThreshold
		}
		if autoCompact && compactTail > 0 && !hasCompactTask && storage.ManifestCommitTailLength(manifest) >= compactTail {
			c.proposeScheduledTask(ctx, tenant, routeEpoch, storage.TaskTypeCompact, nil)
		}
		gcInterval := int64(0)
		if cfg.Maintenance.GCIntervalSeconds != nil {
			gcInterval = *cfg.Maintenance.GCIntervalSeconds
		}
		if gcInterval > 0 && !hasGCTask && (inodePressure || now.Sub(lastGC[tenant]) >= time.Duration(gcInterval)*time.Second) {
			keepSnapshots := 2
			if cfg.Maintenance.KeepSnapshots != nil && *cfg.Maintenance.KeepSnapshots >= 1 {
				keepSnapshots = *cfg.Maintenance.KeepSnapshots
			}
			if c.proposeScheduledTask(ctx, tenant, routeEpoch, storage.TaskTypeGC, map[string]any{
				"keep_snapshots":        keepSnapshots,
				"cleanup_index_orphans": true,
				"max_deletes":           4096,
			}) {
				lastGC[tenant] = now
			}
		}
	}
}

func (c *Cluster) proposeScheduledTask(ctx context.Context, tenant string, routeEpoch uint64, taskType string, params map[string]any) bool {
	payload := map[string]any{"type": taskType}
	if len(params) > 0 {
		payload["params"] = params
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	cmd, err := newCommand("http")
	if err != nil {
		return false
	}
	cmd.Method = http.MethodPost
	cmd.URI = "/v1/tasks"
	cmd.Header = http.Header{
		"Content-Type": []string{"application/json"},
		"X-Tenant-ID":  []string{tenant},
	}
	cmd.Body = body
	cmd.Tenant = tenant
	cmd.RouteEpoch = routeEpoch
	data, err := c.propose(ctx, cmd)
	if err != nil {
		return false
	}
	var res httpResult
	if err := json.Unmarshal(data, &res); err != nil {
		return false
	}
	return res.Status >= 200 && res.Status < 400
}

func (c *Cluster) runQueuedTask(ctx context.Context) (err error) {
	c.maintenanceMu.Lock()
	defer c.maintenanceMu.Unlock()
	finish := c.metrics.Start("maintenance_poll")
	defer func() { finish(err) }()
	tenants, err := c.App.Store.ListManagedTenants(ctx)
	if err != nil {
		return err
	}
	var candidates []storage.Task
	for _, tenant := range tenants {
		tasks, err := c.App.Store.QueuedMaintenanceTasks(ctx, tenant)
		if err != nil {
			return err
		}
		candidates = append(candidates, tasks...)
	}
	key := func(task storage.Task) string { return task.TenantID + "/" + task.ID }
	pending := make(map[string]bool, len(candidates))
	for _, task := range candidates {
		pending[key(task)] = true
	}
	for id := range c.maintenanceRetries {
		if !pending[id] {
			delete(c.maintenanceRetries, id)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return key(candidates[i]) < key(candidates[j]) })
	start := sort.Search(len(candidates), func(i int) bool { return key(candidates[i]) > c.maintenanceCursor })
	var queued *storage.Task
	var admissionErr error
	now := time.Now()
	for offset := range len(candidates) {
		candidate := &candidates[(start+offset)%len(candidates)]
		if now.Before(c.maintenanceRetries[key(*candidate)].next) {
			continue
		}
		if err := c.App.Store.CheckTaskDiskSpace(ctx, *candidate); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			admissionErr = err
			c.metrics.Event("maintenance_deferred")
			continue
		}
		queued = candidate
		// Move past attempts as well as successes, so a retrying task cannot
		// monopolize the worker or prevent a space-releasing GC from running.
		c.maintenanceCursor = key(*queued)
		break
	}
	var restore []byte
	var generation int64
	var staged restoreManifest
	ready := false
	capture := false
	if queued != nil {
		c.metrics.Event("maintenance_selected")
	}
	prepared := queued != nil && c.config.Protocol >= 2 && storage.PreparedMaintenanceTask(*queued) && (queued.Type != storage.TaskTypeGC || c.config.Protocol >= 3)
	if prepared {
		id := key(*queued)
		retry := c.maintenanceRetries[id]
		if retry.conflicts >= 3 {
			operation, resume, err := c.pauseMaintenance(ctx, *queued)
			if err != nil {
				return err
			}
			defer resume()
			ctx = operation
		}
		c.App.mu.RLock()
		snapshotBudget := c.App.MaxSnapshotBytes
		generation, err := c.App.Store.ReplicationTenantGeneration(ctx, queued.TenantID)
		generationKnown := err == nil
		var source *storage.ReplicatedMaintenanceSource
		if err == nil {
			finish := c.metrics.Start("maintenance_capture")
			source, err = c.App.Store.CaptureReplicatedMaintenance(ctx, *queued)
			finish(err)
		}
		c.App.mu.RUnlock()
		if err == nil {
			defer source.Close()
			finish := c.metrics.Start("maintenance_prepare")
			input, buildErr := source.Build(ctx, snapshotBudget)
			finish(buildErr)
			err = buildErr
			if err == nil {
				defer input.Close()
				err = c.replicateMaintenance(ctx, *queued, input, generation)
			}
		}
		if generationKnown && permanentTaskPreparationError(err) {
			cmd, commandErr := newCommand("task")
			if commandErr != nil {
				return commandErr
			}
			cmd.Tenant, cmd.IDs, cmd.ExpectedGeneration = queued.TenantID, []string{queued.ID}, generation
			cmd.Error = err.Error()
			_, err = c.propose(ctx, cmd)
		}
		if err == nil {
			delete(c.maintenanceRetries, id)
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if c.maintenanceRetries == nil {
			c.maintenanceRetries = make(map[string]maintenanceRetry)
		}
		if errors.Is(err, storage.ErrConflict) {
			retry.conflicts++
			c.metrics.Event("maintenance_prepare_conflict")
		} else {
			retry.delay = min(30*time.Second, max(time.Second, 2*retry.delay))
			retry.next = time.Now().Add(retry.delay)
		}
		c.maintenanceRetries[id] = retry
		return err
	}
	var prep *storage.ReplicatedTaskPreparation
	c.App.mu.RLock()
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
		finish := c.metrics.Start("maintenance_capture")
		prep, err = c.App.Store.CaptureReplicatedTaskPreparation(ctx, *queued)
		finish(err)
	}
	c.App.mu.RUnlock()
	if prep != nil {
		defer prep.Close()
		finish := c.metrics.Start("maintenance_prepare")
		restore, err = prep.Build(ctx)
		finish(err)
	}
	if queued == nil {
		return admissionErr
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
	if prepareErr != nil && !permanentTaskPreparationError(prepareErr) {
		// Preparation has no replicated effects. An opaque transport wrapper
		// is not evidence of permanent failure; leave it queued for a fair retry.
		c.metrics.Event("maintenance_prepare_deferred")
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

func permanentTaskPreparationError(err error) bool {
	if errors.Is(err, storage.ErrReplicationSnapshotTooLarge) || errors.Is(err, storage.ErrInvalidArgument) ||
		errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrTenantDisabled) || errors.Is(err, storage.ErrTenantDeleted) {
		return true
	}
	var responseError interface{ HTTPStatusCode() int }
	if errors.As(err, &responseError) {
		status := responseError.HTTPStatusCode()
		return status >= 400 && status < 500 && status != 408 && status != 409 && status != 429
	}
	return false
}
