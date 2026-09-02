package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func indexCatalog(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb index-catalog <tenant-id>")
	}
	catalog, err := store.GetIndexCatalog(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(catalog)
}

func indexInspect(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb index-inspect <tenant-id>")
	}
	inspection, err := store.InspectIndex(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(inspection)
}

func indexDefinitions(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb index-definitions <tenant-id>")
	}
	definitions, err := store.ListIndexDefinitions(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"indexes": definitions})
}

func createIndex(args []string, store *storage.TenantStore) error {
	if len(args) < 3 || len(args) > 4 {
		return fmt.Errorf("usage: graphdb create-index <tenant-id> <kind> <field> [name]")
	}
	definition := storage.IndexDefinition{Kind: args[1], Field: args[2]}
	if len(args) == 4 {
		definition.Name = args[3]
	}
	ctx := context.Background()
	result, err := store.CreateIndex(ctx, args[0], definition)
	if err != nil {
		return err
	}
	result.Task, err = waitForIndexTask(ctx, store, result.Task)
	if err != nil {
		return err
	}
	return printFinishedIndexDefinitionResult(result)
}

func dropIndex(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb drop-index <tenant-id> <name>")
	}
	ctx := context.Background()
	result, err := store.DropIndex(ctx, args[0], args[1])
	if err != nil {
		return err
	}
	result.Task, err = waitForIndexTask(ctx, store, result.Task)
	if err != nil {
		return err
	}
	return printFinishedIndexDefinitionResult(result)
}

func waitForIndexTask(
	ctx context.Context,
	store *storage.TenantStore,
	task storage.IndexTask,
) (storage.IndexTask, error) {
	ticker := time.NewTicker(cliTaskPollInterval)
	defer ticker.Stop()
	for {
		switch task.Status {
		case storage.TaskStatusSucceeded,
			storage.TaskStatusFailed,
			storage.TaskStatusCanceled:
			return task, nil
		}
		select {
		case <-ctx.Done():
			return storage.IndexTask{}, ctx.Err()
		case <-ticker.C:
			var err error
			task, err = store.GetIndexTask(ctx, task.TenantID, task.ID)
			if err != nil {
				return storage.IndexTask{}, err
			}
		}
	}
}

func printFinishedIndexDefinitionResult(result storage.IndexDefinitionResult) error {
	if err := printJSON(result); err != nil {
		return err
	}
	if result.Task.Status == storage.TaskStatusFailed ||
		result.Task.Status == storage.TaskStatusCanceled {
		return fmt.Errorf(
			"index task %q %s: %s",
			result.Task.ID, result.Task.Status, result.Task.Error,
		)
	}
	return nil
}

func indexHealth(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb index-health <tenant-id>")
	}
	health, err := store.IndexHealth(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(health)
}

func integrityAudit(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("usage: graphdb integrity-audit <tenant-id> [--shallow]")
	}
	options := storage.IntegrityAuditOptions{Deep: true}
	if len(args) == 2 {
		if args[1] != "--shallow" {
			return fmt.Errorf("usage: graphdb integrity-audit <tenant-id> [--shallow]")
		}
		options.Deep = false
	}
	report, err := store.AuditIntegrity(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(report)
}

func rebuildIndexes(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb rebuild-indexes <tenant-id>")
	}
	catalog, err := store.RebuildIndexes(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(catalog)
}

func writerLease(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb writer-lease <tenant-id>")
	}
	lease, err := store.GetWriterLease(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(lease)
}

func recoverTenant(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb recover <tenant-id>")
	}
	report, err := store.RecoverTenant(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(report)
}

func repairTenant(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("usage: graphdb repair <tenant-id> [--apply]")
	}
	options := storage.RepairOptions{}
	if len(args) == 2 {
		if args[1] != "--apply" {
			return fmt.Errorf("usage: graphdb repair <tenant-id> [--apply]")
		}
		options.Apply = true
	}
	report, err := store.RepairTenant(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(report)
}

func cleanupCommits(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb cleanup-commits <tenant-id>")
	}
	report, err := store.CleanupCommits(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(report)
}

func runGC(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 3 {
		return fmt.Errorf("usage: graphdb gc <tenant-id> [deadletter-max-age-seconds] [task-max-age-seconds]")
	}
	options := storage.GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}
	if len(args) >= 2 {
		seconds, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || seconds < 0 {
			return fmt.Errorf("deadletter-max-age-seconds must be a non-negative integer")
		}
		options.DeadLetterMaxAge = time.Duration(seconds) * time.Second
	}
	if len(args) == 3 {
		seconds, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || seconds < 0 {
			return fmt.Errorf("task-max-age-seconds must be a non-negative integer")
		}
		options.TaskMaxAge = time.Duration(seconds) * time.Second
	}
	report, err := store.RunGC(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(report)
}
