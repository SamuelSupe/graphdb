package main

import (
	"context"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

const cliTaskPollInterval = 100 * time.Millisecond

func startTask(args []string, store *storage.TenantStore) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("usage: graphdb start-task <tenant-id> <type> [params.json]")
	}
	var params map[string]any
	if len(args) == 3 {
		if err := readJSONFile(args[2], &params); err != nil {
			return err
		}
	}
	task, err := startAndWaitTask(
		context.Background(), store, args[0], args[1], params,
	)
	if err != nil {
		return err
	}
	return printFinishedTask(task)
}

func listTasks(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 3 {
		return fmt.Errorf("usage: graphdb list-tasks <tenant-id> [type] [status]")
	}
	options := storage.TaskListOptions{}
	if len(args) >= 2 {
		options.Type = args[1]
	}
	if len(args) == 3 {
		options.Status = args[2]
	}
	tasks, err := store.ListTasks(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"tasks": tasks})
}

func getTask(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb task <tenant-id> <task-id>")
	}
	task, err := store.GetTask(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	return printJSON(task)
}

func cancelTask(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb cancel-task <tenant-id> <task-id>")
	}
	task, err := store.CancelTask(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	return printJSON(task)
}

func retryTask(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb retry-task <tenant-id> <task-id>")
	}
	task, err := store.RetryTask(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	task, err = waitForTask(context.Background(), store, task)
	if err != nil {
		return err
	}
	return printFinishedTask(task)
}

func startAndWaitTask(
	ctx context.Context,
	store *storage.TenantStore,
	tenantID string,
	taskType string,
	params map[string]any,
) (storage.Task, error) {
	task, err := store.StartTask(ctx, tenantID, taskType, params)
	if err != nil {
		return storage.Task{}, err
	}
	return waitForTask(ctx, store, task)
}

func waitForTask(
	ctx context.Context,
	store *storage.TenantStore,
	task storage.Task,
) (storage.Task, error) {
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
			return storage.Task{}, ctx.Err()
		case <-ticker.C:
			var err error
			task, err = store.GetTask(ctx, task.TenantID, task.ID)
			if err != nil {
				return storage.Task{}, err
			}
		}
	}
}

func printFinishedTask(task storage.Task) error {
	if err := printJSON(task); err != nil {
		return err
	}
	if task.Status == storage.TaskStatusFailed ||
		task.Status == storage.TaskStatusCanceled {
		return fmt.Errorf("task %q %s: %s", task.ID, task.Status, task.Error)
	}
	return nil
}
