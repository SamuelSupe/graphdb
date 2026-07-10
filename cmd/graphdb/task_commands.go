package main

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

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
	task, err := store.StartTask(context.Background(), args[0], args[1], params)
	if err != nil {
		return err
	}
	return printJSON(task)
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
	return printJSON(task)
}
