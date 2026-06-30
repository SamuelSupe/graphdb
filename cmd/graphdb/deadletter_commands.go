package main

import (
	"context"
	"fmt"
	"strconv"

	"graphdb/internal/storage"
)

func deadLetters(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb deadletters <tenant-id> <source>")
	}
	items, err := store.ListDeadLetters(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"deadletters": items})
}

func replayDeadLetters(args []string, store *storage.TenantStore) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("usage: graphdb replay-deadletters <tenant-id> <source> [limit]")
	}
	limit := 0
	if len(args) == 3 {
		value, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		if value < 0 {
			return fmt.Errorf("limit must be a non-negative integer")
		}
		limit = value
	}
	report, err := store.ReplayDeadLetters(context.Background(), args[0], args[1], limit)
	if err != nil {
		return err
	}
	return printJSON(report)
}
