package main

import (
	"context"
	"fmt"
	"os"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func runQuery(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb query <tenant-id> <query.json>")
	}
	var request query.Request
	if err := readJSONFile(args[1], &request); err != nil {
		return err
	}
	return executeQuery(store, args[0], request)
}

func runGQL(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb gql <tenant-id> <query.gql>")
	}
	data, err := os.ReadFile(args[1])
	if err != nil {
		return err
	}
	request, err := query.ParseGQL(string(data))
	if err != nil {
		return err
	}
	return executeQuery(store, args[0], request)
}

func saveQuery(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb save-query <tenant-id> <saved-query.json>")
	}
	var saved storage.SavedQuery
	if err := readJSONFile(args[1], &saved); err != nil {
		return err
	}
	saved, err := store.SaveQuery(context.Background(), args[0], saved)
	if err != nil {
		return err
	}
	return printJSON(saved)
}

func listQueries(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb list-queries <tenant-id>")
	}
	queries, err := store.ListSavedQueries(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"queries": queries})
}

func runSavedQuery(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb run-saved-query <tenant-id> <name>")
	}
	saved, err := store.GetSavedQuery(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	return executeQuery(store, args[0], saved.Request)
}

func executeQuery(store *storage.TenantStore, tenantID string, request query.Request) error {
	g, _, err := store.Load(context.Background(), tenantID)
	if err != nil {
		return err
	}
	response, err := query.Execute(g, request)
	if err != nil {
		return err
	}
	return printJSON(response)
}
