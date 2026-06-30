package main

import (
	"context"
	"fmt"

	"graphdb/internal/graph"
	"graphdb/internal/httpapi"
	"graphdb/internal/storage"
)

func initTenant(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb init-tenant <tenant-id>")
	}
	manifest, err := store.InitTenant(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(manifest)
}

func commit(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb commit <tenant-id> <commit.json>")
	}
	var request httpapi.CommitRequest
	if err := readJSONFile(args[1], &request); err != nil {
		return err
	}
	result, err := store.CommitWithReport(context.Background(), args[0], request.Mutations, storage.CommitOptions{ExpectedVersion: request.ExpectedVersion, IdempotencyKey: request.IdempotencyKey})
	if err != nil {
		return err
	}
	return printJSON(result)
}

func ingest(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb ingest <tenant-id> <ingest.json>")
	}
	var request storage.IngestRequest
	if err := readJSONFile(args[1], &request); err != nil {
		return err
	}
	result, err := store.Ingest(context.Background(), args[0], request)
	if err != nil {
		return err
	}
	return printJSON(result)
}

func collectorStatus(args []string, store *storage.TenantStore) error {
	if len(args) != 3 {
		return fmt.Errorf("usage: graphdb collector-status <tenant-id> <source> <collector-id>")
	}
	status, err := store.GetCollectorStatus(context.Background(), args[0], args[1], args[2])
	if err != nil {
		return err
	}
	return printJSON(status)
}

func sourcePolicy(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb source-policy <tenant-id>")
	}
	policy, configured, err := store.GetSourcePolicy(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": configured, "policy": policy})
}

func setSourcePolicy(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb set-source-policy <tenant-id> <policy.json>")
	}
	var policy graph.SourcePolicy
	if err := readJSONFile(args[1], &policy); err != nil {
		return err
	}
	policy, err := store.PutSourcePolicy(context.Background(), args[0], policy)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": true, "policy": policy})
}

func tenantConfig(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb tenant-config <tenant-id>")
	}
	config, configured, err := store.GetTenantConfig(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": configured, "config": config})
}

func setTenantConfig(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb set-tenant-config <tenant-id> <config.json>")
	}
	var config storage.TenantConfig
	if err := readJSONFile(args[1], &config); err != nil {
		return err
	}
	config, err := store.PutTenantConfig(context.Background(), args[0], config)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": true, "config": config})
}

func tenantUsage(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb tenant-usage <tenant-id>")
	}
	report, err := store.TenantUsage(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(report)
}

func compact(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb compact <tenant-id>")
	}
	manifest, err := store.Compact(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(manifest)
}
