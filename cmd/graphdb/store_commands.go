package main

import (
	"context"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func initTenant(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb init-tenant <tenant-id>")
	}
	manifest, err := store.InitTenant(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(manifest)
}

func commit(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb commit <tenant-id> <commit.json>")
	}
	var request httpapi.CommitRequest
	if err := readJSONFile(args[1], &request); err != nil {
		return err
	}
	result, err := store.CommitWithReport(ctx, args[0], request.Mutations, storage.CommitOptions{ExpectedVersion: request.ExpectedVersion, IdempotencyKey: request.IdempotencyKey})
	if err != nil {
		return err
	}
	return printJSON(result)
}

func ingest(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb ingest <tenant-id> <ingest.json>")
	}
	var request storage.IngestRequest
	if err := readJSONFile(args[1], &request); err != nil {
		return err
	}
	result, err := store.Ingest(ctx, args[0], request)
	if err != nil {
		return err
	}
	return printJSON(result)
}

func collectorStatus(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 3 {
		return fmt.Errorf("usage: graphdb collector-status <tenant-id> <source> <collector-id>")
	}
	status, err := store.GetCollectorStatus(ctx, args[0], args[1], args[2])
	if err != nil {
		return err
	}
	return printJSON(status)
}

func sourcePolicy(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb source-policy <tenant-id>")
	}
	policy, configured, err := store.GetSourcePolicy(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": configured, "policy": policy})
}

func setSourcePolicy(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb set-source-policy <tenant-id> <policy.json>")
	}
	var policy graph.SourcePolicy
	if err := readJSONFile(args[1], &policy); err != nil {
		return err
	}
	policy, err := store.PutSourcePolicy(ctx, args[0], policy)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": true, "policy": policy})
}

func tenantConfig(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb tenant-config <tenant-id>")
	}
	config, configured, err := store.GetTenantConfig(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": configured, "config": config})
}

func setTenantConfig(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb set-tenant-config <tenant-id> <config.json>")
	}
	var config storage.TenantConfig
	if err := readJSONFile(args[1], &config); err != nil {
		return err
	}
	config, err := store.PutTenantConfig(ctx, args[0], config)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"configured": true, "config": config})
}

func tenantUsage(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb tenant-usage <tenant-id>")
	}
	report, err := store.TenantUsage(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(report)
}

func compact(ctx context.Context, args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb compact <tenant-id>")
	}
	manifest, err := store.Compact(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(manifest)
}
