package main

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type coordinatorMigrator interface {
	Migrate(context.Context) error
}

func coordinatorCommand(args []string, store *storage.TenantStore, coordinator storage.WriteCoordinator) error {
	if coordinator == nil {
		return fmt.Errorf("GRAPHDB_COORDINATION=postgres is required")
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: graphdb coordinator migrate|bootstrap|status|sync-legacy-manifest|rollback")
	}
	ctx := context.Background()
	switch args[0] {
	case "migrate":
		if len(args) != 1 {
			return fmt.Errorf("usage: graphdb coordinator migrate")
		}
		migrator, ok := coordinator.(coordinatorMigrator)
		if !ok {
			return fmt.Errorf("coordinator backend %q does not support migrations", coordinator.Backend())
		}
		if err := migrator.Migrate(ctx); err != nil {
			return err
		}
		return printJSON(map[string]any{
			"backend": coordinator.Backend(), "namespace": coordinator.Namespace(), "migrated": true,
		})
	case "bootstrap":
		if len(args) != 2 || (args[1] != "--dry-run" && args[1] != "--apply") {
			return fmt.Errorf("usage: graphdb coordinator bootstrap --dry-run|--apply")
		}
		if err := coordinator.CheckSchema(ctx); err != nil {
			return err
		}
		report, err := store.BootstrapCoordinator(ctx, coordinator, args[1] == "--dry-run")
		if err != nil {
			return err
		}
		return printJSON(report)
	case "status":
		if len(args) != 1 {
			return fmt.Errorf("usage: graphdb coordinator status")
		}
		if err := coordinator.CheckSchema(ctx); err != nil {
			return err
		}
		status, err := coordinator.Status(ctx)
		if err != nil {
			return err
		}
		return printJSON(status)
	case "sync-legacy-manifest":
		if len(args) != 1 {
			return fmt.Errorf("usage: graphdb coordinator sync-legacy-manifest")
		}
		if err := coordinator.CheckSchema(ctx); err != nil {
			return err
		}
		store.SetCoordinator(coordinator)
		if err := store.EnsurePostgresMarker(ctx); err != nil {
			return err
		}
		synced, err := store.SyncLegacyManifests(ctx)
		if err != nil {
			return err
		}
		return printJSON(map[string]any{"synced": synced})
	case "rollback":
		dryRun, err := parseCoordinatorRollbackArgs(args[1:])
		if err != nil {
			return err
		}
		if err := coordinator.CheckSchema(ctx); err != nil {
			return err
		}
		report, err := store.RollbackCoordinator(ctx, coordinator, dryRun)
		if err != nil {
			return err
		}
		return printJSON(report)
	default:
		return fmt.Errorf("unknown coordinator command %q", args[0])
	}
}

func parseCoordinatorRollbackArgs(args []string) (bool, error) {
	if len(args) == 1 && args[0] == "--dry-run" {
		return true, nil
	}
	if len(args) == 2 && args[0] == "--apply" && args[1] == "--writers-stopped" {
		return false, nil
	}
	return false, fmt.Errorf(
		"usage: graphdb coordinator rollback --dry-run | --apply --writers-stopped",
	)
}
