package main

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func listTenants(args []string, store *storage.TenantStore) error {
	if len(args) > 1 || (len(args) == 1 && args[0] != "--include-legacy") {
		return fmt.Errorf("usage: graphdb list-tenants [--include-legacy]")
	}
	var (
		tenants []storage.TenantInfo
		err     error
	)
	if len(args) == 1 {
		tenants, err = store.ListTenantInfosIncludingLegacy(context.Background())
	} else {
		tenants, err = store.ListManagedTenantInfos(context.Background())
	}
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"tenants": tenants})
}

func tenantInfo(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb tenant <tenant-id>")
	}
	info, err := store.GetTenantInfo(context.Background(), args[0])
	if err != nil {
		return err
	}
	return printJSON(info)
}

func createTenant(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("usage: graphdb create-tenant <tenant-id> [metadata.json]")
	}
	options, err := tenantCreateOptions(args)
	if err != nil {
		return err
	}
	info, err := store.CreateTenant(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(info)
}

func setTenantMetadata(args []string, store *storage.TenantStore) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: graphdb set-tenant-metadata <tenant-id> <metadata.json>")
	}
	var options storage.TenantCreateOptions
	if err := readJSONFile(args[1], &options); err != nil {
		return err
	}
	info, err := store.UpdateTenantMetadata(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(info)
}

func disableTenant(args []string, store *storage.TenantStore) error {
	return setTenantStatusCLI(args, store, storage.TenantStatusDisabled, "disable-tenant")
}

func enableTenant(args []string, store *storage.TenantStore) error {
	return setTenantStatusCLI(args, store, storage.TenantStatusActive, "enable-tenant")
}

func deleteTenant(args []string, store *storage.TenantStore) error {
	return setTenantStatusCLI(args, store, storage.TenantStatusDeleted, "delete-tenant")
}

func purgeTenant(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("usage: graphdb purge-tenant <tenant-id> [--force]")
	}
	force := len(args) == 2 && args[1] == "--force"
	if len(args) == 2 && !force {
		return fmt.Errorf("usage: graphdb purge-tenant <tenant-id> [--force]")
	}
	report, err := store.PurgeTenant(context.Background(), args[0], force)
	if err != nil {
		return err
	}
	return printJSON(report)
}

func cloneTenant(args []string, store *storage.TenantStore) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("usage: graphdb clone-tenant <source-tenant-id> <target-tenant-id> [metadata.json]")
	}
	options := storage.TenantCloneOptions{TargetTenantID: args[1]}
	if len(args) == 3 {
		if err := readJSONFile(args[2], &options); err != nil {
			return err
		}
		options.TargetTenantID = args[1]
	}
	info, err := store.CloneTenant(context.Background(), args[0], options)
	if err != nil {
		return err
	}
	return printJSON(info)
}

func backupTenant(args []string, store *storage.TenantStore) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb backup-tenant <tenant-id>")
	}
	task, err := startAndWaitTask(
		context.Background(), store, args[0], storage.TaskTypeTenantBackup, nil,
	)
	if err != nil {
		return err
	}
	return printFinishedTask(task)
}

func restoreTenant(args []string, store *storage.TenantStore) error {
	if len(args) < 2 || len(args) > 4 {
		return fmt.Errorf("usage: graphdb restore-tenant <tenant-id> <backup-key> [--overwrite] [--dry-run]")
	}
	overwrite := false
	dryRun := false
	for _, arg := range args[2:] {
		switch arg {
		case "--overwrite":
			overwrite = true
		case "--dry-run":
			dryRun = true
		default:
			return fmt.Errorf("usage: graphdb restore-tenant <tenant-id> <backup-key> [--overwrite] [--dry-run]")
		}
	}
	task, err := startAndWaitTask(context.Background(), store, args[0], storage.TaskTypeTenantRestore, map[string]any{
		"backup_key": args[1],
		"overwrite":  overwrite,
		"dry_run":    dryRun,
	})
	if err != nil {
		return err
	}
	return printFinishedTask(task)
}

func restoreDrillTenant(args []string, store *storage.TenantStore) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("usage: graphdb restore-drill-tenant <tenant-id> [params.json]")
	}
	var params map[string]any
	if len(args) == 2 {
		if err := readJSONFile(args[1], &params); err != nil {
			return err
		}
	}
	task, err := startAndWaitTask(
		context.Background(), store, args[0], storage.TaskTypeTenantRestoreDrill, params,
	)
	if err != nil {
		return err
	}
	return printFinishedTask(task)
}

func setTenantStatusCLI(args []string, store *storage.TenantStore, status string, command string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb %s <tenant-id>", command)
	}
	info, err := store.SetTenantStatus(context.Background(), args[0], status)
	if err != nil {
		return err
	}
	return printJSON(info)
}

func tenantCreateOptions(args []string) (storage.TenantCreateOptions, error) {
	if len(args) != 2 {
		return storage.TenantCreateOptions{}, nil
	}
	var options storage.TenantCreateOptions
	if err := readJSONFile(args[1], &options); err != nil {
		return storage.TenantCreateOptions{}, err
	}
	return options, nil
}
