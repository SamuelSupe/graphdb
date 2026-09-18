package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type storeFlags struct {
	kind    string
	prefix  string
	dataDir string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var source storeFlags
	var target storeFlags
	tenantID := flag.String("tenant", "", "source tenant id")
	targetTenantID := flag.String("target-tenant", "", "target tenant id; must match -tenant for byte-copy migration")
	dryRun := flag.Bool("dry-run", false, "report planned copy without writing target objects")
	overwrite := flag.Bool("overwrite", false, "delete existing target tenant prefix before copying")

	addStoreFlags("source", &source)
	addStoreFlags("target", &target)
	flag.Parse()

	if strings.TrimSpace(*tenantID) == "" {
		return fmt.Errorf("-tenant is required")
	}
	sourceStore, err := openStore(source)
	if err != nil {
		return fmt.Errorf("source store: %w", err)
	}
	defer sourceStore.Close()
	sourceRoot, _ := filepath.Abs(source.dataDir)
	targetRoot, _ := filepath.Abs(target.dataDir)
	targetStore := sourceStore
	if sourceRoot != targetRoot || target.kind != "local" {
		targetStore, err = openStore(target)
		if err != nil {
			return fmt.Errorf("target store: %w", err)
		}
		defer targetStore.Close()
	}
	sourceTenant := storage.NewTenantStore(sourceStore, source.prefix)
	targetTenant := storage.NewTenantStore(targetStore, target.prefix)
	for _, store := range []*storage.TenantStore{sourceTenant, targetTenant} {
		if err := store.EnsureLocalWriterAllowed(context.Background()); err != nil {
			return err
		}
	}
	targetID := strings.TrimSpace(*targetTenantID)
	if targetID == "" {
		targetID = strings.TrimSpace(*tenantID)
	}
	report, err := storage.CopyTenantObjects(
		context.Background(),
		sourceTenant,
		strings.TrimSpace(*tenantID),
		targetTenant,
		targetID,
		storage.TenantMigrationOptions{DryRun: *dryRun, Overwrite: *overwrite},
	)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func addStoreFlags(prefix string, target *storeFlags) {
	flag.StringVar(&target.kind, prefix+"-storage", "local", "storage kind: local")
	flag.StringVar(&target.prefix, prefix+"-prefix", "graphdb", "GGraphDB object prefix")
	flag.StringVar(&target.dataDir, prefix+"-data-dir", ".graphdb", "local data directory")
}

func openStore(cfg storeFlags) (*storage.FileStore, error) {
	switch strings.TrimSpace(cfg.kind) {
	case "local":
		return storage.OpenFileStore(cfg.dataDir)
	default:
		return nil, fmt.Errorf("unsupported storage kind %q", cfg.kind)
	}
}
