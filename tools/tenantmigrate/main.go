package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type storeFlags struct {
	kind      string
	prefix    string
	dataDir   string
	endpoint  string
	bucket    string
	region    string
	accessKey string
	secretKey string
	pathStyle bool
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
	targetStore, err := openStore(target)
	if err != nil {
		return fmt.Errorf("target store: %w", err)
	}
	targetID := strings.TrimSpace(*targetTenantID)
	if targetID == "" {
		targetID = strings.TrimSpace(*tenantID)
	}
	report, err := storage.CopyTenantObjects(
		context.Background(),
		storage.NewTenantStore(sourceStore, source.prefix),
		strings.TrimSpace(*tenantID),
		storage.NewTenantStore(targetStore, target.prefix),
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
	flag.StringVar(&target.kind, prefix+"-storage", "local", "object store kind: local or s3")
	flag.StringVar(&target.prefix, prefix+"-prefix", "graphdb", "GGraphDB object prefix")
	flag.StringVar(&target.dataDir, prefix+"-data-dir", ".graphdb", "local object store directory")
	flag.StringVar(&target.endpoint, prefix+"-s3-endpoint", "", "S3 endpoint URL")
	flag.StringVar(&target.bucket, prefix+"-s3-bucket", "", "S3 bucket")
	flag.StringVar(&target.region, prefix+"-s3-region", "us-east-1", "S3 region")
	flag.StringVar(&target.accessKey, prefix+"-s3-access-key-id", "", "S3 access key id")
	flag.StringVar(&target.secretKey, prefix+"-s3-secret-access-key", "", "S3 secret access key")
	flag.BoolVar(&target.pathStyle, prefix+"-s3-path-style", false, "use S3 path-style URLs instead of virtual-host URLs")
}

func openStore(cfg storeFlags) (storage.ObjectStore, error) {
	switch strings.TrimSpace(cfg.kind) {
	case "local":
		return storage.NewFileStore(cfg.dataDir), nil
	case "s3":
		return storage.NewS3StoreWithOptions(cfg.endpoint, cfg.bucket, cfg.region, cfg.accessKey, cfg.secretKey, storage.S3Options{PathStyle: cfg.pathStyle})
	default:
		return nil, fmt.Errorf("unsupported storage kind %q", cfg.kind)
	}
}
