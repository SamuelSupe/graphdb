package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"graphdb/internal/config"
	"graphdb/internal/httpapi"
	"graphdb/internal/observability"
	"graphdb/internal/storage"
)

const httpShutdownTimeout = 10 * time.Second

func run(args []string) error {
	if len(args) == 0 {
		printHelp()
		return nil
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	objects, err := config.NewObjectStore(cfg)
	if err != nil {
		return err
	}
	pressure := storage.NewWritePressure(cfg.BackpressureConfig())
	objects = storage.NewDelayedReadObjectStore(objects, cfg.FaultObjectReadDelay)
	objects = storage.NewReadProtectedObjectStore(objects, storage.ReadProtectionConfig{
		MaxConcurrent: cfg.ReadObjectMaxConcurrent,
		Singleflight:  cfg.ReadObjectSingleflight,
	})
	objects = storage.NewMeteredObjectStore(objects, pressure, nil)
	store := storage.NewTenantStore(objects, cfg.Prefix)
	store.ConfigureIndexObjectCache(storage.IndexObjectCacheConfig{
		MaxEntries: cfg.ReaderIndexCacheEntries,
		DiskDir:    cfg.ReaderIndexCacheDir,
	})
	if cfg.InstanceID != "" {
		store.InstanceID = cfg.InstanceID
	}
	store.Backpressure = pressure

	switch args[0] {
	case "serve":
		return serve(cfg, store)
	case "init-tenant":
		return initTenant(args[1:], store)
	case "list-tenants":
		return listTenants(args[1:], store)
	case "tenant":
		return tenantInfo(args[1:], store)
	case "create-tenant":
		return createTenant(args[1:], store)
	case "set-tenant-metadata":
		return setTenantMetadata(args[1:], store)
	case "disable-tenant":
		return disableTenant(args[1:], store)
	case "enable-tenant":
		return enableTenant(args[1:], store)
	case "delete-tenant":
		return deleteTenant(args[1:], store)
	case "purge-tenant":
		return purgeTenant(args[1:], store)
	case "clone-tenant":
		return cloneTenant(args[1:], store)
	case "backup-tenant":
		return backupTenant(args[1:], store)
	case "restore-tenant":
		return restoreTenant(args[1:], store)
	case "restore-drill-tenant":
		return restoreDrillTenant(args[1:], store)
	case "commit":
		return commit(args[1:], store)
	case "ingest":
		return ingest(args[1:], store)
	case "collector-status":
		return collectorStatus(args[1:], store)
	case "source-policy":
		return sourcePolicy(args[1:], store)
	case "set-source-policy":
		return setSourcePolicy(args[1:], store)
	case "tenant-config":
		return tenantConfig(args[1:], store)
	case "set-tenant-config":
		return setTenantConfig(args[1:], store)
	case "tenant-usage":
		return tenantUsage(args[1:], store)
	case "deadletters":
		return deadLetters(args[1:], store)
	case "replay-deadletters":
		return replayDeadLetters(args[1:], store)
	case "query":
		return runQuery(args[1:], store)
	case "gql":
		return runGQL(args[1:], store)
	case "save-query":
		return saveQuery(args[1:], store)
	case "list-queries":
		return listQueries(args[1:], store)
	case "run-saved-query":
		return runSavedQuery(args[1:], store)
	case "start-task":
		return startTask(args[1:], store)
	case "list-tasks":
		return listTasks(args[1:], store)
	case "task":
		return getTask(args[1:], store)
	case "cancel-task":
		return cancelTask(args[1:], store)
	case "retry-task":
		return retryTask(args[1:], store)
	case "index-catalog":
		return indexCatalog(args[1:], store)
	case "index-inspect":
		return indexInspect(args[1:], store)
	case "index-definitions":
		return indexDefinitions(args[1:], store)
	case "create-index":
		return createIndex(args[1:], store)
	case "drop-index":
		return dropIndex(args[1:], store)
	case "index-health":
		return indexHealth(args[1:], store)
	case "integrity-audit":
		return integrityAudit(args[1:], store)
	case "rebuild-indexes":
		return rebuildIndexes(args[1:], store)
	case "writer-lease":
		return writerLease(args[1:], store)
	case "recover":
		return recoverTenant(args[1:], store)
	case "repair":
		return repairTenant(args[1:], store)
	case "cleanup-commits":
		return cleanupCommits(args[1:], store)
	case "gc":
		return runGC(args[1:], store)
	case "compact":
		return compact(args[1:], store)
	case "help", "-h", "--help":
		printHelp()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(cfg config.Config, store *storage.TenantStore) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveContext(ctx, cfg, store)
}

func serveContext(ctx context.Context, cfg config.Config, store *storage.TenantStore) error {
	shutdownTrace, err := observability.SetupOTLP(ctx, observability.TraceConfig{
		Endpoint:    cfg.OTLPEndpoint,
		Insecure:    cfg.OTLPInsecure,
		ServiceName: cfg.ServiceName,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTrace(shutdownCtx)
	}()
	obs := observability.New(os.Stdout, cfg.SlowQueryThreshold)
	if metered, ok := store.Objects.(*storage.MeteredObjectStore); ok {
		metered.Observer = obs.Metrics
	}
	store.BackpressureObserver = obs.Metrics
	store.CacheObserver = obs.Metrics
	obs.StartIndexHealthMonitor(ctx, cfg.IndexHealthInterval, func(checkCtx context.Context, tenantID string) (string, int, error) {
		health, err := store.IndexHealth(checkCtx, tenantID)
		if err != nil {
			return "error", 1, err
		}
		return health.Status, len(health.Issues), nil
	})
	cache := storage.NewReaderCache(store, cfg.PollInterval)
	cache.Observer = obs.Metrics
	cache.Start(ctx)
	admission := httpapi.NewQueryAdmission(cfg.QueryMaxConcurrent, cfg.QueryMaxPerTenant, cfg.QueryQueueTimeout)
	readAdmission := httpapi.NewQueryAdmission(cfg.ReadMaxConcurrent, cfg.ReadMaxPerTenant, cfg.ReadQueueTimeout)
	writeAdmission := httpapi.NewWriteAdmission(cfg.WriteMaxConcurrent, cfg.WriteMaxPerTenant, cfg.WriteQueueTimeout)
	api := &httpapi.Server{
		Store:                 store,
		Cache:                 cache,
		Mode:                  cfg.Mode,
		Admission:             admission,
		ReadAdmission:         readAdmission,
		WriteAdmission:        writeAdmission,
		WriteExecutionTimeout: cfg.WriteExecutionTimeout,
		ReaderCatchupTimeout:  cfg.ReaderCatchupTimeout,
		Observability:         obs,
		UsageCacheTTL:         cfg.TenantUsageCacheTTL,
	}
	api.StartMaintenanceLoop(ctx, cfg.MaintenanceInterval)
	server := newHTTPServer(cfg, api)
	obs.Logger.Info("server_start", map[string]any{
		"addr": cfg.Addr, "mode": cfg.Mode, "storage": cfg.StoreKind, "prefix": cfg.Prefix,
		"otlp_enabled": cfg.OTLPEndpoint != "",
	})
	return runHTTPServer(ctx, server, httpShutdownTimeout)
}

func newHTTPServer(cfg config.Config, api *httpapi.Server) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      10 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
}

func runHTTPServer(ctx context.Context, server *http.Server, shutdownTimeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		err := <-errCh
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func printHelp() {
	fmt.Println(`graphdb commands:
  graphdb serve
  graphdb init-tenant <tenant-id>
  graphdb list-tenants
  graphdb tenant <tenant-id>
  graphdb create-tenant <tenant-id> [metadata.json]
  graphdb set-tenant-metadata <tenant-id> <metadata.json>
  graphdb disable-tenant <tenant-id>
  graphdb enable-tenant <tenant-id>
  graphdb delete-tenant <tenant-id>
  graphdb purge-tenant <tenant-id> [--force]
  graphdb clone-tenant <source-tenant-id> <target-tenant-id> [metadata.json]
  graphdb backup-tenant <tenant-id>
  graphdb restore-tenant <tenant-id> <backup-key> [--overwrite] [--dry-run]
  graphdb restore-drill-tenant <tenant-id> [params.json]
  graphdb commit <tenant-id> <commit.json>
  graphdb ingest <tenant-id> <ingest.json>
  graphdb collector-status <tenant-id> <source> <collector-id>
  graphdb source-policy <tenant-id>
  graphdb set-source-policy <tenant-id> <policy.json>
  graphdb tenant-config <tenant-id>
  graphdb set-tenant-config <tenant-id> <config.json>
  graphdb tenant-usage <tenant-id>
  graphdb deadletters <tenant-id> <source>
  graphdb replay-deadletters <tenant-id> <source> [limit]
  graphdb query <tenant-id> <query.json>
  graphdb gql <tenant-id> <query.gql>
  graphdb save-query <tenant-id> <saved-query.json>
  graphdb list-queries <tenant-id>
  graphdb run-saved-query <tenant-id> <name>
  graphdb start-task <tenant-id> <type> [params.json]
  graphdb list-tasks <tenant-id> [type] [status]
  graphdb task <tenant-id> <task-id>
  graphdb cancel-task <tenant-id> <task-id>
  graphdb retry-task <tenant-id> <task-id>
  graphdb index-catalog <tenant-id>
  graphdb index-inspect <tenant-id>
  graphdb index-definitions <tenant-id>
  graphdb create-index <tenant-id> <kind> <field> [name]
  graphdb drop-index <tenant-id> <name>
  graphdb index-health <tenant-id>
  graphdb integrity-audit <tenant-id> [--shallow]
  graphdb rebuild-indexes <tenant-id>
  graphdb writer-lease <tenant-id>
  graphdb recover <tenant-id>
  graphdb repair <tenant-id> [--apply]
  graphdb cleanup-commits <tenant-id>
  graphdb gc <tenant-id> [deadletter-max-age-seconds] [task-max-age-seconds]
  graphdb compact <tenant-id>

Environment:
  GRAPHDB_MODE=all|writer|reader
  GRAPHDB_STORAGE=local|s3
  GRAPHDB_DATA_DIR=.graphdb
  GRAPHDB_PREFIX=graphdb
  GRAPHDB_QUERY_MAX_CONCURRENT=64
  GRAPHDB_QUERY_MAX_PER_TENANT=32
  GRAPHDB_QUERY_QUEUE_TIMEOUT=5s
  GRAPHDB_READ_MAX_CONCURRENT=128
  GRAPHDB_READ_MAX_PER_TENANT=64
  GRAPHDB_READ_QUEUE_TIMEOUT=500ms
  GRAPHDB_READ_OBJECT_MAX_CONCURRENT=128
  GRAPHDB_READ_OBJECT_SINGLEFLIGHT=true
  GRAPHDB_WRITE_MAX_CONCURRENT=32
  GRAPHDB_WRITE_MAX_PER_TENANT=1 (single-writer mode; 0 disables this admission dimension)
  GRAPHDB_WRITE_QUEUE_TIMEOUT=2s
  GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD=2s
  GRAPHDB_WRITE_CAS_CONFLICT_WINDOW=30s
  GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD=5
  GRAPHDB_WRITE_MAX_COMMIT_TAIL=300
  GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT=0
  GRAPHDB_WRITE_MAX_EDGES_PER_TENANT=0
  GRAPHDB_SLOW_QUERY_THRESHOLD=500ms
  GRAPHDB_INDEX_HEALTH_INTERVAL=30s
  GRAPHDB_MAINTENANCE_INTERVAL=30s
  GRAPHDB_TENANT_USAGE_CACHE_TTL=60s
  GRAPHDB_READER_CATCHUP_TIMEOUT=2s
  GRAPHDB_READER_INDEX_CACHE_ENTRIES=4096
  GRAPHDB_READER_INDEX_CACHE_DIR=.graphdb/cache/index-objects
  GRAPHDB_FAULT_OBJECT_READ_DELAY=25ms
  GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces
  GRAPHDB_OTLP_INSECURE=true
  GRAPHDB_SERVICE_NAME=graphdb
  S3_ENDPOINT=http://localhost:9000
  S3_BUCKET=graphdb
  S3_REGION=us-east-1
  S3_ACCESS_KEY_ID=minioadmin
  S3_SECRET_ACCESS_KEY=minioadmin`)
}
