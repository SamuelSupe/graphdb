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

	"gitlab.jiagouyun.com/guance/graphdb/internal/buildinfo"
	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/httpapi"
	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

const httpShutdownTimeout = 10 * time.Second

func run(args []string) error {
	if len(args) == 0 {
		printHelp()
		return nil
	}
	if args[0] == "version" || args[0] == "--version" {
		printVersion()
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
	storage.ConfigureParquetDecodeMaxConcurrent(cfg.ParquetDecodeMaxConcurrent)
	objects = storage.NewMeteredObjectStore(objects, pressure, nil)
	if cfg.WriterObjectCache && (cfg.Mode == "all" || cfg.Mode == "writer") {
		objects = storage.NewWriterObjectCache(objects, cfg.WriterObjectCacheConfig())
	}
	store := storage.NewTenantStore(objects, cfg.Prefix)
	store.MaxWriteCacheBytes = cfg.WriteCacheMaxBytes
	store.WriteEntityRecords = cfg.IndexEntityRecords
	store.UseEntityRecordsForRead = cfg.IndexEntityRecords
	store.EntityPagePackMaxBytes = cfg.EntityPagePackMaxBytes
	store.MaterializeCollectorStatus = cfg.IngestCollectorStatusMaterialized
	store.IngestMetadataMode = cfg.IngestMetadataMode
	store.ConfigureIndexObjectCache(storage.IndexObjectCacheConfig{
		MaxEntries: cfg.ReaderIndexCacheEntries,
		MaxBytes:   cfg.ReaderIndexCacheMaxBytes,
		DiskDir:    cfg.ReaderIndexCacheDir,
	})
	if cfg.InstanceID != "" {
		store.InstanceID = cfg.InstanceID
		store.ReaderID = cfg.InstanceID
	} else if hostname, err := os.Hostname(); err == nil && hostname != "" {
		store.ReaderID = fmt.Sprintf("%s|%s|%s|%s", hostname, cfg.Mode, cfg.Addr, cfg.Prefix)
	}
	store.Backpressure = pressure
	store.CoordinatorRetryLimit = cfg.WriteCASMaxRetries
	store.CoordinatorPendingTTL = cfg.CoordinatorPendingReservationTTL
	store.CoordinatorCleanup = cfg.CoordinatorCleanupConfig()

	coordinator, err := config.NewCoordinator(context.Background(), cfg)
	if err != nil {
		return err
	}
	if coordinator != nil {
		defer coordinator.Close()
	}
	if args[0] == "coordinator" {
		return coordinatorCommand(args[1:], store, coordinator)
	}
	if coordinator != nil {
		if err := coordinator.CheckSchema(context.Background()); err != nil {
			return err
		}
		store.SetCoordinator(coordinator)
		if err := store.EnsurePostgresMarker(context.Background()); err != nil {
			return err
		}
	} else if commandMayWrite(args[0], cfg.Mode) {
		if err := store.EnsureLocalWriterAllowed(context.Background()); err != nil {
			return err
		}
	}

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
	case "graphql":
		return runGraphQL(args[1:], store)
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

func commandMayWrite(command string, mode string) bool {
	if command == "serve" {
		return mode == "all" || mode == "writer"
	}
	switch command {
	case "init-tenant", "create-tenant", "set-tenant-metadata",
		"disable-tenant", "enable-tenant", "delete-tenant", "purge-tenant",
		"clone-tenant", "backup-tenant", "restore-tenant", "restore-drill-tenant",
		"commit", "ingest",
		"set-source-policy", "set-tenant-config", "replay-deadletters",
		"save-query", "start-task", "cancel-task", "retry-task",
		"create-index", "drop-index", "rebuild-indexes", "recover",
		"repair", "cleanup-commits", "gc", "compact":
		return true
	default:
		return false
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
	var ingestService *storage.IngestService
	if cfg.IngestMode == "wal" {
		ingestConfig := cfg.IngestServiceConfig()
		ingestConfig.Observer = obs.Metrics
		ingestConfig.Logger = obs.Logger
		ingestService, err = storage.OpenIngestService(store, ingestConfig)
		if err != nil {
			return fmt.Errorf("open ingest WAL service: %w", err)
		}
	}
	if metered := storage.FindMeteredObjectStore(store.Objects); metered != nil {
		metered.Observer = obs.Metrics
	}
	store.BackpressureObserver = obs.Metrics
	store.CacheObserver = obs.Metrics
	store.CoordinatorObserver = obs.Metrics
	store.StartCoordinatorStatusMonitor(ctx, cfg.PollInterval, cfg.ReadinessTimeout)
	obs.StartIndexHealthMonitor(ctx, cfg.IndexHealthInterval, func(checkCtx context.Context, tenantID string) (string, int, error) {
		health, err := store.IndexHealthWithOptions(checkCtx, tenantID, storage.IndexHealthOptions{})
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
		ReadinessTimeout:      cfg.ReadinessTimeout,
		IngestService:         ingestService,
		Observability:         obs,
		UsageCacheTTL:         cfg.TenantUsageCacheTTL,
	}
	api.StartMaintenanceLoop(ctx, cfg.MaintenanceInterval)
	if cfg.Mode == "all" || cfg.Mode == "writer" {
		store.StartCoordinatorMaintenance(ctx, cfg.PollInterval)
	}
	var servers []*http.Server
	if cfg.AdminAddr != "" {
		servers = []*http.Server{
			newDataHTTPServer(cfg, api),
			newAdminHTTPServer(cfg, api),
		}
	} else {
		servers = []*http.Server{newHTTPServer(cfg, api)}
	}
	obs.Logger.Info("server_start", map[string]any{
		"addr": cfg.Addr, "admin_addr": cfg.AdminAddr, "pprof_enabled": cfg.PprofEnabled,
		"mode": cfg.Mode, "storage": cfg.StoreKind, "prefix": cfg.Prefix,
		"coordination":         store.CoordinationBackend(),
		"ingest_mode":          cfg.IngestMode,
		"ingest_metadata_mode": cfg.IngestMetadataMode,
		"otlp_enabled":         cfg.OTLPEndpoint != "",
	})
	serverErr := runHTTPServers(ctx, servers, httpShutdownTimeout)
	if ingestService == nil {
		return serverErr
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.IngestShutdownTimeout)
	defer cancel()
	return errors.Join(serverErr, ingestService.Close(shutdownCtx))
}

func newHTTPServer(cfg config.Config, api *httpapi.Server) *http.Server {
	return newHTTPServerWithHandler(cfg.Addr, api.Handler())
}

func newDataHTTPServer(cfg config.Config, api *httpapi.Server) *http.Server {
	return newHTTPServerWithHandler(cfg.Addr, api.DataHandler())
}

func newAdminHTTPServer(cfg config.Config, api *httpapi.Server) *http.Server {
	return newHTTPServerWithHandler(cfg.AdminAddr, api.AdminHandler(cfg.PprofEnabled))
}

func newHTTPServerWithHandler(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      10 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
}

func runHTTPServer(ctx context.Context, server *http.Server, shutdownTimeout time.Duration) error {
	return runHTTPServers(ctx, []*http.Server{server}, shutdownTimeout)
}

func runHTTPServers(ctx context.Context, servers []*http.Server, shutdownTimeout time.Duration) error {
	if len(servers) == 0 {
		return nil
	}
	errCh := make(chan error, len(servers))
	for _, server := range servers {
		go func(server *http.Server) {
			errCh <- server.ListenAndServe()
		}(server)
	}
	var firstErr error
	received := 0
	select {
	case err := <-errCh:
		received++
		if !errors.Is(err, http.ErrServerClosed) {
			firstErr = err
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for received < len(servers) {
		err := <-errCh
		received++
		if err != nil && !errors.Is(err, http.ErrServerClosed) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func printVersion() {
	info := buildinfo.Current()
	fmt.Printf("GGraphDB %s commit=%s built=%s go=%s\n", info.Version, info.Commit, info.Date, info.GoVersion)
}

func printHelp() {
	fmt.Println(`graphdb commands:
  graphdb serve
  graphdb version
  graphdb coordinator migrate
  graphdb coordinator bootstrap --dry-run|--apply
  graphdb coordinator status
  graphdb coordinator sync-legacy-manifest
  graphdb coordinator rollback --dry-run
  graphdb coordinator rollback --apply --writers-stopped
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
  graphdb graphql <tenant-id> <graphql-request.json>
  graphdb gql <tenant-id> <legacy-query.gql> (deprecated legacy text DSL)
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
  GRAPHDB_ADDR=:8080
  GRAPHDB_ADMIN_ADDR=127.0.0.1:8081 (optional separate admin listener)
  GRAPHDB_PPROF_ENABLED=false (requires GRAPHDB_ADMIN_ADDR)
  GRAPHDB_MODE=all|writer|reader
  GRAPHDB_COORDINATION=local|postgres
  GRAPHDB_POSTGRES_DSN=postgres://user:password@host:5432/graphdb
  GRAPHDB_POSTGRES_SCHEMA=graphdb_coordination
  GRAPHDB_COORDINATOR_NAMESPACE=<stable-cluster-id>
  GRAPHDB_WRITE_CAS_MAX_RETRIES=8
  GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION=24h
  GRAPHDB_COORDINATOR_OUTBOX_RETENTION=1h
  GRAPHDB_COORDINATOR_CLEANUP_INTERVAL=1m
  GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE=5000
  GRAPHDB_READINESS_TIMEOUT=2s
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
  GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT=2
  GRAPHDB_WRITE_MAX_CONCURRENT=32
  GRAPHDB_WRITE_MAX_PER_TENANT=1 (1 is strict request serialization; 2-4 enables bounded request pipelining; 0 disables this admission dimension)
  GRAPHDB_WRITE_QUEUE_TIMEOUT=2s
  GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD=2s
  GRAPHDB_WRITE_CAS_CONFLICT_WINDOW=30s
  GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD=5
  GRAPHDB_WRITE_MAX_COMMIT_TAIL=20000
  GRAPHDB_WRITE_CACHE_MAX_BYTES=4GiB
  GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT=0
  GRAPHDB_WRITE_MAX_EDGES_PER_TENANT=0
  GRAPHDB_INGEST_MODE=wal|direct (default: wal for local writers; PostgreSQL requires explicit direct)
  GRAPHDB_INGEST_WAL_DIR=${GRAPHDB_DATA_DIR}/wal/ingest
  GRAPHDB_INGEST_WAL_DURABILITY=sync
  GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB
  GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=3ms
  GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB
  GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB
  GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK=80
  GRAPHDB_INGEST_WAL_HIGH_WATERMARK=70
  GRAPHDB_INGEST_WAL_STOP_WATERMARK=85
  GRAPHDB_INGEST_MAX_PENDING_AGE=2m
  GRAPHDB_INGEST_FLUSH_INTERVAL=250ms
  GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=8
  GRAPHDB_INGEST_FLUSH_MAX_BYTES=2MiB
  GRAPHDB_INGEST_FLUSH_WORKERS=2
  GRAPHDB_INGEST_METADATA_MODE=segment|legacy (default follows ingest mode)
  GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms
  GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2
  GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s
  GRAPHDB_SLOW_QUERY_THRESHOLD=500ms
  GRAPHDB_INDEX_HEALTH_INTERVAL=30s
  GRAPHDB_MAINTENANCE_INTERVAL=30s
  GRAPHDB_TENANT_USAGE_CACHE_TTL=60s
  GRAPHDB_READER_CATCHUP_TIMEOUT=2s
  GRAPHDB_READER_INDEX_CACHE_ENTRIES=4096
  GRAPHDB_READER_INDEX_CACHE_MAX_BYTES=256MiB
  GRAPHDB_READER_INDEX_CACHE_DIR=.graphdb/cache/index-objects
  GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB
  GRAPHDB_FAULT_OBJECT_READ_DELAY=25ms
  GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces
  GRAPHDB_OTLP_INSECURE=true
  GRAPHDB_SERVICE_NAME=graphdb
  S3_ENDPOINT=http://localhost:9000
  S3_BUCKET=graphdb
  S3_PATH_STYLE=false
  S3_REGION=us-east-1
  S3_ACCESS_KEY_ID=minioadmin
  S3_SECRET_ACCESS_KEY=minioadmin`)
}
