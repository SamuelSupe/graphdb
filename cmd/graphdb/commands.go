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

	"gitlab.jiagouyun.com/guance/graphdb/internal/bootstrap"
	"gitlab.jiagouyun.com/guance/graphdb/internal/buildinfo"
	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/httpapi"
	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

const (
	httpShutdownTimeout           = 10 * time.Second
	backgroundTaskShutdownTimeout = 30 * time.Second
)

func run(args []string) error {
	if len(args) == 0 {
		printHelp()
		return nil
	}
	command, ok := findCommand(args[0])
	if !ok {
		return fmt.Errorf("unknown command %q", args[0])
	}
	if command.kind == commandVersion {
		printVersion()
		return nil
	}
	if command.kind == commandHelp {
		printHelp()
		return nil
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	runtime, err := bootstrap.NewStorageRuntime(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer runtime.Close()
	store := runtime.Store
	coordinator := runtime.Coordinator
	if command.kind == commandCoordinator {
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
	} else if command.mayWrite(cfg.Mode) {
		if err := store.EnsureLocalWriterAllowed(context.Background()); err != nil {
			return err
		}
	}

	if command.kind == commandServe {
		return serve(cfg, store)
	}
	if command.handler == nil {
		return fmt.Errorf("command %q is not executable", command.name)
	}
	return command.handler(args[1:], store)
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
	store.SetObservers(obs.Metrics, obs.Metrics, obs.Metrics)
	store.StartCoordinatorStatusMonitor(ctx, cfg.PollInterval, cfg.ReadinessTimeout)
	obs.StartIndexHealthMonitor(ctx, cfg.IndexHealthInterval, func(checkCtx context.Context, tenantID string) (string, int, error) {
		health, err := store.IndexHealthWithOptions(checkCtx, tenantID, storage.IndexHealthOptions{})
		if err != nil {
			return "error", 1, err
		}
		return health.Status, len(health.Issues), nil
	})
	cache := storage.NewReaderCache(store, cfg.PollInterval)
	cache.IdleTTL = cfg.ReaderCacheIdleTTL
	cache.LoadTimeout = cfg.ReaderCacheLoadTimeout
	cache.ConfigureLoadAdmission(
		cfg.ReaderCacheLoadMaxConcurrent,
		cfg.ReaderCacheLoadQueueTimeout,
	)
	cache.Observer = obs.Metrics
	cache.Start(ctx)
	admission := httpapi.NewQueryAdmission(cfg.QueryMaxConcurrent, cfg.QueryMaxPerTenant, cfg.QueryQueueTimeout)
	readAdmission := httpapi.NewQueryAdmission(cfg.ReadMaxConcurrent, cfg.ReadMaxPerTenant, cfg.ReadQueueTimeout)
	writeAdmission := httpapi.NewWriteAdmission(cfg.WriteMaxConcurrent, cfg.WriteMaxPerTenant, cfg.WriteQueueTimeout)
	var apiIngestService httpapi.IngestService
	if ingestService != nil {
		apiIngestService = ingestService
	}
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
		RetrievalSearcher:     retrieval.NewService(store, nil),
		IngestService:         apiIngestService,
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
		"coordination": store.CoordinationBackend(),
		"ingest_mode":  cfg.IngestMode,
		"otlp_enabled": cfg.OTLPEndpoint != "",
	})
	serverErr := runHTTPServers(ctx, servers, httpShutdownTimeout)
	taskShutdownCtx, taskShutdownCancel := context.WithTimeout(
		context.Background(),
		backgroundTaskShutdownTimeout,
	)
	taskShutdownErr := store.ShutdownTasks(taskShutdownCtx)
	taskShutdownCancel()
	if ingestService == nil {
		return errors.Join(serverErr, taskShutdownErr)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.IngestShutdownTimeout)
	defer cancel()
	return errors.Join(serverErr, taskShutdownErr, ingestService.Close(shutdownCtx))
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
	fmt.Println("graphdb commands:")
	for _, command := range commandSpecs {
		for _, usage := range command.usage {
			fmt.Printf("  %s\n", usage)
		}
	}
	fmt.Println(`
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
  GRAPHDB_WRITE_MAX_COMMIT_TAIL=300
  GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT=0
  GRAPHDB_WRITE_MAX_EDGES_PER_TENANT=0
  GRAPHDB_INGEST_MODE=direct|wal
  GRAPHDB_INGEST_WAL_DIR=${GRAPHDB_DATA_DIR}/wal/ingest
  GRAPHDB_INGEST_WAL_DURABILITY=sync|os
  GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB
  GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=5ms
  GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB
  GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB
  GRAPHDB_INGEST_FLUSH_INTERVAL=10s
  GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=256
  GRAPHDB_INGEST_FLUSH_MAX_BYTES=8MiB
  GRAPHDB_INGEST_FLUSH_WORKERS=1
  GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s
  GRAPHDB_SLOW_QUERY_THRESHOLD=500ms
  GRAPHDB_INDEX_HEALTH_INTERVAL=30s
  GRAPHDB_MAINTENANCE_INTERVAL=30s
  GRAPHDB_TENANT_USAGE_CACHE_TTL=60s
  GRAPHDB_READER_CACHE_IDLE_TTL=15m
  GRAPHDB_READER_CACHE_LOAD_TIMEOUT=1m
  GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT=4
  GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT=2s
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
