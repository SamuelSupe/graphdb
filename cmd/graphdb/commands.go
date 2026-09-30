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

	"github.com/SamuelSupe/graphdb/v2/internal/bootstrap"
	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/ha"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const (
	httpShutdownTimeout           = 10 * time.Second
	backgroundTaskShutdownTimeout = 30 * time.Second
)

func run(args []string) (err error) {
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
	if command.kind == commandCoordinator {
		return fmt.Errorf("coordinator commands are unsupported in the local disk edition")
	}
	if command.kind == commandRouter {
		cfg, err := config.LoadRouter()
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		router := sharding.NewRouter(cfg.Catalog, cfg.Token)
		defer router.Client.HTTP.CloseIdleConnections()
		return runHTTPServer(ctx, newHTTPServerWithHandler(cfg.Addr, router), httpShutdownTimeout)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.Raft.Enabled && command.kind != commandServe {
		return fmt.Errorf("offline commands are disabled in HA mode; use the cluster HTTP API")
	}
	runtime, err := bootstrap.NewStorageRuntime(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, runtime.Close()) }()
	store := runtime.Store

	if command.kind == commandServe {
		return serve(cfg, runtime)
	}
	if command.handler == nil {
		return fmt.Errorf("command %q is not executable", command.name)
	}
	return command.handler(args[1:], store)
}

func serve(cfg config.Config, runtime *bootstrap.StorageRuntime) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveContext(ctx, cfg, runtime)
}

func serveContext(ctx context.Context, cfg config.Config, runtime *bootstrap.StorageRuntime) error {
	store := runtime.Store
	ctx, stop := context.WithCancel(ctx)
	defer stop()
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
	cache := storage.NewReaderCache(store, cfg.PollInterval)
	cache.IdleTTL = cfg.ReaderCacheIdleTTL
	cache.ConfigureCapacity(cfg.ReaderCacheMaxTenants, cfg.ReaderCacheMaxBytes)
	cache.LoadTimeout = cfg.ReaderCacheLoadTimeout
	cache.ConfigureLoadAdmission(
		cfg.ReaderCacheLoadMaxConcurrent,
		cfg.ReaderCacheLoadQueueTimeout,
	)
	var ingestService *storage.IngestService
	if cfg.IngestMode == "wal" && !cfg.Raft.Enabled {
		ingestConfig := cfg.IngestServiceConfig()
		ingestConfig.Observer = obs.Metrics
		ingestConfig.Logger = obs.Logger

		ingestService, err = storage.OpenIngestService(store, ingestConfig)
		if err != nil {
			return fmt.Errorf("open ingest WAL service: %w", err)
		}
	}
	runtime.Ingest = ingestService
	if metered := storage.FindMeteredObjectStore(store.Objects); metered != nil {
		metered.Observer = obs.Metrics
	}
	store.SetObservers(obs.Metrics, obs.Metrics)

	obs.StartIndexHealthMonitor(ctx, cfg.IndexHealthInterval, func(checkCtx context.Context, tenantID string) (string, int, error) {
		health, err := store.IndexHealthWithOptions(checkCtx, tenantID, storage.IndexHealthOptions{})
		if err != nil {
			return "error", 1, err
		}
		return health.Status, len(health.Issues), nil
	})
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
		IngestService:         apiIngestService,
		Observability:         obs,
		UsageCacheTTL:         cfg.TenantUsageCacheTTL,
	}
	var cluster *ha.Cluster
	if cfg.Raft.Enabled {
		cluster = ha.New(cfg, store, runtime.Files)
		api.Cluster = cluster
		cluster.App.Handler = api.Handler()
		if err := cluster.Start(ctx, cfg.Raft); err != nil {
			return err
		}
		defer cluster.Close()
	} else {
		api.StartMaintenanceLoop(ctx, cfg.MaintenanceInterval)
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
	if cluster != nil {
		servers = append(servers, newHTTPServerWithHandler(cfg.Raft.Addr, cluster.PrivateHandler()))
	}
	obs.Logger.Info("server_start", map[string]any{
		"addr": cfg.Addr, "admin_addr": cfg.AdminAddr, "pprof_enabled": cfg.PprofEnabled,
		"mode": cfg.Mode, "storage": cfg.StoreKind, "prefix": cfg.Prefix,
		"coordination": store.CoordinationBackend(),
		"ingest_mode":  cfg.IngestMode,
		"otlp_enabled": cfg.OTLPEndpoint != "",
	})
	serverErr := runHTTPServers(ctx, servers, httpShutdownTimeout)
	stop()
	var clusterErr error
	if cluster != nil {
		clusterErr = cluster.Close()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), max(backgroundTaskShutdownTimeout, cfg.IngestShutdownTimeout))
	defer cancel()
	return errors.Join(serverErr, clusterErr, runtime.Shutdown(shutdownCtx))
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
		if err := server.Shutdown(shutdownCtx); err != nil {
			// Shutdown leaves active request contexts alive on timeout. Close
			// their connections so canceled reads release their file views.
			firstErr = errors.Join(firstErr, err, server.Close())
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
  GRAPHDB_MODE=all
  GRAPHDB_COORDINATION=local
  GRAPHDB_WRITE_CAS_MAX_RETRIES=8
  GRAPHDB_READINESS_TIMEOUT=2s
  GRAPHDB_STORAGE=local
  GRAPHDB_DATA_DIR=.graphdb
  GRAPHDB_PREFIX=graphdb
  GRAPHDB_RAFT_NODE_ID= (set to enable HA; initial IDs 1,2,3)
  GRAPHDB_RAFT_CLUSTER_ID= (required in HA)
  GRAPHDB_RAFT_ADDR=:8082 (required separate private listener in HA)
  GRAPHDB_RAFT_PEERS= (JSON map of IDs to private HTTP origins)
  GRAPHDB_RAFT_TOKEN= (shared token, at least 32 bytes)
  GRAPHDB_RAFT_BOOTSTRAP=true (false for a replacement learner)
  GRAPHDB_RAFT_DIR=${GRAPHDB_DATA_DIR}/.graphdb-raft
  GRAPHDB_RAFT_SNAPSHOT_ENTRIES=1000
  GRAPHDB_RAFT_MAX_SNAPSHOT_BYTES=512MiB
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
  GRAPHDB_READER_CACHE_MAX_TENANTS=64
  GRAPHDB_READER_CACHE_MAX_BYTES=512MiB
  GRAPHDB_READER_CACHE_LOAD_TIMEOUT=1m
  GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT=4
  GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT=2s
  GRAPHDB_READER_CATCHUP_TIMEOUT=2s
  GRAPHDB_READER_INDEX_CACHE_ENTRIES=4096
  GRAPHDB_READER_INDEX_CACHE_MAX_BYTES=256MiB
  GRAPHDB_READER_INDEX_CACHE_DIR= (optional; disabled by default)
  GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB
  GRAPHDB_FAULT_OBJECT_READ_DELAY=25ms
  GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces
  GRAPHDB_OTLP_INSECURE=true
  GRAPHDB_SERVICE_NAME=graphdb`)
}
