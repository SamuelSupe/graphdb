package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type Config struct {
	Addr                              string
	AdminAddr                         string
	PprofEnabled                      bool
	Mode                              string
	Prefix                            string
	PollInterval                      time.Duration
	DataDir                           string
	StoreKind                         string
	QueryMaxConcurrent                int
	QueryMaxPerTenant                 int
	QueryQueueTimeout                 time.Duration
	ReadMaxConcurrent                 int
	ReadMaxPerTenant                  int
	ReadQueueTimeout                  time.Duration
	ReadObjectMaxConcurrent           int
	ReadObjectSingleflight            bool
	ParquetDecodeMaxConcurrent        int
	WriteMaxConcurrent                int
	WriteMaxPerTenant                 int
	WriteQueueTimeout                 time.Duration
	WriteExecutionTimeout             time.Duration
	WriteObjectLatencyThreshold       time.Duration
	WriteObjectErrorWindow            time.Duration
	WriteObjectErrorThreshold         int
	WriteCASConflictWindow            time.Duration
	WriteCASConflictThreshold         int
	WriteMaxCommitTail                int
	WriteMaxObjectsPerTenant          int
	WriteMaxBytesPerTenant            int64
	WriteMaxEntitiesPerTenant         int
	WriteMaxEdgesPerTenant            int
	WriteCacheMaxBytes                int64
	WriterObjectCache                 bool
	WriterObjectCacheMaxBytes         int64
	WriterObjectCacheMaxKeys          int
	WriterObjectCacheNegativeTTL      time.Duration
	IngestCollectorStatusMaterialized bool
	IngestMode                        string
	IngestWALDir                      string
	IngestWALDurability               string
	IngestWALBufferBytes              int64
	IngestWALFsyncInterval            time.Duration
	IngestWALMaxBytes                 int64
	IngestWALSegmentBytes             int64
	IngestWALAppendQueue              int
	IngestQueueMemoryBytes            int64
	IngestQueueHighWatermark          int
	IngestWALHighWatermark            int
	IngestWALStopWatermark            int
	IngestMaxPendingAge               time.Duration
	IngestFlushInterval               time.Duration
	IngestFlushMaxRequests            int
	IngestFlushMaxBytes               int64
	IngestFlushWorkers                int
	IngestMetadataMode                string
	IngestMetadataFlushInterval       time.Duration
	IngestMetadataMaxRequests         int
	IngestMetadataMaxBytes            int64
	IngestMetadataFlushWorkers        int
	IngestShutdownTimeout             time.Duration
	SlowQueryThreshold                time.Duration
	IndexHealthInterval               time.Duration
	MaintenanceInterval               time.Duration
	TenantUsageCacheTTL               time.Duration
	ReaderCatchupTimeout              time.Duration
	ReadinessTimeout                  time.Duration
	ReaderIndexCacheEntries           int
	ReaderIndexCacheMaxBytes          int64
	ReaderIndexCacheDir               string
	IndexEntityRecords                bool
	EntityPagePackMaxBytes            int64
	FaultObjectReadDelay              time.Duration
	OTLPEndpoint                      string
	OTLPInsecure                      bool
	ServiceName                       string
	InstanceID                        string
	Coordination                      string
	PostgresDSN                       string
	PostgresSchema                    string
	CoordinatorNamespace              string
	WriteCASMaxRetries                int
	CoordinatorIdempotencyRetention   time.Duration
	CoordinatorPendingReservationTTL  time.Duration
	CoordinatorOutboxRetention        time.Duration
	CoordinatorCleanupInterval        time.Duration
	CoordinatorCleanupBatchSize       int

	S3Endpoint        string
	S3Bucket          string
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3PathStyle       bool
	S3Provider        string
	S3Versioning      string
	WriterTopology    string
}

func Load() (Config, error) {
	cleanup := storage.DefaultCoordinatorCleanupConfig()
	mode := getenv("GRAPHDB_MODE", "all")
	ingestMode := strings.ToLower(strings.TrimSpace(os.Getenv("GRAPHDB_INGEST_MODE")))
	if ingestMode == "" {
		if mode == "reader" {
			ingestMode = "direct"
		} else {
			ingestMode = "wal"
		}
	}
	ingestMetadataMode := strings.ToLower(strings.TrimSpace(os.Getenv("GRAPHDB_INGEST_METADATA_MODE")))
	if ingestMetadataMode == "" {
		if ingestMode == "wal" {
			ingestMetadataMode = storage.IngestMetadataModeSegment
		} else {
			ingestMetadataMode = storage.IngestMetadataModeLegacy
		}
	}
	cfg := Config{
		Addr:                              getenv("GRAPHDB_ADDR", ":8080"),
		AdminAddr:                         strings.TrimSpace(os.Getenv("GRAPHDB_ADMIN_ADDR")),
		Mode:                              mode,
		Prefix:                            getenv("GRAPHDB_PREFIX", "graphdb"),
		PollInterval:                      2 * time.Second,
		DataDir:                           getenv("GRAPHDB_DATA_DIR", ".graphdb"),
		StoreKind:                         os.Getenv("GRAPHDB_STORAGE"),
		QueryMaxConcurrent:                64,
		QueryMaxPerTenant:                 32,
		QueryQueueTimeout:                 5 * time.Second,
		ReadMaxConcurrent:                 128,
		ReadMaxPerTenant:                  64,
		ReadQueueTimeout:                  500 * time.Millisecond,
		ReadObjectMaxConcurrent:           128,
		ReadObjectSingleflight:            true,
		ParquetDecodeMaxConcurrent:        2,
		WriteMaxConcurrent:                32,
		WriteMaxPerTenant:                 1,
		WriteQueueTimeout:                 2 * time.Second,
		WriteExecutionTimeout:             90 * time.Second,
		WriteObjectLatencyThreshold:       2 * time.Second,
		WriteObjectErrorWindow:            30 * time.Second,
		WriteObjectErrorThreshold:         1,
		WriteCASConflictWindow:            30 * time.Second,
		WriteCASConflictThreshold:         5,
		WriteMaxCommitTail:                20000,
		WriteCacheMaxBytes:                4 * 1024 * 1024 * 1024,
		WriterObjectCache:                 true,
		WriterObjectCacheMaxBytes:         512 * 1024 * 1024,
		WriterObjectCacheMaxKeys:          200000,
		WriterObjectCacheNegativeTTL:      5 * time.Minute,
		IngestCollectorStatusMaterialized: true,
		IngestMode:                        ingestMode,
		IngestWALDurability:               strings.ToLower(strings.TrimSpace(getenv("GRAPHDB_INGEST_WAL_DURABILITY", storage.IngestWALDurabilitySync))),
		IngestWALBufferBytes:              4 * 1024 * 1024,
		IngestWALFsyncInterval:            3 * time.Millisecond,
		IngestWALMaxBytes:                 10 * 1024 * 1024 * 1024,
		IngestWALSegmentBytes:             256 * 1024 * 1024,
		IngestWALAppendQueue:              4096,
		IngestQueueMemoryBytes:            256 * 1024 * 1024,
		IngestQueueHighWatermark:          80,
		IngestWALHighWatermark:            70,
		IngestWALStopWatermark:            85,
		IngestMaxPendingAge:               2 * time.Minute,
		IngestFlushInterval:               250 * time.Millisecond,
		IngestFlushMaxRequests:            8,
		IngestFlushMaxBytes:               2 * 1024 * 1024,
		IngestFlushWorkers:                2,
		IngestMetadataMode:                ingestMetadataMode,
		IngestMetadataFlushInterval:       500 * time.Millisecond,
		IngestMetadataMaxRequests:         256,
		IngestMetadataMaxBytes:            8 * 1024 * 1024,
		IngestMetadataFlushWorkers:        2,
		IngestShutdownTimeout:             30 * time.Second,
		SlowQueryThreshold:                500 * time.Millisecond,
		IndexHealthInterval:               30 * time.Second,
		MaintenanceInterval:               30 * time.Second,
		TenantUsageCacheTTL:               60 * time.Second,
		ReaderCatchupTimeout:              2 * time.Second,
		ReadinessTimeout:                  2 * time.Second,
		ReaderIndexCacheEntries:           4096,
		ReaderIndexCacheMaxBytes:          256 * 1024 * 1024,
		IndexEntityRecords:                false,
		EntityPagePackMaxBytes:            32 * 1024 * 1024,
		OTLPEndpoint:                      os.Getenv("GRAPHDB_OTLP_ENDPOINT"),
		ServiceName:                       getenv("GRAPHDB_SERVICE_NAME", "graphdb"),
		InstanceID:                        strings.TrimSpace(os.Getenv("GRAPHDB_INSTANCE_ID")),
		Coordination:                      normalizeCoordination(os.Getenv("GRAPHDB_COORDINATION")),
		PostgresDSN:                       strings.TrimSpace(os.Getenv("GRAPHDB_POSTGRES_DSN")),
		PostgresSchema:                    getenv("GRAPHDB_POSTGRES_SCHEMA", "graphdb_coordination"),
		CoordinatorNamespace:              strings.TrimSpace(os.Getenv("GRAPHDB_COORDINATOR_NAMESPACE")),
		WriteCASMaxRetries:                8,
		CoordinatorIdempotencyRetention:   cleanup.IdempotencyRetention,
		CoordinatorPendingReservationTTL:  cleanup.PendingReservationTTL,
		CoordinatorOutboxRetention:        cleanup.OutboxRetention,
		CoordinatorCleanupInterval:        cleanup.Interval,
		CoordinatorCleanupBatchSize:       cleanup.BatchSize,
		S3Endpoint:                        os.Getenv("S3_ENDPOINT"),
		S3Bucket:                          os.Getenv("S3_BUCKET"),
		S3Region:                          getenv("S3_REGION", "us-east-1"),
		S3Provider:                        storage.NormalizeObjectProvider(os.Getenv("S3_PROVIDER")),
		S3Versioning:                      strings.ToLower(strings.TrimSpace(os.Getenv("S3_VERSIONING"))),
		WriterTopology:                    normalizeWriterTopology(os.Getenv("GRAPHDB_WRITER_TOPOLOGY")),
	}
	cfg.S3AccessKeyID = firstNonEmpty(os.Getenv("S3_ACCESS_KEY_ID"), os.Getenv("AWS_ACCESS_KEY_ID"))
	cfg.S3SecretAccessKey = firstNonEmpty(os.Getenv("S3_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
	if err := loadBoolEnv("S3_PATH_STYLE", &cfg.S3PathStyle); err != nil {
		return Config{}, err
	}
	if err := loadBoolEnv("GRAPHDB_PPROF_ENABLED", &cfg.PprofEnabled); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_POLL_INTERVAL", &cfg.PollInterval); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_QUERY_MAX_CONCURRENT", &cfg.QueryMaxConcurrent); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_QUERY_MAX_PER_TENANT", &cfg.QueryMaxPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_QUERY_QUEUE_TIMEOUT", &cfg.QueryQueueTimeout); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_READ_MAX_CONCURRENT", &cfg.ReadMaxConcurrent); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_READ_MAX_PER_TENANT", &cfg.ReadMaxPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_READ_QUEUE_TIMEOUT", &cfg.ReadQueueTimeout); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_READ_OBJECT_MAX_CONCURRENT", &cfg.ReadObjectMaxConcurrent); err != nil {
		return Config{}, err
	}
	if err := loadBoolEnv("GRAPHDB_READ_OBJECT_SINGLEFLIGHT", &cfg.ReadObjectSingleflight); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT", &cfg.ParquetDecodeMaxConcurrent); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_MAX_CONCURRENT", &cfg.WriteMaxConcurrent); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_MAX_PER_TENANT", &cfg.WriteMaxPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_WRITE_QUEUE_TIMEOUT", &cfg.WriteQueueTimeout); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_WRITE_EXECUTION_TIMEOUT", &cfg.WriteExecutionTimeout); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD", &cfg.WriteObjectLatencyThreshold); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_WRITE_OBJECT_ERROR_WINDOW", &cfg.WriteObjectErrorWindow); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD", &cfg.WriteObjectErrorThreshold); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_WRITE_CAS_CONFLICT_WINDOW", &cfg.WriteCASConflictWindow); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD", &cfg.WriteCASConflictThreshold); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_CAS_MAX_RETRIES", &cfg.WriteCASMaxRetries); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION", &cfg.CoordinatorIdempotencyRetention); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL", &cfg.CoordinatorPendingReservationTTL); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_COORDINATOR_OUTBOX_RETENTION", &cfg.CoordinatorOutboxRetention); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_COORDINATOR_CLEANUP_INTERVAL", &cfg.CoordinatorCleanupInterval); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE", &cfg.CoordinatorCleanupBatchSize); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_MAX_COMMIT_TAIL", &cfg.WriteMaxCommitTail); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT", &cfg.WriteMaxObjectsPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadInt64Env("GRAPHDB_WRITE_MAX_BYTES_PER_TENANT", &cfg.WriteMaxBytesPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT", &cfg.WriteMaxEntitiesPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITE_MAX_EDGES_PER_TENANT", &cfg.WriteMaxEdgesPerTenant); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_WRITE_CACHE_MAX_BYTES", &cfg.WriteCacheMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadBoolEnv("GRAPHDB_WRITER_OBJECT_CACHE", &cfg.WriterObjectCache); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES", &cfg.WriterObjectCacheMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_KEYS", &cfg.WriterObjectCacheMaxKeys); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL", &cfg.WriterObjectCacheNegativeTTL); err != nil {
		return Config{}, err
	}
	if err := loadBoolEnv("GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED", &cfg.IngestCollectorStatusMaterialized); err != nil {
		return Config{}, err
	}
	cfg.IngestWALDir = strings.TrimSpace(os.Getenv("GRAPHDB_INGEST_WAL_DIR"))
	if err := loadBytesEnv("GRAPHDB_INGEST_WAL_BUFFER_BYTES", &cfg.IngestWALBufferBytes); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_INGEST_WAL_FSYNC_INTERVAL", &cfg.IngestWALFsyncInterval); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_INGEST_WAL_MAX_BYTES", &cfg.IngestWALMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_INGEST_WAL_SEGMENT_BYTES", &cfg.IngestWALSegmentBytes); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_WAL_APPEND_QUEUE", &cfg.IngestWALAppendQueue); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES", &cfg.IngestQueueMemoryBytes); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK", &cfg.IngestQueueHighWatermark); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_WAL_HIGH_WATERMARK", &cfg.IngestWALHighWatermark); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_WAL_STOP_WATERMARK", &cfg.IngestWALStopWatermark); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_INGEST_MAX_PENDING_AGE", &cfg.IngestMaxPendingAge); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_INGEST_FLUSH_INTERVAL", &cfg.IngestFlushInterval); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_FLUSH_MAX_REQUESTS", &cfg.IngestFlushMaxRequests); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_INGEST_FLUSH_MAX_BYTES", &cfg.IngestFlushMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_FLUSH_WORKERS", &cfg.IngestFlushWorkers); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL", &cfg.IngestMetadataFlushInterval); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_METADATA_MAX_REQUESTS", &cfg.IngestMetadataMaxRequests); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_INGEST_METADATA_MAX_BYTES", &cfg.IngestMetadataMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_INGEST_METADATA_FLUSH_WORKERS", &cfg.IngestMetadataFlushWorkers); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_INGEST_SHUTDOWN_TIMEOUT", &cfg.IngestShutdownTimeout); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_SLOW_QUERY_THRESHOLD", &cfg.SlowQueryThreshold); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_INDEX_HEALTH_INTERVAL", &cfg.IndexHealthInterval); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_MAINTENANCE_INTERVAL", &cfg.MaintenanceInterval); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_TENANT_USAGE_CACHE_TTL", &cfg.TenantUsageCacheTTL); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_READER_CATCHUP_TIMEOUT", &cfg.ReaderCatchupTimeout); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_READINESS_TIMEOUT", &cfg.ReadinessTimeout); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_READER_INDEX_CACHE_ENTRIES", &cfg.ReaderIndexCacheEntries); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_READER_INDEX_CACHE_MAX_BYTES", &cfg.ReaderIndexCacheMaxBytes); err != nil {
		return Config{}, err
	}
	cfg.ReaderIndexCacheDir = strings.TrimSpace(os.Getenv("GRAPHDB_READER_INDEX_CACHE_DIR"))
	if err := loadBoolEnv("GRAPHDB_INDEX_ENTITY_RECORDS", &cfg.IndexEntityRecords); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES", &cfg.EntityPagePackMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_FAULT_OBJECT_READ_DELAY", &cfg.FaultObjectReadDelay); err != nil {
		return Config{}, err
	}
	if err := loadBoolEnv("GRAPHDB_OTLP_INSECURE", &cfg.OTLPInsecure); err != nil {
		return Config{}, err
	}
	if cfg.StoreKind == "" {
		if cfg.S3Bucket != "" {
			cfg.StoreKind = "s3"
		} else {
			cfg.StoreKind = "local"
		}
	}
	prefix, err := normalizeObjectPrefix(cfg.Prefix)
	if err != nil {
		return Config{}, err
	}
	cfg.Prefix = prefix
	if cfg.ReaderIndexCacheDir == "" && cfg.DataDir != "" {
		cfg.ReaderIndexCacheDir = filepath.Join(cfg.DataDir, "cache", "index-objects")
	}
	if cfg.IngestWALDir == "" && cfg.DataDir != "" {
		cfg.IngestWALDir = filepath.Join(cfg.DataDir, "wal", "ingest")
	}
	switch cfg.Mode {
	case "all", "writer", "reader":
	default:
		return Config{}, fmt.Errorf("unsupported GRAPHDB_MODE %q", cfg.Mode)
	}
	if err := cfg.validateObjectStore(); err != nil {
		return Config{}, err
	}
	if err := cfg.validateCoordination(); err != nil {
		return Config{}, err
	}
	if err := cfg.validateIngest(); err != nil {
		return Config{}, err
	}
	if cfg.PprofEnabled && cfg.AdminAddr == "" {
		return Config{}, fmt.Errorf("GRAPHDB_ADMIN_ADDR is required when GRAPHDB_PPROF_ENABLED=true")
	}
	if cfg.AdminAddr != "" && cfg.AdminAddr == cfg.Addr {
		return Config{}, fmt.Errorf("GRAPHDB_ADMIN_ADDR must differ from GRAPHDB_ADDR")
	}
	return cfg, nil
}

func (cfg Config) validateIngest() error {
	switch cfg.IngestMetadataMode {
	case storage.IngestMetadataModeLegacy:
	case storage.IngestMetadataModeSegment:
		if cfg.IngestMode != "wal" {
			return fmt.Errorf("GRAPHDB_INGEST_METADATA_MODE=segment requires GRAPHDB_INGEST_MODE=wal")
		}
	default:
		return fmt.Errorf("unsupported GRAPHDB_INGEST_METADATA_MODE %q", cfg.IngestMetadataMode)
	}
	switch cfg.IngestMode {
	case "direct":
		return nil
	case "wal":
	default:
		return fmt.Errorf("unsupported GRAPHDB_INGEST_MODE %q", cfg.IngestMode)
	}
	if cfg.coordinationMode() != storage.CoordinationLocal {
		return fmt.Errorf("GRAPHDB_INGEST_MODE=wal requires GRAPHDB_COORDINATION=local")
	}
	if cfg.Mode == "reader" {
		return fmt.Errorf("GRAPHDB_INGEST_MODE=wal is unavailable in reader mode")
	}
	if err := cfg.IngestServiceConfig().Validate(); err != nil {
		return err
	}
	if cfg.IngestWALDurability != storage.IngestWALDurabilitySync {
		return fmt.Errorf("GRAPHDB_INGEST_WAL_DURABILITY=sync is required for durable 202 responses")
	}
	return nil
}

func (cfg Config) IngestServiceConfig() storage.IngestServiceConfig {
	return storage.IngestServiceConfig{
		WAL: storage.IngestWALConfig{
			Dir:           cfg.IngestWALDir,
			Durability:    cfg.IngestWALDurability,
			BufferBytes:   int(cfg.IngestWALBufferBytes),
			FsyncInterval: cfg.IngestWALFsyncInterval,
			MaxBytes:      cfg.IngestWALMaxBytes,
			SegmentBytes:  cfg.IngestWALSegmentBytes,
			AppendQueue:   cfg.IngestWALAppendQueue,
		},
		QueueMemoryBytes:   cfg.IngestQueueMemoryBytes,
		QueueHighWatermark: cfg.IngestQueueHighWatermark,
		WALHighWatermark:   cfg.IngestWALHighWatermark,
		WALStopWatermark:   cfg.IngestWALStopWatermark,
		MaxPendingAge:      cfg.IngestMaxPendingAge,
		FlushInterval:      cfg.IngestFlushInterval,
		FlushMaxRequests:   cfg.IngestFlushMaxRequests,
		FlushMaxBytes:      cfg.IngestFlushMaxBytes,
		FlushWorkers:       cfg.IngestFlushWorkers,
		Metadata: storage.IngestMetadataConfig{
			Mode:          cfg.IngestMetadataMode,
			FlushInterval: cfg.IngestMetadataFlushInterval,
			MaxRequests:   cfg.IngestMetadataMaxRequests,
			MaxBytes:      cfg.IngestMetadataMaxBytes,
			FlushWorkers:  cfg.IngestMetadataFlushWorkers,
		},
		FlushTimeout:  cfg.WriteExecutionTimeout,
		RetryInterval: time.Second,
	}
}

func NewCoordinator(ctx context.Context, cfg Config) (storage.WriteCoordinator, error) {
	if cfg.coordinationMode() == storage.CoordinationLocal {
		return nil, nil
	}
	return storage.NewPostgresCoordinator(
		ctx,
		cfg.PostgresDSN,
		cfg.PostgresSchema,
		cfg.CoordinatorNamespace,
	)
}

func (cfg Config) validateCoordination() error {
	switch cfg.coordinationMode() {
	case storage.CoordinationLocal:
		return nil
	case storage.CoordinationPostgres:
	default:
		return fmt.Errorf("unsupported GRAPHDB_COORDINATION %q", cfg.Coordination)
	}
	if cfg.PostgresDSN == "" {
		return fmt.Errorf("GRAPHDB_POSTGRES_DSN is required when GRAPHDB_COORDINATION=postgres")
	}
	if cfg.CoordinatorNamespace == "" {
		return fmt.Errorf("GRAPHDB_COORDINATOR_NAMESPACE is required when GRAPHDB_COORDINATION=postgres")
	}
	if cfg.StoreKind != "s3" {
		return fmt.Errorf("GRAPHDB_COORDINATION=postgres requires GRAPHDB_STORAGE=s3")
	}
	if cfg.objectProvider() != storage.ObjectProviderGenericS3 {
		return fmt.Errorf("GRAPHDB_COORDINATION=postgres requires S3_PROVIDER=%s", storage.ObjectProviderGenericS3)
	}
	if cfg.writerTopology() != storage.WriterTopologyCAS {
		return fmt.Errorf("GRAPHDB_COORDINATION=postgres requires GRAPHDB_WRITER_TOPOLOGY=%s", storage.WriterTopologyCAS)
	}
	if cfg.WriteExecutionTimeout <= 0 {
		return fmt.Errorf("GRAPHDB_WRITE_EXECUTION_TIMEOUT must be > 0 when GRAPHDB_COORDINATION=postgres")
	}
	if cfg.CoordinatorPendingReservationTTL <= cfg.WriteExecutionTimeout {
		return fmt.Errorf(
			"GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL must be greater than GRAPHDB_WRITE_EXECUTION_TIMEOUT when GRAPHDB_COORDINATION=postgres",
		)
	}
	return nil
}

func (cfg Config) coordinationMode() string {
	return normalizeCoordination(cfg.Coordination)
}

func normalizeCoordination(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return storage.CoordinationLocal
	}
	return value
}

func (cfg Config) BackpressureConfig() storage.BackpressureConfig {
	return storage.BackpressureConfig{
		ObjectLatencyThreshold: cfg.WriteObjectLatencyThreshold,
		ObjectErrorWindow:      cfg.WriteObjectErrorWindow,
		ObjectErrorThreshold:   cfg.WriteObjectErrorThreshold,
		CASConflictWindow:      cfg.WriteCASConflictWindow,
		CASConflictThreshold:   cfg.WriteCASConflictThreshold,
		MaxCommitTail:          cfg.WriteMaxCommitTail,
		MaxObjectsPerTenant:    cfg.WriteMaxObjectsPerTenant,
		MaxBytesPerTenant:      cfg.WriteMaxBytesPerTenant,
		MaxEntitiesPerTenant:   cfg.WriteMaxEntitiesPerTenant,
		MaxEdgesPerTenant:      cfg.WriteMaxEdgesPerTenant,
		RetryAfter:             2 * time.Second,
	}
}

func (cfg Config) WriterObjectCacheConfig() storage.WriterObjectCacheConfig {
	return storage.WriterObjectCacheConfig{
		MaxBytes:    cfg.WriterObjectCacheMaxBytes,
		MaxKeys:     cfg.WriterObjectCacheMaxKeys,
		NegativeTTL: cfg.WriterObjectCacheNegativeTTL,
	}
}

func (cfg Config) CoordinatorCleanupConfig() storage.CoordinatorCleanupConfig {
	return storage.CoordinatorCleanupConfig{
		IdempotencyRetention:  cfg.CoordinatorIdempotencyRetention,
		PendingReservationTTL: cfg.CoordinatorPendingReservationTTL,
		OutboxRetention:       cfg.CoordinatorOutboxRetention,
		Interval:              cfg.CoordinatorCleanupInterval,
		BatchSize:             cfg.CoordinatorCleanupBatchSize,
	}
}

func loadBoolEnv(key string, target *bool) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		*target = true
	case "0", "false", "no", "n", "off":
		*target = false
	default:
		return fmt.Errorf("%s must be a boolean", key)
	}
	return nil
}

func loadIntEnv(key string, target *int) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	*target = value
	return nil
}

func loadInt64Env(key string, target *int64) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	*target = value
	return nil
}

func loadBytesEnv(key string, target *int64) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	value, err := parseBytes(raw)
	if err != nil {
		return fmt.Errorf("%s must be a byte size: %w", key, err)
	}
	if value < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	*target = value
	return nil
}

func loadDurationEnv(key string, target *time.Duration) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s must be a duration: %w", key, err)
	}
	if value < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	*target = value
	return nil
}

func parseBytes(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("empty value")
	}
	lower := strings.ToLower(value)
	multipliers := []struct {
		suffix string
		mul    int64
	}{
		{"kib", 1024},
		{"mib", 1024 * 1024},
		{"gib", 1024 * 1024 * 1024},
		{"kb", 1000},
		{"mb", 1000 * 1000},
		{"gb", 1000 * 1000 * 1000},
		{"b", 1},
	}
	for _, multiplier := range multipliers {
		if strings.HasSuffix(lower, multiplier.suffix) {
			number := strings.TrimSpace(value[:len(value)-len(multiplier.suffix)])
			parsed, err := strconv.ParseInt(number, 10, 64)
			if err != nil {
				return 0, err
			}
			return parsed * multiplier.mul, nil
		}
	}
	return strconv.ParseInt(value, 10, 64)
}

func NewObjectStore(cfg Config) (storage.ObjectStore, error) {
	if err := cfg.validateObjectStore(); err != nil {
		return nil, err
	}
	switch cfg.StoreKind {
	case "local":
		return storage.NewFileStore(cfg.DataDir), nil
	case "s3":
		options := storage.S3Options{PathStyle: cfg.S3PathStyle}
		switch cfg.objectProvider() {
		case storage.ObjectProviderGenericS3:
			return storage.NewS3StoreWithOptions(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3Region, cfg.S3AccessKeyID, cfg.S3SecretAccessKey, options)
		case storage.ObjectProviderAliyunOSS:
			objects, err := storage.NewAliyunOSSStore(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3Region, cfg.S3AccessKeyID, cfg.S3SecretAccessKey, options)
			if err != nil {
				return nil, err
			}
			return storage.NewSingleWriterObjectStore(objects), nil
		case storage.ObjectProviderHuaweiOBS:
			objects, err := storage.NewHuaweiOBSStore(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3Region, cfg.S3AccessKeyID, cfg.S3SecretAccessKey, options)
			if err != nil {
				return nil, err
			}
			return storage.NewSingleWriterObjectStore(objects), nil
		case storage.ObjectProviderTencentCOS:
			objects, err := storage.NewTencentCOSStore(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3Region, cfg.S3AccessKeyID, cfg.S3SecretAccessKey, options)
			if err != nil {
				return nil, err
			}
			return storage.NewSingleWriterObjectStore(objects), nil
		}
		return nil, fmt.Errorf("unsupported S3_PROVIDER %q", cfg.S3Provider)
	default:
		return nil, fmt.Errorf("unsupported GRAPHDB_STORAGE %q", cfg.StoreKind)
	}
}

func (cfg Config) validateObjectStore() error {
	if cfg.StoreKind != "s3" {
		return nil
	}
	provider := cfg.objectProvider()
	if !storage.IsKnownObjectProvider(provider) {
		return fmt.Errorf("unsupported S3_PROVIDER %q", cfg.S3Provider)
	}
	topology := cfg.writerTopology()
	switch topology {
	case storage.WriterTopologyCAS, storage.WriterTopologySingle:
	default:
		return fmt.Errorf("unsupported GRAPHDB_WRITER_TOPOLOGY %q", cfg.WriterTopology)
	}
	if !storage.IsNativeObjectProvider(provider) {
		return nil
	}
	if topology != storage.WriterTopologySingle {
		return fmt.Errorf("S3_PROVIDER %q requires GRAPHDB_WRITER_TOPOLOGY=%s", provider, storage.WriterTopologySingle)
	}
	if strings.ToLower(strings.TrimSpace(cfg.S3Versioning)) != storage.BucketVersioningDisabled {
		return fmt.Errorf("S3_PROVIDER %q requires S3_VERSIONING=%s", provider, storage.BucketVersioningDisabled)
	}
	if provider == storage.ObjectProviderTencentCOS && cfg.S3PathStyle {
		return fmt.Errorf("S3_PROVIDER %q does not support S3_PATH_STYLE=true", provider)
	}
	return nil
}

func (cfg Config) objectProvider() string {
	return storage.NormalizeObjectProvider(cfg.S3Provider)
}

func (cfg Config) writerTopology() string {
	return normalizeWriterTopology(cfg.WriterTopology)
}

func normalizeWriterTopology(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return storage.WriterTopologyCAS
	}
	return value
}

func normalizeObjectPrefix(prefix string) (string, error) {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return "", nil
	}
	if strings.Contains(prefix, "\\") {
		return "", fmt.Errorf("GRAPHDB_PREFIX contains invalid path separator")
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("GRAPHDB_PREFIX contains invalid path segment %q", part)
		}
	}
	return prefix, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
