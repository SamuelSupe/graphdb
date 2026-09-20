package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/backupstore"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type Config struct {
	Backup                            backupstore.Config
	Addr                              string
	AdminAddr                         string
	PprofEnabled                      bool
	Mode                              string
	Prefix                            string
	PollInterval                      time.Duration
	ReaderCacheIdleTTL                time.Duration
	ReaderCacheMaxTenants             int
	ReaderCacheMaxBytes               int64
	ReaderCacheLoadTimeout            time.Duration
	ReaderCacheLoadMaxConcurrent      int
	ReaderCacheLoadQueueTimeout       time.Duration
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
	IngestFlushInterval               time.Duration
	IngestFlushMaxRequests            int
	IngestFlushMaxBytes               int64
	IngestFlushWorkers                int
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
}

func Load() (Config, error) {
	backup, err := loadBackupConfig()
	if err != nil {
		return Config{}, err
	}
	for _, env := range os.Environ() {
		key, value, _ := strings.Cut(env, "=")
		if value != "" && (strings.HasPrefix(key, "GRAPHDB_POSTGRES_") || strings.HasPrefix(key, "GRAPHDB_COORDINATOR_")) {
			return Config{}, fmt.Errorf("%s is unsupported in the local disk edition", key)
		}
	}
	cfg := Config{
		Backup:                            backup,
		Addr:                              getenv("GRAPHDB_ADDR", ":8080"),
		AdminAddr:                         strings.TrimSpace(os.Getenv("GRAPHDB_ADMIN_ADDR")),
		Mode:                              getenv("GRAPHDB_MODE", "all"),
		Prefix:                            getenv("GRAPHDB_PREFIX", "graphdb"),
		PollInterval:                      2 * time.Second,
		ReaderCacheIdleTTL:                15 * time.Minute,
		ReaderCacheMaxTenants:             64,
		ReaderCacheMaxBytes:               512 * 1024 * 1024,
		ReaderCacheLoadTimeout:            time.Minute,
		ReaderCacheLoadMaxConcurrent:      4,
		ReaderCacheLoadQueueTimeout:       2 * time.Second,
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
		WriteMaxCommitTail:                1500,
		WriteCacheMaxBytes:                512 * 1024 * 1024,
		WriterObjectCache:                 true,
		WriterObjectCacheMaxBytes:         512 * 1024 * 1024,
		WriterObjectCacheMaxKeys:          200000,
		WriterObjectCacheNegativeTTL:      5 * time.Minute,
		IngestCollectorStatusMaterialized: true,
		IngestMode:                        strings.ToLower(strings.TrimSpace(getenv("GRAPHDB_INGEST_MODE", "direct"))),
		IngestWALDurability:               strings.ToLower(strings.TrimSpace(getenv("GRAPHDB_INGEST_WAL_DURABILITY", storage.IngestWALDurabilitySync))),
		IngestWALBufferBytes:              4 * 1024 * 1024,
		IngestWALFsyncInterval:            5 * time.Millisecond,
		IngestWALMaxBytes:                 10 * 1024 * 1024 * 1024,
		IngestWALSegmentBytes:             256 * 1024 * 1024,
		IngestWALAppendQueue:              4096,
		IngestQueueMemoryBytes:            256 * 1024 * 1024,
		IngestFlushInterval:               10 * time.Second,
		IngestFlushMaxRequests:            256,
		IngestFlushMaxBytes:               8 * 1024 * 1024,
		IngestFlushWorkers:                1,
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
		PostgresSchema:                    strings.TrimSpace(os.Getenv("GRAPHDB_POSTGRES_SCHEMA")),
		CoordinatorNamespace:              strings.TrimSpace(os.Getenv("GRAPHDB_COORDINATOR_NAMESPACE")),
	}
	if err := loadBoolEnv("GRAPHDB_PPROF_ENABLED", &cfg.PprofEnabled); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_POLL_INTERVAL", &cfg.PollInterval); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_READER_CACHE_IDLE_TTL", &cfg.ReaderCacheIdleTTL); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_READER_CACHE_MAX_TENANTS", &cfg.ReaderCacheMaxTenants); err != nil {
		return Config{}, err
	}
	if err := loadBytesEnv("GRAPHDB_READER_CACHE_MAX_BYTES", &cfg.ReaderCacheMaxBytes); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_READER_CACHE_LOAD_TIMEOUT", &cfg.ReaderCacheLoadTimeout); err != nil {
		return Config{}, err
	}
	if err := loadIntEnv("GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT", &cfg.ReaderCacheLoadMaxConcurrent); err != nil {
		return Config{}, err
	}
	if err := loadDurationEnv("GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT", &cfg.ReaderCacheLoadQueueTimeout); err != nil {
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
		cfg.StoreKind = "local"
	}
	prefix, err := normalizeObjectPrefix(cfg.Prefix)
	if err != nil {
		return Config{}, err
	}
	cfg.Prefix = prefix

	if cfg.IngestWALDir == "" && cfg.DataDir != "" {
		cfg.IngestWALDir = filepath.Join(cfg.DataDir, "wal", "ingest")
	}
	switch cfg.Mode {
	case "all":
	default:
		return Config{}, fmt.Errorf("unsupported GRAPHDB_MODE %q", cfg.Mode)
	}
	if err := cfg.validateObjectStore(); err != nil {
		return Config{}, err
	}
	if err := cfg.ValidateCoordination(); err != nil {
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
	switch cfg.IngestMode {
	case "direct":
		return nil
	case "wal":
	default:
		return fmt.Errorf("unsupported GRAPHDB_INGEST_MODE %q", cfg.IngestMode)
	}
	if cfg.Mode == "reader" {
		return fmt.Errorf("GRAPHDB_INGEST_MODE=wal is unavailable in reader mode")
	}

	return cfg.IngestServiceConfig().Validate()
}

func (cfg Config) IngestServiceConfig() storage.IngestServiceConfig {
	return storage.IngestServiceConfig{
		OwnerID: cfg.InstanceID,
		WAL: storage.IngestWALConfig{
			Dir:           cfg.IngestWALDir,
			Durability:    cfg.IngestWALDurability,
			BufferBytes:   int(cfg.IngestWALBufferBytes),
			FsyncInterval: cfg.IngestWALFsyncInterval,
			MaxBytes:      cfg.IngestWALMaxBytes,
			SegmentBytes:  cfg.IngestWALSegmentBytes,
			AppendQueue:   cfg.IngestWALAppendQueue,
		},
		QueueMemoryBytes: cfg.IngestQueueMemoryBytes,
		FlushInterval:    cfg.IngestFlushInterval,
		FlushMaxRequests: cfg.IngestFlushMaxRequests,
		FlushMaxBytes:    cfg.IngestFlushMaxBytes,
		FlushWorkers:     cfg.IngestFlushWorkers,
		FlushTimeout:     cfg.WriteExecutionTimeout,
		RetryInterval:    time.Second,
	}
}

func (cfg Config) ValidateCoordination() error {
	if cfg.coordinationMode() != storage.CoordinationLocal {
		return fmt.Errorf("GRAPHDB_COORDINATION=%s is unsupported; local disk edition requires local", cfg.Coordination)
	}
	for _, key := range []string{"GRAPHDB_WRITE_CAS_MAX_RETRIES", "GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION", "GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL", "GRAPHDB_COORDINATOR_OUTBOX_RETENTION", "GRAPHDB_COORDINATOR_CLEANUP_INTERVAL", "GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE"} {
		if os.Getenv(key) != "" {
			return fmt.Errorf("%s is unsupported in the local disk edition", key)
		}
	}
	if cfg.PostgresDSN != "" || cfg.CoordinatorNamespace != "" || cfg.PostgresSchema != "" {
		return fmt.Errorf("PostgreSQL coordinator configuration is unsupported")
	}
	return nil
}

func (cfg Config) coordinationMode() string {
	return normalizeCoordination(cfg.Coordination)
}

func (cfg Config) CoordinationMode() string {
	return cfg.coordinationMode()
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

func (cfg Config) validateObjectStore() error {
	if cfg.StoreKind != "" && cfg.StoreKind != "local" {
		return fmt.Errorf("GRAPHDB_STORAGE=%s is unsupported; local disk edition requires local", cfg.StoreKind)
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		return fmt.Errorf("GRAPHDB_DATA_DIR is required")
	}
	for _, key := range []string{"S3_ENDPOINT", "S3_BUCKET", "S3_PROVIDER", "S3_PATH_STYLE", "S3_REGION", "S3_VERSIONING", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "GRAPHDB_WRITER_TOPOLOGY"} {
		if os.Getenv(key) != "" {
			return fmt.Errorf("%s is unsupported in the local disk edition", key)
		}
	}
	return nil
}

func (cfg Config) ValidateObjectStore() error {
	return cfg.validateObjectStore()
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
