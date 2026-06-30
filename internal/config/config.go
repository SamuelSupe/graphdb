package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"graphdb/internal/storage"
)

type Config struct {
	Addr                        string
	Mode                        string
	Prefix                      string
	PollInterval                time.Duration
	DataDir                     string
	StoreKind                   string
	QueryMaxConcurrent          int
	QueryMaxPerTenant           int
	QueryQueueTimeout           time.Duration
	ReadMaxConcurrent           int
	ReadMaxPerTenant            int
	ReadQueueTimeout            time.Duration
	ReadObjectMaxConcurrent     int
	ReadObjectSingleflight      bool
	WriteMaxConcurrent          int
	WriteMaxPerTenant           int
	WriteQueueTimeout           time.Duration
	WriteExecutionTimeout       time.Duration
	WriteObjectLatencyThreshold time.Duration
	WriteObjectErrorWindow      time.Duration
	WriteObjectErrorThreshold   int
	WriteCASConflictWindow      time.Duration
	WriteCASConflictThreshold   int
	WriteMaxCommitTail          int
	WriteMaxObjectsPerTenant    int
	WriteMaxBytesPerTenant      int64
	WriteMaxEntitiesPerTenant   int
	WriteMaxEdgesPerTenant      int
	SlowQueryThreshold          time.Duration
	IndexHealthInterval         time.Duration
	MaintenanceInterval         time.Duration
	TenantUsageCacheTTL         time.Duration
	ReaderCatchupTimeout        time.Duration
	ReaderIndexCacheEntries     int
	ReaderIndexCacheDir         string
	FaultObjectReadDelay        time.Duration
	OTLPEndpoint                string
	OTLPInsecure                bool
	ServiceName                 string
	InstanceID                  string

	S3Endpoint        string
	S3Bucket          string
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string
}

func Load() (Config, error) {
	cfg := Config{
		Addr:                        getenv("GRAPHDB_ADDR", ":8080"),
		Mode:                        getenv("GRAPHDB_MODE", "all"),
		Prefix:                      getenv("GRAPHDB_PREFIX", "graphdb"),
		PollInterval:                2 * time.Second,
		DataDir:                     getenv("GRAPHDB_DATA_DIR", ".graphdb"),
		StoreKind:                   os.Getenv("GRAPHDB_STORAGE"),
		QueryMaxConcurrent:          64,
		QueryMaxPerTenant:           32,
		QueryQueueTimeout:           5 * time.Second,
		ReadMaxConcurrent:           128,
		ReadMaxPerTenant:            64,
		ReadQueueTimeout:            500 * time.Millisecond,
		ReadObjectMaxConcurrent:     128,
		ReadObjectSingleflight:      true,
		WriteMaxConcurrent:          32,
		WriteMaxPerTenant:           1,
		WriteQueueTimeout:           2 * time.Second,
		WriteExecutionTimeout:       90 * time.Second,
		WriteObjectLatencyThreshold: 2 * time.Second,
		WriteObjectErrorWindow:      30 * time.Second,
		WriteObjectErrorThreshold:   1,
		WriteCASConflictWindow:      30 * time.Second,
		WriteCASConflictThreshold:   5,
		WriteMaxCommitTail:          300,
		SlowQueryThreshold:          500 * time.Millisecond,
		IndexHealthInterval:         30 * time.Second,
		MaintenanceInterval:         30 * time.Second,
		TenantUsageCacheTTL:         60 * time.Second,
		ReaderCatchupTimeout:        2 * time.Second,
		ReaderIndexCacheEntries:     4096,
		OTLPEndpoint:                os.Getenv("GRAPHDB_OTLP_ENDPOINT"),
		ServiceName:                 getenv("GRAPHDB_SERVICE_NAME", "graphdb"),
		InstanceID:                  strings.TrimSpace(os.Getenv("GRAPHDB_INSTANCE_ID")),
		S3Endpoint:                  os.Getenv("S3_ENDPOINT"),
		S3Bucket:                    os.Getenv("S3_BUCKET"),
		S3Region:                    getenv("S3_REGION", "us-east-1"),
	}
	cfg.S3AccessKeyID = firstNonEmpty(os.Getenv("S3_ACCESS_KEY_ID"), os.Getenv("AWS_ACCESS_KEY_ID"))
	cfg.S3SecretAccessKey = firstNonEmpty(os.Getenv("S3_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
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
	if err := loadIntEnv("GRAPHDB_READER_INDEX_CACHE_ENTRIES", &cfg.ReaderIndexCacheEntries); err != nil {
		return Config{}, err
	}
	cfg.ReaderIndexCacheDir = strings.TrimSpace(os.Getenv("GRAPHDB_READER_INDEX_CACHE_DIR"))
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
	switch cfg.Mode {
	case "all", "writer", "reader":
	default:
		return Config{}, fmt.Errorf("unsupported GRAPHDB_MODE %q", cfg.Mode)
	}
	if cfg.WriteMaxPerTenant > 1 {
		return Config{}, fmt.Errorf("GRAPHDB_WRITE_MAX_PER_TENANT must be 0 or 1 in single-writer mode")
	}
	return cfg, nil
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

func NewObjectStore(cfg Config) (storage.ObjectStore, error) {
	switch cfg.StoreKind {
	case "local":
		return storage.NewFileStore(cfg.DataDir), nil
	case "s3":
		return storage.NewS3Store(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3Region, cfg.S3AccessKeyID, cfg.S3SecretAccessKey)
	default:
		return nil, fmt.Errorf("unsupported GRAPHDB_STORAGE %q", cfg.StoreKind)
	}
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
