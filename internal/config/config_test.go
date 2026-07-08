package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadRejectsNegativeQueryAdmissionLimits(t *testing.T) {
	cases := []string{
		"GRAPHDB_QUERY_MAX_CONCURRENT",
		"GRAPHDB_QUERY_MAX_PER_TENANT",
		"GRAPHDB_READ_MAX_CONCURRENT",
		"GRAPHDB_READ_MAX_PER_TENANT",
		"GRAPHDB_READ_OBJECT_MAX_CONCURRENT",
		"GRAPHDB_WRITE_MAX_CONCURRENT",
		"GRAPHDB_WRITE_MAX_PER_TENANT",
		"GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD",
		"GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD",
		"GRAPHDB_WRITE_MAX_COMMIT_TAIL",
		"GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT",
		"GRAPHDB_WRITE_MAX_BYTES_PER_TENANT",
		"GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT",
		"GRAPHDB_WRITE_MAX_EDGES_PER_TENANT",
		"GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES",
		"GRAPHDB_WRITER_OBJECT_CACHE_MAX_KEYS",
		"GRAPHDB_READER_INDEX_CACHE_ENTRIES",
	}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			setLocalConfigEnv(t)
			t.Setenv(key, "-1")
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), key+" must be >= 0") {
				t.Fatalf("Load err = %v, want %s validation", err, key)
			}
		})
	}
}

func TestLoadAllowsZeroQueryAdmissionLimits(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_QUERY_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_QUERY_MAX_PER_TENANT", "0")
	t.Setenv("GRAPHDB_READ_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_READ_MAX_PER_TENANT", "0")
	t.Setenv("GRAPHDB_READ_OBJECT_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_WRITE_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_WRITE_MAX_PER_TENANT", "0")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD", "0")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD", "0")
	t.Setenv("GRAPHDB_WRITE_MAX_COMMIT_TAIL", "0")
	t.Setenv("GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT", "0")
	t.Setenv("GRAPHDB_WRITE_MAX_BYTES_PER_TENANT", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.QueryMaxConcurrent != 0 || cfg.QueryMaxPerTenant != 0 {
		t.Fatalf("query limits = %d/%d, want 0/0", cfg.QueryMaxConcurrent, cfg.QueryMaxPerTenant)
	}
	if cfg.ReadMaxConcurrent != 0 || cfg.ReadMaxPerTenant != 0 || cfg.ReadObjectMaxConcurrent != 0 {
		t.Fatalf("read limits = %#v, want zeros", cfg)
	}
	if cfg.WriteMaxConcurrent != 0 || cfg.WriteMaxPerTenant != 0 || cfg.WriteObjectErrorThreshold != 0 || cfg.WriteCASConflictThreshold != 0 || cfg.WriteMaxCommitTail != 0 || cfg.WriteMaxObjectsPerTenant != 0 || cfg.WriteMaxBytesPerTenant != 0 {
		t.Fatalf("write limits = %#v, want zeros", cfg)
	}
}

func TestLoadRejectsNegativeDurations(t *testing.T) {
	cases := []string{
		"GRAPHDB_POLL_INTERVAL",
		"GRAPHDB_QUERY_QUEUE_TIMEOUT",
		"GRAPHDB_READ_QUEUE_TIMEOUT",
		"GRAPHDB_WRITE_QUEUE_TIMEOUT",
		"GRAPHDB_WRITE_EXECUTION_TIMEOUT",
		"GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD",
		"GRAPHDB_WRITE_OBJECT_ERROR_WINDOW",
		"GRAPHDB_WRITE_CAS_CONFLICT_WINDOW",
		"GRAPHDB_MAINTENANCE_INTERVAL",
		"GRAPHDB_TENANT_USAGE_CACHE_TTL",
		"GRAPHDB_READER_CATCHUP_TIMEOUT",
		"GRAPHDB_FAULT_OBJECT_READ_DELAY",
		"GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL",
	}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			setLocalConfigEnv(t)
			t.Setenv(key, "-1s")
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), key+" must be >= 0") {
				t.Fatalf("Load err = %v, want %s validation", err, key)
			}
		})
	}
}

func TestLoadAllowsZeroDurations(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_POLL_INTERVAL", "0")
	t.Setenv("GRAPHDB_QUERY_QUEUE_TIMEOUT", "0")
	t.Setenv("GRAPHDB_READ_QUEUE_TIMEOUT", "0")
	t.Setenv("GRAPHDB_WRITE_QUEUE_TIMEOUT", "0")
	t.Setenv("GRAPHDB_WRITE_EXECUTION_TIMEOUT", "0")
	t.Setenv("GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD", "0")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_WINDOW", "0")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_WINDOW", "0")
	t.Setenv("GRAPHDB_MAINTENANCE_INTERVAL", "0")
	t.Setenv("GRAPHDB_TENANT_USAGE_CACHE_TTL", "0")
	t.Setenv("GRAPHDB_READER_CATCHUP_TIMEOUT", "0")
	t.Setenv("GRAPHDB_FAULT_OBJECT_READ_DELAY", "0")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval != 0 || cfg.QueryQueueTimeout != 0 || cfg.ReadQueueTimeout != 0 {
		t.Fatalf("durations = %s/%s/%s, want 0/0/0", cfg.PollInterval, cfg.QueryQueueTimeout, cfg.ReadQueueTimeout)
	}
	if cfg.WriteQueueTimeout != 0 || cfg.WriteExecutionTimeout != 0 || cfg.WriteObjectLatencyThreshold != 0 || cfg.WriteObjectErrorWindow != 0 || cfg.WriteCASConflictWindow != 0 {
		t.Fatalf("write durations = %s/%s/%s/%s/%s, want 0/0/0/0/0", cfg.WriteQueueTimeout, cfg.WriteExecutionTimeout, cfg.WriteObjectLatencyThreshold, cfg.WriteObjectErrorWindow, cfg.WriteCASConflictWindow)
	}
	if cfg.MaintenanceInterval != 0 {
		t.Fatalf("maintenance interval = %s, want 0", cfg.MaintenanceInterval)
	}
	if cfg.TenantUsageCacheTTL != 0 {
		t.Fatalf("tenant usage cache ttl = %s, want 0", cfg.TenantUsageCacheTTL)
	}
	if cfg.ReaderCatchupTimeout != 0 {
		t.Fatalf("reader catchup timeout = %s, want 0", cfg.ReaderCatchupTimeout)
	}
	if cfg.WriterObjectCacheNegativeTTL != 0 {
		t.Fatalf("writer object cache negative ttl = %s, want 0", cfg.WriterObjectCacheNegativeTTL)
	}
}

func TestLoadParsesPositiveDurations(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_POLL_INTERVAL", "500ms")
	t.Setenv("GRAPHDB_QUERY_QUEUE_TIMEOUT", "3s")
	t.Setenv("GRAPHDB_READ_QUEUE_TIMEOUT", "250ms")
	t.Setenv("GRAPHDB_WRITE_QUEUE_TIMEOUT", "150ms")
	t.Setenv("GRAPHDB_WRITE_EXECUTION_TIMEOUT", "9s")
	t.Setenv("GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD", "750ms")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_WINDOW", "6s")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_WINDOW", "4s")
	t.Setenv("GRAPHDB_MAINTENANCE_INTERVAL", "7s")
	t.Setenv("GRAPHDB_TENANT_USAGE_CACHE_TTL", "45s")
	t.Setenv("GRAPHDB_READER_CATCHUP_TIMEOUT", "850ms")
	t.Setenv("GRAPHDB_FAULT_OBJECT_READ_DELAY", "25ms")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL", "3m")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval != 500*time.Millisecond || cfg.QueryQueueTimeout != 3*time.Second || cfg.ReadQueueTimeout != 250*time.Millisecond {
		t.Fatalf("durations = %s/%s/%s", cfg.PollInterval, cfg.QueryQueueTimeout, cfg.ReadQueueTimeout)
	}
	if cfg.WriteQueueTimeout != 150*time.Millisecond || cfg.WriteExecutionTimeout != 9*time.Second || cfg.WriteObjectLatencyThreshold != 750*time.Millisecond || cfg.WriteObjectErrorWindow != 6*time.Second || cfg.WriteCASConflictWindow != 4*time.Second {
		t.Fatalf("write durations = %s/%s/%s/%s/%s", cfg.WriteQueueTimeout, cfg.WriteExecutionTimeout, cfg.WriteObjectLatencyThreshold, cfg.WriteObjectErrorWindow, cfg.WriteCASConflictWindow)
	}
	if cfg.MaintenanceInterval != 7*time.Second {
		t.Fatalf("maintenance interval = %s, want 7s", cfg.MaintenanceInterval)
	}
	if cfg.TenantUsageCacheTTL != 45*time.Second {
		t.Fatalf("tenant usage cache ttl = %s, want 45s", cfg.TenantUsageCacheTTL)
	}
	if cfg.ReaderCatchupTimeout != 850*time.Millisecond {
		t.Fatalf("reader catchup timeout = %s, want 850ms", cfg.ReaderCatchupTimeout)
	}
	if cfg.FaultObjectReadDelay != 25*time.Millisecond {
		t.Fatalf("fault read delay = %s, want 25ms", cfg.FaultObjectReadDelay)
	}
	if cfg.WriterObjectCacheNegativeTTL != 3*time.Minute {
		t.Fatalf("writer object cache negative ttl = %s, want 3m", cfg.WriterObjectCacheNegativeTTL)
	}
}

func TestLoadParsesWriteBackpressureConfig(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITE_MAX_CONCURRENT", "9")
	t.Setenv("GRAPHDB_WRITE_MAX_PER_TENANT", "1")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD", "3")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD", "7")
	t.Setenv("GRAPHDB_WRITE_MAX_COMMIT_TAIL", "11")
	t.Setenv("GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT", "303")
	t.Setenv("GRAPHDB_WRITE_MAX_BYTES_PER_TENANT", "4040")
	t.Setenv("GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT", "101")
	t.Setenv("GRAPHDB_WRITE_MAX_EDGES_PER_TENANT", "202")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WriteMaxConcurrent != 9 || cfg.WriteMaxPerTenant != 1 || cfg.WriteObjectErrorThreshold != 3 || cfg.WriteCASConflictThreshold != 7 || cfg.WriteMaxCommitTail != 11 {
		t.Fatalf("write limits = %#v", cfg)
	}
	backpressure := cfg.BackpressureConfig()
	if backpressure.MaxObjectsPerTenant != 303 || backpressure.MaxBytesPerTenant != 4040 || backpressure.MaxEntitiesPerTenant != 101 || backpressure.MaxEdgesPerTenant != 202 {
		t.Fatalf("backpressure config = %#v", backpressure)
	}
}

func TestLoadRejectsWriteMaxPerTenantAboveOne(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITE_MAX_PER_TENANT", "2")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_WRITE_MAX_PER_TENANT must be 0 or 1") {
		t.Fatalf("Load err = %v, want single-writer validation", err)
	}
}

func TestLoadParsesWriterObjectCacheConfig(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE", "false")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES", "64MiB")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_KEYS", "1234")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL", "45s")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WriterObjectCache {
		t.Fatalf("writer object cache enabled = true, want false")
	}
	if cfg.WriterObjectCacheMaxBytes != 64*1024*1024 || cfg.WriterObjectCacheMaxKeys != 1234 || cfg.WriterObjectCacheNegativeTTL != 45*time.Second {
		t.Fatalf("writer object cache config = %#v", cfg.WriterObjectCacheConfig())
	}
}

func TestLoadRejectsInvalidWriterObjectCacheToggle(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE", "maybe")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_WRITER_OBJECT_CACHE must be a boolean") {
		t.Fatalf("Load err = %v, want writer object cache bool validation", err)
	}
}

func TestLoadRejectsInvalidWriterObjectCacheBytes(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES", "64XB")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES must be a byte size") {
		t.Fatalf("Load err = %v, want writer object cache byte validation", err)
	}
}

func TestLoadParsesReaderIndexCacheConfig(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_READ_MAX_CONCURRENT", "17")
	t.Setenv("GRAPHDB_READ_MAX_PER_TENANT", "5")
	t.Setenv("GRAPHDB_READ_OBJECT_MAX_CONCURRENT", "23")
	t.Setenv("GRAPHDB_READ_OBJECT_SINGLEFLIGHT", "false")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_ENTRIES", "123")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_DIR", "/tmp/graphdb-index-cache")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ReaderIndexCacheEntries != 123 || cfg.ReaderIndexCacheDir != "/tmp/graphdb-index-cache" {
		t.Fatalf("reader index cache config = %#v", cfg)
	}
	if cfg.ReadMaxConcurrent != 17 || cfg.ReadMaxPerTenant != 5 || cfg.ReadObjectMaxConcurrent != 23 || cfg.ReadObjectSingleflight {
		t.Fatalf("reader read-path config = %#v", cfg)
	}
}

func TestLoadParsesIndexEntityRecordsConfig(t *testing.T) {
	setLocalConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load default: %v", err)
	}
	if cfg.IndexEntityRecords {
		t.Fatal("index entity records default enabled = true, want false")
	}

	t.Setenv("GRAPHDB_INDEX_ENTITY_RECORDS", "true")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load enabled: %v", err)
	}
	if !cfg.IndexEntityRecords {
		t.Fatal("index entity records enabled = false, want true")
	}
}

func TestLoadParsesIngestCollectorStatusMaterializedConfig(t *testing.T) {
	setLocalConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load default: %v", err)
	}
	if cfg.IngestCollectorStatusMaterialized {
		t.Fatal("ingest collector status materialized default enabled = true, want false")
	}

	t.Setenv("GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED", "true")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load enabled: %v", err)
	}
	if !cfg.IngestCollectorStatusMaterialized {
		t.Fatal("ingest collector status materialized enabled = false, want true")
	}
}

func TestLoadRejectsInvalidIngestCollectorStatusMaterializedToggle(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED", "maybe")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED must be a boolean") {
		t.Fatalf("Load err = %v, want ingest collector status bool validation", err)
	}
}

func TestLoadRejectsInvalidIndexEntityRecordsToggle(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_INDEX_ENTITY_RECORDS", "maybe")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_INDEX_ENTITY_RECORDS must be a boolean") {
		t.Fatalf("Load err = %v, want index entity records bool validation", err)
	}
}

func TestLoadParsesObservabilityConfig(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_SLOW_QUERY_THRESHOLD", "250ms")
	t.Setenv("GRAPHDB_INDEX_HEALTH_INTERVAL", "5s")
	t.Setenv("GRAPHDB_MAINTENANCE_INTERVAL", "9s")
	t.Setenv("GRAPHDB_TENANT_USAGE_CACHE_TTL", "11s")
	t.Setenv("GRAPHDB_OTLP_ENDPOINT", "http://otel:4318/v1/traces")
	t.Setenv("GRAPHDB_OTLP_INSECURE", "true")
	t.Setenv("GRAPHDB_SERVICE_NAME", "graphdb-test")
	t.Setenv("GRAPHDB_INSTANCE_ID", "reader-a")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SlowQueryThreshold != 250*time.Millisecond || cfg.IndexHealthInterval != 5*time.Second || cfg.MaintenanceInterval != 9*time.Second {
		t.Fatalf("observability durations = %s/%s/%s", cfg.SlowQueryThreshold, cfg.IndexHealthInterval, cfg.MaintenanceInterval)
	}
	if cfg.TenantUsageCacheTTL != 11*time.Second {
		t.Fatalf("tenant usage cache ttl = %s, want 11s", cfg.TenantUsageCacheTTL)
	}
	if cfg.OTLPEndpoint != "http://otel:4318/v1/traces" || !cfg.OTLPInsecure || cfg.ServiceName != "graphdb-test" {
		t.Fatalf("otlp config = %#v", cfg)
	}
	if cfg.InstanceID != "reader-a" {
		t.Fatalf("instance id = %q, want reader-a", cfg.InstanceID)
	}
}

func TestLoadRejectsInvalidOTLPInsecure(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_OTLP_INSECURE", "sometimes")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_OTLP_INSECURE must be a boolean") {
		t.Fatalf("Load err = %v, want OTLP bool validation", err)
	}
}

func TestLoadNormalizesObjectPrefix(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_PREFIX", " /prod/blue/ ")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Prefix != "prod/blue" {
		t.Fatalf("prefix = %q, want prod/blue", cfg.Prefix)
	}
}

func TestLoadRejectsAmbiguousObjectPrefix(t *testing.T) {
	cases := []string{
		".",
		"prod/../blue",
		"prod//blue",
		`prod\blue`,
	}
	for _, value := range cases {
		t.Run(value, func(t *testing.T) {
			setLocalConfigEnv(t)
			t.Setenv("GRAPHDB_PREFIX", value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "GRAPHDB_PREFIX") {
				t.Fatalf("Load err = %v, want GRAPHDB_PREFIX validation", err)
			}
		})
	}
}

func setLocalConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GRAPHDB_STORAGE", "local")
	t.Setenv("GRAPHDB_MODE", "all")
	t.Setenv("GRAPHDB_PREFIX", "")
	t.Setenv("GRAPHDB_QUERY_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_QUERY_MAX_PER_TENANT", "")
	t.Setenv("GRAPHDB_QUERY_QUEUE_TIMEOUT", "")
	t.Setenv("GRAPHDB_READ_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_READ_MAX_PER_TENANT", "")
	t.Setenv("GRAPHDB_READ_QUEUE_TIMEOUT", "")
	t.Setenv("GRAPHDB_READ_OBJECT_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_READ_OBJECT_SINGLEFLIGHT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_QUEUE_TIMEOUT", "")
	t.Setenv("GRAPHDB_WRITE_EXECUTION_TIMEOUT", "")
	t.Setenv("GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD", "")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_WINDOW", "")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD", "")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_WINDOW", "")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD", "")
	t.Setenv("GRAPHDB_WRITE_MAX_COMMIT_TAIL", "")
	t.Setenv("GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_BYTES_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_EDGES_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_KEYS", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL", "")
	t.Setenv("GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED", "")
	t.Setenv("GRAPHDB_POLL_INTERVAL", "")
	t.Setenv("GRAPHDB_SLOW_QUERY_THRESHOLD", "")
	t.Setenv("GRAPHDB_INDEX_HEALTH_INTERVAL", "")
	t.Setenv("GRAPHDB_MAINTENANCE_INTERVAL", "")
	t.Setenv("GRAPHDB_TENANT_USAGE_CACHE_TTL", "")
	t.Setenv("GRAPHDB_READER_CATCHUP_TIMEOUT", "")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_ENTRIES", "")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_DIR", "")
	t.Setenv("GRAPHDB_INDEX_ENTITY_RECORDS", "")
	t.Setenv("GRAPHDB_FAULT_OBJECT_READ_DELAY", "")
	t.Setenv("GRAPHDB_OTLP_ENDPOINT", "")
	t.Setenv("GRAPHDB_OTLP_INSECURE", "")
	t.Setenv("GRAPHDB_SERVICE_NAME", "")
	t.Setenv("GRAPHDB_INSTANCE_ID", "")
}
