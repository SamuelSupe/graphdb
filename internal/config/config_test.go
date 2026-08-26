package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestLoadRejectsNegativeQueryAdmissionLimits(t *testing.T) {
	cases := []string{
		"GRAPHDB_QUERY_MAX_CONCURRENT",
		"GRAPHDB_QUERY_MAX_PER_TENANT",
		"GRAPHDB_READ_MAX_CONCURRENT",
		"GRAPHDB_READ_MAX_PER_TENANT",
		"GRAPHDB_READ_OBJECT_MAX_CONCURRENT",
		"GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT",
		"GRAPHDB_WRITE_MAX_CONCURRENT",
		"GRAPHDB_WRITE_MAX_PER_TENANT",
		"GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD",
		"GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD",
		"GRAPHDB_WRITE_CAS_MAX_RETRIES",
		"GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE",
		"GRAPHDB_WRITE_MAX_COMMIT_TAIL",
		"GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT",
		"GRAPHDB_WRITE_MAX_BYTES_PER_TENANT",
		"GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT",
		"GRAPHDB_WRITE_MAX_EDGES_PER_TENANT",
		"GRAPHDB_WRITE_CACHE_MAX_BYTES",
		"GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES",
		"GRAPHDB_WRITER_OBJECT_CACHE_MAX_KEYS",
		"GRAPHDB_READER_INDEX_CACHE_ENTRIES",
		"GRAPHDB_READER_INDEX_CACHE_MAX_BYTES",
		"GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES",
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

func TestLoadKeepsPprofDisabledByDefault(t *testing.T) {
	setLocalConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PprofEnabled || cfg.AdminAddr != "" {
		t.Fatalf("admin defaults = addr %q pprof %t, want empty/false", cfg.AdminAddr, cfg.PprofEnabled)
	}
}

func TestLoadIngestWALDefaultsAndDeploymentBoundary(t *testing.T) {
	setLocalConfigEnv(t)
	dataDir := t.TempDir()
	t.Setenv("GRAPHDB_DATA_DIR", dataDir)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IngestWALDir != filepath.Join(dataDir, "wal", "ingest") ||
		cfg.WriteCacheMaxBytes != 4*1024*1024*1024 ||
		cfg.WriteMaxCommitTail != 20000 ||
		cfg.IngestWALDurability != storage.IngestWALDurabilitySync ||
		cfg.IngestWALBufferBytes != 4*1024*1024 ||
		cfg.IngestWALFsyncInterval != 3*time.Millisecond ||
		cfg.IngestMode != "wal" ||
		cfg.IngestFlushWorkers != 2 ||
		cfg.IngestFlushInterval != 250*time.Millisecond ||
		cfg.IngestFlushMaxRequests != 8 ||
		cfg.IngestFlushMaxBytes != 2*1024*1024 ||
		cfg.IngestQueueHighWatermark != 80 ||
		cfg.IngestWALHighWatermark != 70 ||
		cfg.IngestWALStopWatermark != 85 ||
		cfg.IngestMaxPendingAge != 2*time.Minute ||
		cfg.IngestMetadataMode != storage.IngestMetadataModeSegment ||
		cfg.IngestMetadataFlushInterval != 500*time.Millisecond ||
		cfg.IngestMetadataMaxRequests != 256 ||
		cfg.IngestMetadataMaxBytes != 8*1024*1024 ||
		cfg.IngestMetadataFlushWorkers != 2 {
		t.Fatalf("WAL defaults = %#v", cfg.IngestServiceConfig())
	}

	t.Run("segment", func(t *testing.T) {
		setLocalConfigEnv(t)
		t.Setenv("GRAPHDB_INGEST_MODE", "wal")
		t.Setenv("GRAPHDB_INGEST_METADATA_MODE", "segment")
		t.Setenv("GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL", "17s")
		t.Setenv("GRAPHDB_INGEST_METADATA_MAX_REQUESTS", "31")
		t.Setenv("GRAPHDB_INGEST_METADATA_MAX_BYTES", "3MiB")
		t.Setenv("GRAPHDB_INGEST_METADATA_FLUSH_WORKERS", "3")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.IngestMetadataMode != storage.IngestMetadataModeSegment ||
			cfg.IngestMetadataFlushInterval != 17*time.Second ||
			cfg.IngestMetadataMaxRequests != 31 ||
			cfg.IngestMetadataMaxBytes != 3*1024*1024 ||
			cfg.IngestMetadataFlushWorkers != 3 {
			t.Fatalf("segment metadata config = %#v", cfg.IngestServiceConfig().Metadata)
		}
	})
	t.Run("segment requires wal", func(t *testing.T) {
		setLocalConfigEnv(t)
		t.Setenv("GRAPHDB_INGEST_MODE", "direct")
		t.Setenv("GRAPHDB_INGEST_METADATA_MODE", "segment")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "requires GRAPHDB_INGEST_MODE=wal") {
			t.Fatalf("Load err = %v, want WAL boundary", err)
		}
	})

	t.Run("postgres", func(t *testing.T) {
		setPostgresConfigEnv(t)
		t.Setenv("GRAPHDB_INGEST_MODE", "")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "requires GRAPHDB_COORDINATION=local") {
			t.Fatalf("Load err = %v, want local coordination boundary", err)
		}
		t.Setenv("GRAPHDB_INGEST_MODE", "direct")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.IngestMode != "direct" || cfg.IngestMetadataMode != storage.IngestMetadataModeLegacy {
			t.Fatalf("postgres ingest defaults = mode %q metadata %q", cfg.IngestMode, cfg.IngestMetadataMode)
		}
	})
	t.Run("reader", func(t *testing.T) {
		setLocalConfigEnv(t)
		t.Setenv("GRAPHDB_MODE", "reader")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.IngestMode != "direct" || cfg.IngestMetadataMode != storage.IngestMetadataModeLegacy {
			t.Fatalf("reader ingest defaults = mode %q metadata %q", cfg.IngestMode, cfg.IngestMetadataMode)
		}
		t.Setenv("GRAPHDB_INGEST_MODE", "wal")
		t.Setenv("GRAPHDB_INGEST_METADATA_MODE", "segment")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "unavailable in reader mode") {
			t.Fatalf("Load err = %v, want reader boundary", err)
		}
	})
	t.Run("durability", func(t *testing.T) {
		setLocalConfigEnv(t)
		t.Setenv("GRAPHDB_INGEST_MODE", "wal")
		t.Setenv("GRAPHDB_INGEST_WAL_DURABILITY", "invalid")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "durability") {
			t.Fatalf("Load err = %v, want durability validation", err)
		}
	})
	t.Run("os durability is not a durable acceptance contract", func(t *testing.T) {
		setLocalConfigEnv(t)
		t.Setenv("GRAPHDB_INGEST_WAL_DURABILITY", storage.IngestWALDurabilityOS)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "sync is required") {
			t.Fatalf("Load err = %v, want durable 202 boundary", err)
		}
	})
}

func TestLoadRequiresSeparateAdminListenerForPprof(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_PPROF_ENABLED", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "GRAPHDB_ADMIN_ADDR is required") {
		t.Fatalf("Load err = %v, want admin addr validation", err)
	}

	t.Setenv("GRAPHDB_ADMIN_ADDR", "127.0.0.1:8081")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.PprofEnabled || cfg.AdminAddr != "127.0.0.1:8081" {
		t.Fatalf("admin config = addr %q pprof %t", cfg.AdminAddr, cfg.PprofEnabled)
	}
}

func TestLoadRejectsSharedDataAndAdminListener(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_ADDR", "127.0.0.1:8080")
	t.Setenv("GRAPHDB_ADMIN_ADDR", "127.0.0.1:8080")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("Load err = %v, want distinct listener validation", err)
	}
}

func TestLoadAllowsZeroQueryAdmissionLimits(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_QUERY_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_QUERY_MAX_PER_TENANT", "0")
	t.Setenv("GRAPHDB_READ_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_READ_MAX_PER_TENANT", "0")
	t.Setenv("GRAPHDB_READ_OBJECT_MAX_CONCURRENT", "0")
	t.Setenv("GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT", "0")
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
	if cfg.ReadMaxConcurrent != 0 || cfg.ReadMaxPerTenant != 0 || cfg.ReadObjectMaxConcurrent != 0 || cfg.ParquetDecodeMaxConcurrent != 0 {
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
		"GRAPHDB_READINESS_TIMEOUT",
		"GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION",
		"GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL",
		"GRAPHDB_COORDINATOR_OUTBOX_RETENTION",
		"GRAPHDB_COORDINATOR_CLEANUP_INTERVAL",
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

func TestLoadAllowsWriteRequestPipeliningPerTenant(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITE_MAX_PER_TENANT", "4")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WriteMaxPerTenant != 4 {
		t.Fatalf("WriteMaxPerTenant = %d, want 4", cfg.WriteMaxPerTenant)
	}
}

func TestLoadPostgresCoordinationRequirements(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_COORDINATION", "postgres")
	t.Setenv("GRAPHDB_INGEST_MODE", "direct")
	t.Setenv("GRAPHDB_POSTGRES_DSN", "postgres://graphdb:test@postgres/graphdb")
	t.Setenv("GRAPHDB_COORDINATOR_NAMESPACE", "production")
	t.Setenv("GRAPHDB_STORAGE", "s3")
	t.Setenv("S3_PROVIDER", storage.ObjectProviderGenericS3)
	t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologyCAS)
	t.Setenv("GRAPHDB_WRITE_CAS_MAX_RETRIES", "12")
	t.Setenv("GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION", "48h")
	t.Setenv("GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL", "4m")
	t.Setenv("GRAPHDB_COORDINATOR_OUTBOX_RETENTION", "2h")
	t.Setenv("GRAPHDB_COORDINATOR_CLEANUP_INTERVAL", "30s")
	t.Setenv("GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE", "7000")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Coordination != storage.CoordinationPostgres ||
		cfg.PostgresSchema != "graphdb_coordination" ||
		cfg.CoordinatorNamespace != "production" ||
		cfg.WriteCASMaxRetries != 12 ||
		cfg.CoordinatorIdempotencyRetention != 48*time.Hour ||
		cfg.CoordinatorPendingReservationTTL != 4*time.Minute ||
		cfg.CoordinatorOutboxRetention != 2*time.Hour ||
		cfg.CoordinatorCleanupInterval != 30*time.Second ||
		cfg.CoordinatorCleanupBatchSize != 7000 {
		t.Fatalf("coordination config = %#v", cfg)
	}
}

func TestLoadRejectsUnsafePostgresReservationTTL(t *testing.T) {
	for _, test := range []struct {
		name         string
		writeTimeout string
		pendingTTL   string
	}{
		{name: "unbounded write", writeTimeout: "0", pendingTTL: "3m"},
		{name: "equal ttl", writeTimeout: "3m", pendingTTL: "3m"},
		{name: "short ttl", writeTimeout: "4m", pendingTTL: "3m"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setPostgresConfigEnv(t)
			t.Setenv("GRAPHDB_WRITE_EXECUTION_TIMEOUT", test.writeTimeout)
			t.Setenv("GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL", test.pendingTTL)
			if _, err := Load(); err == nil ||
				!strings.Contains(err.Error(), "GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL") &&
					!strings.Contains(err.Error(), "GRAPHDB_WRITE_EXECUTION_TIMEOUT") {
				t.Fatalf("Load err = %v, want reservation/write timeout validation", err)
			}
		})
	}
}

func TestLoadRejectsUnsafePostgresCoordination(t *testing.T) {
	tests := []struct {
		name string
		set  func(*testing.T)
		want string
	}{
		{
			name: "missing dsn",
			set:  func(t *testing.T) { t.Setenv("GRAPHDB_POSTGRES_DSN", "") },
			want: "GRAPHDB_POSTGRES_DSN",
		},
		{
			name: "missing namespace",
			set:  func(t *testing.T) { t.Setenv("GRAPHDB_COORDINATOR_NAMESPACE", "") },
			want: "GRAPHDB_COORDINATOR_NAMESPACE",
		},
		{
			name: "local object store",
			set:  func(t *testing.T) { t.Setenv("GRAPHDB_STORAGE", "local") },
			want: "GRAPHDB_STORAGE=s3",
		},
		{
			name: "native provider",
			set: func(t *testing.T) {
				t.Setenv("S3_PROVIDER", storage.ObjectProviderAliyunOSS)
				t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologySingle)
				t.Setenv("S3_VERSIONING", storage.BucketVersioningDisabled)
			},
			want: "S3_PROVIDER=generic-s3",
		},
		{
			name: "single topology",
			set:  func(t *testing.T) { t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologySingle) },
			want: "GRAPHDB_WRITER_TOPOLOGY=cas",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setLocalConfigEnv(t)
			t.Setenv("GRAPHDB_COORDINATION", "postgres")
			t.Setenv("GRAPHDB_POSTGRES_DSN", "postgres://graphdb:test@postgres/graphdb")
			t.Setenv("GRAPHDB_COORDINATOR_NAMESPACE", "production")
			t.Setenv("GRAPHDB_STORAGE", "s3")
			t.Setenv("S3_PROVIDER", storage.ObjectProviderGenericS3)
			t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologyCAS)
			test.set(t)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load err=%v, want %q", err, test.want)
			}
		})
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

func TestLoadParsesWriteCacheMemoryLimit(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITE_CACHE_MAX_BYTES", "96MiB")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WriteCacheMaxBytes != 96*1024*1024 {
		t.Fatalf("write cache max bytes = %d, want %d", cfg.WriteCacheMaxBytes, 96*1024*1024)
	}
}

func TestLoadRejectsInvalidWriteCacheMemoryLimit(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_WRITE_CACHE_MAX_BYTES", "96XB")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GRAPHDB_WRITE_CACHE_MAX_BYTES must be a byte size") {
		t.Fatalf("Load err = %v, want write cache byte validation", err)
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
	t.Setenv("GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT", "7")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_ENTRIES", "123")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_MAX_BYTES", "48MiB")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_DIR", "/tmp/graphdb-index-cache")
	t.Setenv("GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES", "12MiB")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ReaderIndexCacheEntries != 123 || cfg.ReaderIndexCacheMaxBytes != 48*1024*1024 || cfg.ReaderIndexCacheDir != "/tmp/graphdb-index-cache" {
		t.Fatalf("reader index cache config = %#v", cfg)
	}
	if cfg.ReadMaxConcurrent != 17 || cfg.ReadMaxPerTenant != 5 || cfg.ReadObjectMaxConcurrent != 23 || cfg.ReadObjectSingleflight || cfg.ParquetDecodeMaxConcurrent != 7 {
		t.Fatalf("reader read-path config = %#v", cfg)
	}
	if cfg.EntityPagePackMaxBytes != 12*1024*1024 {
		t.Fatalf("entity page pack max bytes = %d, want %d", cfg.EntityPagePackMaxBytes, 12*1024*1024)
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
	if !cfg.IngestCollectorStatusMaterialized {
		t.Fatal("ingest collector status materialized default enabled = false, want true")
	}

	t.Setenv("GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED", "false")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load enabled: %v", err)
	}
	if cfg.IngestCollectorStatusMaterialized {
		t.Fatal("ingest collector status materialized disabled = true, want false")
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

func TestLoadParsesS3PathStyle(t *testing.T) {
	setLocalConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.S3PathStyle {
		t.Fatal("S3PathStyle default = true, want false")
	}

	t.Setenv("S3_PATH_STYLE", "true")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load with S3_PATH_STYLE: %v", err)
	}
	if !cfg.S3PathStyle {
		t.Fatal("S3PathStyle = false, want true")
	}
}

func TestLoadRejectsInvalidS3PathStyle(t *testing.T) {
	setLocalConfigEnv(t)
	t.Setenv("S3_PATH_STYLE", "sometimes")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "S3_PATH_STYLE must be a boolean") {
		t.Fatalf("Load err = %v, want S3_PATH_STYLE validation", err)
	}
}

func TestLoadValidatesNativeObjectStorageProfile(t *testing.T) {
	setup := func(t *testing.T) {
		t.Helper()
		setLocalConfigEnv(t)
		t.Setenv("GRAPHDB_STORAGE", "s3")
		t.Setenv("S3_PROVIDER", storage.ObjectProviderAliyunOSS)
		t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologySingle)
		t.Setenv("S3_VERSIONING", storage.BucketVersioningDisabled)
	}
	cases := []struct {
		name string
		set  func(t *testing.T)
		want string
	}{
		{
			name: "requires single writer",
			set: func(t *testing.T) {
				t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologyCAS)
			},
			want: "GRAPHDB_WRITER_TOPOLOGY=single",
		},
		{
			name: "requires disabled versioning",
			set: func(t *testing.T) {
				t.Setenv("S3_VERSIONING", "enabled")
			},
			want: "S3_VERSIONING=disabled",
		},
		{
			name: "rejects unknown provider",
			set: func(t *testing.T) {
				t.Setenv("S3_PROVIDER", "unknown")
			},
			want: "unsupported S3_PROVIDER",
		},
		{
			name: "rejects cos path style",
			set: func(t *testing.T) {
				t.Setenv("S3_PROVIDER", storage.ObjectProviderTencentCOS)
				t.Setenv("S3_PATH_STYLE", "true")
			},
			want: "does not support S3_PATH_STYLE=true",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			setup(t)
			tt.set(t)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNewObjectStoreSelectsNativeSingleWriterProfiles(t *testing.T) {
	cases := []struct {
		provider string
		endpoint string
		region   string
	}{
		{provider: storage.ObjectProviderAliyunOSS, endpoint: "https://oss-cn-hangzhou.aliyuncs.com", region: "cn-hangzhou"},
		{provider: storage.ObjectProviderHuaweiOBS, endpoint: "https://obs.cn-north-4.myhuaweicloud.com", region: "cn-north-4"},
		{provider: storage.ObjectProviderTencentCOS, endpoint: "https://graphdb-1250000000.cos.ap-guangzhou.myqcloud.com", region: "ap-guangzhou"},
	}
	for _, tt := range cases {
		t.Run(tt.provider, func(t *testing.T) {
			objects, err := NewObjectStore(Config{
				StoreKind:         "s3",
				S3Provider:        tt.provider,
				S3Versioning:      storage.BucketVersioningDisabled,
				WriterTopology:    storage.WriterTopologySingle,
				S3Endpoint:        tt.endpoint,
				S3Bucket:          "graphdb-1250000000",
				S3Region:          tt.region,
				S3AccessKeyID:     "access-key",
				S3SecretAccessKey: "secret-key",
			})
			if err != nil {
				t.Fatalf("NewObjectStore: %v", err)
			}
			if _, ok := objects.(*storage.SingleWriterObjectStore); !ok {
				t.Fatalf("store type = %T, want *storage.SingleWriterObjectStore", objects)
			}
		})
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
	t.Setenv("GRAPHDB_ADDR", "")
	t.Setenv("GRAPHDB_ADMIN_ADDR", "")
	t.Setenv("GRAPHDB_PPROF_ENABLED", "")
	t.Setenv("GRAPHDB_PREFIX", "")
	t.Setenv("GRAPHDB_QUERY_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_QUERY_MAX_PER_TENANT", "")
	t.Setenv("GRAPHDB_QUERY_QUEUE_TIMEOUT", "")
	t.Setenv("GRAPHDB_READ_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_READ_MAX_PER_TENANT", "")
	t.Setenv("GRAPHDB_READ_QUEUE_TIMEOUT", "")
	t.Setenv("GRAPHDB_READ_OBJECT_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_READ_OBJECT_SINGLEFLIGHT", "")
	t.Setenv("GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_CONCURRENT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_QUEUE_TIMEOUT", "")
	t.Setenv("GRAPHDB_WRITE_EXECUTION_TIMEOUT", "")
	t.Setenv("GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD", "")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_WINDOW", "")
	t.Setenv("GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD", "")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_WINDOW", "")
	t.Setenv("GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD", "")
	t.Setenv("GRAPHDB_WRITE_CAS_MAX_RETRIES", "")
	t.Setenv("GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION", "")
	t.Setenv("GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL", "")
	t.Setenv("GRAPHDB_COORDINATOR_OUTBOX_RETENTION", "")
	t.Setenv("GRAPHDB_COORDINATOR_CLEANUP_INTERVAL", "")
	t.Setenv("GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE", "")
	t.Setenv("GRAPHDB_WRITE_MAX_COMMIT_TAIL", "")
	t.Setenv("GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_BYTES_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_MAX_EDGES_PER_TENANT", "")
	t.Setenv("GRAPHDB_WRITE_CACHE_MAX_BYTES", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_BYTES", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_MAX_KEYS", "")
	t.Setenv("GRAPHDB_WRITER_OBJECT_CACHE_NEGATIVE_TTL", "")
	t.Setenv("GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED", "")
	t.Setenv("GRAPHDB_INGEST_MODE", "")
	t.Setenv("GRAPHDB_INGEST_WAL_DIR", "")
	t.Setenv("GRAPHDB_INGEST_WAL_DURABILITY", "")
	t.Setenv("GRAPHDB_INGEST_WAL_BUFFER_BYTES", "")
	t.Setenv("GRAPHDB_INGEST_WAL_FSYNC_INTERVAL", "")
	t.Setenv("GRAPHDB_INGEST_WAL_MAX_BYTES", "")
	t.Setenv("GRAPHDB_INGEST_WAL_SEGMENT_BYTES", "")
	t.Setenv("GRAPHDB_INGEST_WAL_APPEND_QUEUE", "")
	t.Setenv("GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES", "")
	t.Setenv("GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK", "")
	t.Setenv("GRAPHDB_INGEST_WAL_HIGH_WATERMARK", "")
	t.Setenv("GRAPHDB_INGEST_WAL_STOP_WATERMARK", "")
	t.Setenv("GRAPHDB_INGEST_MAX_PENDING_AGE", "")
	t.Setenv("GRAPHDB_INGEST_FLUSH_INTERVAL", "")
	t.Setenv("GRAPHDB_INGEST_FLUSH_MAX_REQUESTS", "")
	t.Setenv("GRAPHDB_INGEST_FLUSH_MAX_BYTES", "")
	t.Setenv("GRAPHDB_INGEST_FLUSH_WORKERS", "")
	t.Setenv("GRAPHDB_INGEST_METADATA_MODE", "")
	t.Setenv("GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL", "")
	t.Setenv("GRAPHDB_INGEST_METADATA_MAX_REQUESTS", "")
	t.Setenv("GRAPHDB_INGEST_METADATA_MAX_BYTES", "")
	t.Setenv("GRAPHDB_INGEST_METADATA_FLUSH_WORKERS", "")
	t.Setenv("GRAPHDB_INGEST_SHUTDOWN_TIMEOUT", "")
	t.Setenv("GRAPHDB_POLL_INTERVAL", "")
	t.Setenv("GRAPHDB_SLOW_QUERY_THRESHOLD", "")
	t.Setenv("GRAPHDB_INDEX_HEALTH_INTERVAL", "")
	t.Setenv("GRAPHDB_MAINTENANCE_INTERVAL", "")
	t.Setenv("GRAPHDB_TENANT_USAGE_CACHE_TTL", "")
	t.Setenv("GRAPHDB_READER_CATCHUP_TIMEOUT", "")
	t.Setenv("GRAPHDB_READINESS_TIMEOUT", "")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_ENTRIES", "")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_MAX_BYTES", "")
	t.Setenv("GRAPHDB_READER_INDEX_CACHE_DIR", "")
	t.Setenv("GRAPHDB_INDEX_ENTITY_RECORDS", "")
	t.Setenv("GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES", "")
	t.Setenv("GRAPHDB_FAULT_OBJECT_READ_DELAY", "")
	t.Setenv("GRAPHDB_OTLP_ENDPOINT", "")
	t.Setenv("GRAPHDB_OTLP_INSECURE", "")
	t.Setenv("GRAPHDB_SERVICE_NAME", "")
	t.Setenv("GRAPHDB_INSTANCE_ID", "")
	t.Setenv("GRAPHDB_COORDINATION", "")
	t.Setenv("GRAPHDB_POSTGRES_DSN", "")
	t.Setenv("GRAPHDB_POSTGRES_SCHEMA", "")
	t.Setenv("GRAPHDB_COORDINATOR_NAMESPACE", "")
	t.Setenv("S3_PATH_STYLE", "")
	t.Setenv("S3_PROVIDER", "")
	t.Setenv("S3_VERSIONING", "")
	t.Setenv("GRAPHDB_WRITER_TOPOLOGY", "")
}

func setPostgresConfigEnv(t *testing.T) {
	t.Helper()
	setLocalConfigEnv(t)
	t.Setenv("GRAPHDB_INGEST_MODE", "direct")
	t.Setenv("GRAPHDB_COORDINATION", "postgres")
	t.Setenv("GRAPHDB_POSTGRES_DSN", "postgres://graphdb:test@postgres/graphdb")
	t.Setenv("GRAPHDB_COORDINATOR_NAMESPACE", "production")
	t.Setenv("GRAPHDB_STORAGE", "s3")
	t.Setenv("S3_PROVIDER", storage.ObjectProviderGenericS3)
	t.Setenv("GRAPHDB_WRITER_TOPOLOGY", storage.WriterTopologyCAS)
}
