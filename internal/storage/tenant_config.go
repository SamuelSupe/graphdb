package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type TenantConfig struct {
	Backpressure TenantBackpressureConfig `json:"backpressure,omitempty"`
	Quota        TenantQuotaConfig        `json:"quota,omitempty"`
	Maintenance  TenantMaintenanceConfig  `json:"maintenance,omitempty"`
	Indexes      TenantIndexConfig        `json:"indexes,omitempty"`
}

type TenantBackpressureConfig struct {
	ObjectLatencyThresholdMS *int64 `json:"object_latency_threshold_ms,omitempty"`
	CASConflictWindowMS      *int64 `json:"cas_conflict_window_ms,omitempty"`
	CASConflictThreshold     *int   `json:"cas_conflict_threshold,omitempty"`
	MaxCommitTail            *int   `json:"max_commit_tail,omitempty"`
	RetryAfterMS             *int64 `json:"retry_after_ms,omitempty"`
}

type TenantQuotaConfig struct {
	MaxEntitiesPerTenant *int `json:"max_entities_per_tenant,omitempty"`
	MaxEdgesPerTenant    *int `json:"max_edges_per_tenant,omitempty"`
}

type TenantMaintenanceConfig struct {
	AutoCompact                 *bool  `json:"auto_compact,omitempty"`
	CompactCommitTailThreshold  *int   `json:"compact_commit_tail_threshold,omitempty"`
	CompactObjectCountThreshold *int   `json:"compact_object_count_threshold,omitempty"`
	CompactBytesThreshold       *int64 `json:"compact_bytes_threshold,omitempty"`
	SmallFileObjectThreshold    *int   `json:"small_file_object_threshold,omitempty"`
	SmallFileBytesThreshold     *int64 `json:"small_file_bytes_threshold,omitempty"`
	EntityPageSplitThreshold    *int   `json:"entity_page_split_threshold,omitempty"`
	EntityPageMergeThreshold    *int   `json:"entity_page_merge_threshold,omitempty"`
	EdgeShardSplitThreshold     *int   `json:"edge_shard_split_threshold,omitempty"`
	EdgeShardMergeThreshold     *int   `json:"edge_shard_merge_threshold,omitempty"`
	GCIntervalSeconds           *int64 `json:"gc_interval_seconds,omitempty"`
	KeepSnapshots               *int   `json:"keep_snapshots,omitempty"`
	DeadLetterMaxAgeSeconds     *int64 `json:"deadletter_max_age_seconds,omitempty"`
	TaskMaxAgeSeconds           *int64 `json:"task_max_age_seconds,omitempty"`
	CleanupIndexOrphans         *bool  `json:"cleanup_index_orphans,omitempty"`
}

type TenantIndexConfig struct {
	AutoRebuild    *bool `json:"auto_rebuild,omitempty"`
	RebuildOnStale *bool `json:"rebuild_on_stale,omitempty"`
}

func DefaultTenantConfig() TenantConfig {
	autoCompact := true
	compactTail := 1000
	smallObjects := 1000
	smallBytes := int64(64 * 1024)
	gcInterval := int64(30 * 60)
	keepSnapshots := 2
	cleanupOrphans := true
	autoRebuild := true
	rebuildOnStale := true
	return TenantConfig{
		Maintenance: TenantMaintenanceConfig{
			AutoCompact:                &autoCompact,
			CompactCommitTailThreshold: &compactTail,
			SmallFileObjectThreshold:   &smallObjects,
			SmallFileBytesThreshold:    &smallBytes,
			GCIntervalSeconds:          &gcInterval,
			KeepSnapshots:              &keepSnapshots,
			CleanupIndexOrphans:        &cleanupOrphans,
		},
		Indexes: TenantIndexConfig{
			AutoRebuild:    &autoRebuild,
			RebuildOnStale: &rebuildOnStale,
		},
	}
}

func TenantConfigWithDefaults(config TenantConfig) TenantConfig {
	defaults := DefaultTenantConfig()
	config.Maintenance = maintenanceConfigWithDefaults(config.Maintenance, defaults.Maintenance)
	config.Indexes = indexConfigWithDefaults(config.Indexes, defaults.Indexes)
	return config
}

func maintenanceConfigWithDefaults(config TenantMaintenanceConfig, defaults TenantMaintenanceConfig) TenantMaintenanceConfig {
	if config.AutoCompact == nil {
		config.AutoCompact = defaults.AutoCompact
	}
	if config.CompactCommitTailThreshold == nil {
		config.CompactCommitTailThreshold = defaults.CompactCommitTailThreshold
	}
	if config.SmallFileObjectThreshold == nil {
		config.SmallFileObjectThreshold = defaults.SmallFileObjectThreshold
	}
	if config.SmallFileBytesThreshold == nil {
		config.SmallFileBytesThreshold = defaults.SmallFileBytesThreshold
	}
	if config.GCIntervalSeconds == nil {
		config.GCIntervalSeconds = defaults.GCIntervalSeconds
	}
	if config.KeepSnapshots == nil {
		config.KeepSnapshots = defaults.KeepSnapshots
	}
	if config.CleanupIndexOrphans == nil {
		config.CleanupIndexOrphans = defaults.CleanupIndexOrphans
	}
	return config
}

func indexConfigWithDefaults(config TenantIndexConfig, defaults TenantIndexConfig) TenantIndexConfig {
	if config.AutoRebuild == nil {
		config.AutoRebuild = defaults.AutoRebuild
	}
	if config.RebuildOnStale == nil {
		config.RebuildOnStale = defaults.RebuildOnStale
	}
	return config
}

type tenantConfigRecord struct {
	TenantID string       `json:"tenant_id,omitempty"`
	Config   TenantConfig `json:"config"`
}

func (s *TenantStore) GetTenantConfig(ctx context.Context, tenantID string) (TenantConfig, bool, error) {
	config, configured, _, err := s.getTenantConfigWithMeta(ctx, tenantID)
	return config, configured, err
}

func (s *TenantStore) PutTenantConfig(ctx context.Context, tenantID string, config TenantConfig) (TenantConfig, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantConfig{}, err
	}
	if err := validateTenantConfig(config); err != nil {
		return TenantConfig{}, err
	}
	if s.coordinated() {
		return s.putCoordinatedTenantConfig(ctx, tenantID, config)
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return TenantConfig{}, err
	}
	_, _, meta, err := s.getTenantConfigForWrite(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}
	record := tenantConfigRecord{TenantID: tenantID, Config: config}
	nextMeta, err := s.putTenantConfigRecordWithMeta(ctx, tenantID, record, meta)
	if err != nil {
		s.deleteCachedTenantConfig(tenantID)
		if errors.Is(err, ErrConflict) {
			return TenantConfig{}, fmt.Errorf("%w: tenant config for tenant %q changed while publishing", ErrConflict, tenantID)
		}
		return TenantConfig{}, err
	}
	s.setCachedTenantConfig(tenantID, config, true, nextMeta)
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantConfig{}, err
	}
	return config, nil
}

func (s *TenantStore) getTenantConfigWithMeta(ctx context.Context, tenantID string) (TenantConfig, bool, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantConfig{}, false, ObjectMeta{}, err
	}
	if s.coordinated() {
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return TenantConfig{}, false, ObjectMeta{}, err
		}
		return snapshot.TenantConfig, snapshot.TenantConfigConfigured,
			coordinatedManifestMeta(s.tenantConfigKey(tenantID), head), nil
	}
	key := s.tenantConfigKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return TenantConfig{}, false, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return TenantConfig{}, false, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return TenantConfig{}, false, ObjectMeta{}, fmt.Errorf("unsupported tenant config: only parquet configs are readable")
	}
	record, err := decodeParquetTenantConfig(ctx, data)
	if err != nil {
		return TenantConfig{}, false, ObjectMeta{}, err
	}
	if record.TenantID != "" && record.TenantID != tenantID {
		return TenantConfig{}, false, ObjectMeta{}, fmt.Errorf("tenant config mismatch: path tenant %q contains tenant %q", tenantID, record.TenantID)
	}
	if err := validateTenantConfig(record.Config); err != nil {
		return TenantConfig{}, false, ObjectMeta{}, err
	}
	return record.Config, true, meta, nil
}

func (s *TenantStore) putTenantConfigRecordWithMeta(ctx context.Context, tenantID string, record tenantConfigRecord, meta ObjectMeta) (ObjectMeta, error) {
	if s.coordinated() {
		if _, err := s.PutTenantConfig(ctx, tenantID, record.Config); err != nil {
			return ObjectMeta{}, err
		}
		_, _, next, err := s.getTenantConfigWithMeta(ctx, tenantID)
		return next, err
	}
	record.TenantID = tenantID
	data, err := marshalParquetTenantConfig(ctx, record)
	if err != nil {
		return ObjectMeta{}, err
	}
	return s.putTenantBytesWithMetaResult(ctx, tenantID, s.tenantConfigKey(tenantID), data, meta)
}

func (s *TenantStore) effectiveBackpressureConfig(ctx context.Context, tenantID string) (BackpressureConfig, error) {
	base := BackpressureConfig{}
	if s.Backpressure != nil {
		base = s.Backpressure.Config()
	}
	config, ok, _, err := s.getTenantConfigForWrite(ctx, tenantID)
	if err != nil {
		return BackpressureConfig{}, err
	}
	if !ok {
		return base, nil
	}
	applyTenantBackpressureConfig(&base, config.Backpressure)
	applyTenantQuotaConfig(&base, config.Quota)
	return normalizeBackpressureConfig(base), nil
}

func (s *TenantStore) getTenantConfigForWrite(ctx context.Context, tenantID string) (TenantConfig, bool, ObjectMeta, error) {
	if s.coordinated() {
		return s.getTenantConfigWithMeta(ctx, tenantID)
	}
	if config, configured, meta, ok := s.getCachedTenantConfig(tenantID); ok {
		return config, configured, meta, nil
	}
	config, configured, meta, err := s.getTenantConfigWithMeta(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, false, ObjectMeta{}, err
	}
	s.setCachedTenantConfig(tenantID, config, configured, meta)
	return config, configured, meta, nil
}

func (s *TenantStore) putCoordinatedTenantConfig(
	ctx context.Context,
	tenantID string,
	config TenantConfig,
) (TenantConfig, error) {
	if _, err := s.ensureCoordinatedTenantHead(ctx, tenantID); err != nil {
		return TenantConfig{}, err
	}
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return TenantConfig{}, err
		}
		snapshot.TenantConfig = config
		snapshot.TenantConfigConfigured = true
		_, published, err := s.publishCoordinatedWriteContext(ctx, head, snapshot)
		if err != nil {
			return TenantConfig{}, err
		}
		if published {
			s.deleteCachedTenantConfig(tenantID)
			if err := s.mirrorLatestWriteContext(ctx, tenantID); err != nil {
				return TenantConfig{}, err
			}
			if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
				return TenantConfig{}, err
			}
			return config, nil
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return TenantConfig{}, err
		}
	}
	return TenantConfig{}, fmt.Errorf("%w: tenant config for tenant %q changed while publishing", ErrWriteConflict, tenantID)
}

func applyTenantBackpressureConfig(base *BackpressureConfig, config TenantBackpressureConfig) {
	if config.ObjectLatencyThresholdMS != nil {
		base.ObjectLatencyThreshold = time.Duration(*config.ObjectLatencyThresholdMS) * time.Millisecond
	}
	if config.CASConflictWindowMS != nil {
		base.CASConflictWindow = time.Duration(*config.CASConflictWindowMS) * time.Millisecond
	}
	if config.CASConflictThreshold != nil {
		base.CASConflictThreshold = *config.CASConflictThreshold
	}
	if config.MaxCommitTail != nil {
		base.MaxCommitTail = *config.MaxCommitTail
	}
	if config.RetryAfterMS != nil {
		base.RetryAfter = time.Duration(*config.RetryAfterMS) * time.Millisecond
	}
}

func applyTenantQuotaConfig(base *BackpressureConfig, config TenantQuotaConfig) {
	if config.MaxEntitiesPerTenant != nil {
		base.MaxEntitiesPerTenant = *config.MaxEntitiesPerTenant
	}
	if config.MaxEdgesPerTenant != nil {
		base.MaxEdgesPerTenant = *config.MaxEdgesPerTenant
	}
}

func validateTenantConfig(config TenantConfig) error {
	if err := nonNegativeInt64("backpressure.object_latency_threshold_ms", config.Backpressure.ObjectLatencyThresholdMS); err != nil {
		return err
	}
	if err := nonNegativeInt64("backpressure.cas_conflict_window_ms", config.Backpressure.CASConflictWindowMS); err != nil {
		return err
	}
	if err := nonNegativeInt("backpressure.cas_conflict_threshold", config.Backpressure.CASConflictThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("backpressure.max_commit_tail", config.Backpressure.MaxCommitTail); err != nil {
		return err
	}
	if err := nonNegativeInt64("backpressure.retry_after_ms", config.Backpressure.RetryAfterMS); err != nil {
		return err
	}
	if err := nonNegativeInt("quota.max_entities_per_tenant", config.Quota.MaxEntitiesPerTenant); err != nil {
		return err
	}
	if err := nonNegativeInt("quota.max_edges_per_tenant", config.Quota.MaxEdgesPerTenant); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.compact_commit_tail_threshold", config.Maintenance.CompactCommitTailThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.compact_object_count_threshold", config.Maintenance.CompactObjectCountThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt64("maintenance.compact_bytes_threshold", config.Maintenance.CompactBytesThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.small_file_object_threshold", config.Maintenance.SmallFileObjectThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt64("maintenance.small_file_bytes_threshold", config.Maintenance.SmallFileBytesThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.entity_page_split_threshold", config.Maintenance.EntityPageSplitThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.entity_page_merge_threshold", config.Maintenance.EntityPageMergeThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.edge_shard_split_threshold", config.Maintenance.EdgeShardSplitThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.edge_shard_merge_threshold", config.Maintenance.EdgeShardMergeThreshold); err != nil {
		return err
	}
	if err := nonNegativeInt64("maintenance.gc_interval_seconds", config.Maintenance.GCIntervalSeconds); err != nil {
		return err
	}
	if err := nonNegativeInt("maintenance.keep_snapshots", config.Maintenance.KeepSnapshots); err != nil {
		return err
	}
	if err := nonNegativeInt64("maintenance.deadletter_max_age_seconds", config.Maintenance.DeadLetterMaxAgeSeconds); err != nil {
		return err
	}
	return nonNegativeInt64("maintenance.task_max_age_seconds", config.Maintenance.TaskMaxAgeSeconds)
}

func nonNegativeInt(name string, value *int) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be >= 0", name)
	}
	return nil
}

func nonNegativeInt64(name string, value *int64) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be >= 0", name)
	}
	return nil
}
