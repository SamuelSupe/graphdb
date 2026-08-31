package bootstrap

import (
	"context"
	"fmt"
	"os"

	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type StorageRuntime struct {
	Store       *storage.TenantStore
	Coordinator storage.WriteCoordinator
}

func NewStorageRuntime(ctx context.Context, cfg config.Config) (*StorageRuntime, error) {
	objects, err := newObjectStore(cfg)
	if err != nil {
		return nil, err
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

	store := storage.NewTenantStoreWithOptions(objects, cfg.Prefix, storage.TenantStoreOptions{
		InstanceID:                 cfg.InstanceID,
		ReaderID:                   readerID(cfg),
		RequireCoordinationMarker:  cfg.CoordinationMode() == storage.CoordinationPostgres,
		MaxWriteCacheBytes:         cfg.WriteCacheMaxBytes,
		WriteEntityRecords:         cfg.IndexEntityRecords,
		UseEntityRecordsForRead:    cfg.IndexEntityRecords,
		EntityPagePackMaxBytes:     cfg.EntityPagePackMaxBytes,
		MaterializeCollectorStatus: cfg.IngestCollectorStatusMaterialized,
		Backpressure:               pressure,
		CoordinatorRetryLimit:      cfg.WriteCASMaxRetries,
		CoordinatorPendingTTL:      cfg.CoordinatorPendingReservationTTL,
		CoordinatorCleanup:         cfg.CoordinatorCleanupConfig(),
		IndexObjectCache: storage.IndexObjectCacheConfig{
			MaxEntries: cfg.ReaderIndexCacheEntries,
			MaxBytes:   cfg.ReaderIndexCacheMaxBytes,
			DiskDir:    cfg.ReaderIndexCacheDir,
		},
	})
	coordinator, err := newCoordinator(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &StorageRuntime{Store: store, Coordinator: coordinator}, nil
}

func (r *StorageRuntime) Close() {
	if r != nil && r.Coordinator != nil {
		r.Coordinator.Close()
	}
}

func readerID(cfg config.Config) string {
	if cfg.InstanceID != "" {
		return cfg.InstanceID
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s", hostname, cfg.Mode, cfg.Addr, cfg.Prefix)
}
