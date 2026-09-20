package bootstrap

import (
	"context"
	"fmt"
	"os"

	"gitlab.jiagouyun.com/guance/graphdb/internal/backupstore"
	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type StorageRuntime struct {
	Store *storage.TenantStore

	Files *storage.FileStore
}

func NewStorageRuntime(ctx context.Context, cfg config.Config) (*StorageRuntime, error) {
	if cfg.Mode != "" && cfg.Mode != "all" {
		return nil, fmt.Errorf("only GRAPHDB_MODE=all is supported in the local disk edition")
	}
	if err := cfg.ValidateObjectStore(); err != nil {
		return nil, err
	}
	if err := cfg.ValidateCoordination(); err != nil {
		return nil, err
	}
	files, err := storage.OpenFileStore(cfg.DataDir)
	var objects storage.ObjectStore = files
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
		InstanceID: cfg.InstanceID,
		ReaderID:   readerID(cfg),

		MaxWriteCacheBytes:         cfg.WriteCacheMaxBytes,
		WriteEntityRecords:         cfg.IndexEntityRecords,
		UseEntityRecordsForRead:    cfg.IndexEntityRecords,
		EntityPagePackMaxBytes:     cfg.EntityPagePackMaxBytes,
		MaterializeCollectorStatus: cfg.IngestCollectorStatusMaterialized,
		Backpressure:               pressure,

		IndexObjectCache: storage.IndexObjectCacheConfig{
			MaxEntries: cfg.ReaderIndexCacheEntries,
			MaxBytes:   cfg.ReaderIndexCacheMaxBytes,
			DiskDir:    cfg.ReaderIndexCacheDir,
		},
	})
	if err := store.EnsureLocalWriterAllowed(ctx); err != nil {
		files.Close()
		return nil, err
	}
	store.Backups, err = backupstore.New(ctx, cfg.Backup)
	if err != nil {
		files.Close()
		return nil, err
	}
	return &StorageRuntime{Store: store, Files: files}, nil
}

func (r *StorageRuntime) Close() {
	if r != nil {
		if r.Files != nil {
			r.Files.Close()
		}
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
