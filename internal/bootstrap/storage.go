package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type StorageRuntime struct {
	Store *storage.TenantStore

	Files  *storage.FileStore
	Ingest *storage.IngestService
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

		MaxMaintenanceBytes:        cfg.MaintenanceMaxBytes,
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

func (r *StorageRuntime) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return r.Shutdown(ctx)
}

// Keep directory ownership until every worker has stopped using the data files.
// A timeout leaves the lock held; callers can retry Shutdown with a new context.
func (r *StorageRuntime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.Store != nil {
		r.Store.StopBackground()
	}
	var ingestErr error
	if r.Ingest != nil {
		ingestErr = r.Ingest.Close(ctx)
	}
	if r.Store != nil {
		if err := r.Store.ShutdownTasks(ctx); err != nil {
			return errors.Join(ingestErr, err)
		}
	}
	if r.Files != nil {
		return errors.Join(ingestErr, r.Files.Close())
	}
	return ingestErr
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
