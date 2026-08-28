package bootstrap

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func newCoordinator(ctx context.Context, cfg config.Config) (storage.WriteCoordinator, error) {
	if cfg.CoordinationMode() == storage.CoordinationLocal {
		return nil, nil
	}
	return storage.NewPostgresCoordinator(
		ctx,
		cfg.PostgresDSN,
		cfg.PostgresSchema,
		cfg.CoordinatorNamespace,
	)
}

func newObjectStore(cfg config.Config) (storage.ObjectStore, error) {
	if err := cfg.ValidateObjectStore(); err != nil {
		return nil, err
	}
	switch cfg.StoreKind {
	case "local":
		return storage.NewFileStore(cfg.DataDir), nil
	case "s3":
		options := storage.S3Options{PathStyle: cfg.S3PathStyle}
		switch cfg.ObjectProvider() {
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
