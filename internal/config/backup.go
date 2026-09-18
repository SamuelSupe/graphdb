package config

import (
	"os"

	"gitlab.jiagouyun.com/guance/graphdb/internal/backupstore"
)

func loadBackupConfig() (backupstore.Config, error) {
	cfg := backupstore.Config{
		Bucket:          os.Getenv("GRAPHDB_BACKUP_S3_BUCKET"),
		Prefix:          os.Getenv("GRAPHDB_BACKUP_S3_PREFIX"),
		Endpoint:        os.Getenv("GRAPHDB_BACKUP_S3_ENDPOINT"),
		Region:          os.Getenv("GRAPHDB_BACKUP_S3_REGION"),
		AccessKeyID:     os.Getenv("GRAPHDB_BACKUP_S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("GRAPHDB_BACKUP_S3_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("GRAPHDB_BACKUP_S3_SESSION_TOKEN"),
	}
	if err := loadBoolEnv("GRAPHDB_BACKUP_S3_PATH_STYLE", &cfg.PathStyle); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}
