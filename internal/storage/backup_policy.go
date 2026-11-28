package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/backup"

type TenantBackupConfig = backup.TenantBackupConfig

func backupPolicy(c TenantBackupConfig) TenantBackupConfig {
	return backup.DefaultPolicy(c)
}

func validateBackupPolicy(c TenantBackupConfig) error {
	return backup.ValidatePolicy(c)
}
