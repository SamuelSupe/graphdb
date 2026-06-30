package storage

import "context"

func putManifestFixture(ctx context.Context, store *TenantStore, tenantID string, manifest Manifest) error {
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, store.manifestKey(tenantID), data)
}
