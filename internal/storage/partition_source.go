package storage

import (
	"context"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// RetirePartitionSource is only called inside the HA replication journal after
// the partitioned generation is active. Metadata, backup records and terminal
// ingestion receipts remain; graph data and migrated commit bodies are removed
// in bounded pages so conversion releases its original single-group capacity.
func (s *TenantStore) RetirePartitionSource(ctx context.Context, tenant string, includeLegacyIndexes bool) (bool, error) {
	if err := ValidateTenantID(tenant); err != nil {
		return false, err
	}
	manifest, meta, err := s.getManifest(ctx, tenant)
	if err != nil {
		return false, err
	}
	if manifest.Version != 0 || manifest.SnapshotKey != "" || manifest.SnapshotCatalogKey != "" || len(manifest.CommitKeys) > 0 || len(manifest.CommitSegments) > 0 {
		digest, err := graph.New().ContentHash()
		if err != nil {
			return false, err
		}
		empty := Manifest{TenantID: tenant, WriterFence: manifest.WriterFence, WriterFenceEpoch: manifest.WriterFenceEpoch, DataHash: digest, UpdatedAt: manifest.UpdatedAt}
		if _, err := s.putManifestMetaUnchecked(ctx, tenant, empty, meta); err != nil {
			return false, err
		}
	}
	defer s.invalidateTenantState(tenant)
	tenantPrefix := s.tenantObjectPrefix(tenant)
	prefixes := []string{s.commitPrefix(tenant), s.snapshotPrefix(tenant), tenantPrefix + "idempotency/commits/",
		tenantPrefix + "indexes/parquet/"}
	if includeLegacyIndexes {
		prefixes = append(prefixes, s.secondaryIndexPrefix(tenant), s.edgeShardPrefix(tenant),
			s.reverseIndexPrefix(tenant), s.entityPagePrefix(tenant), s.entityRecordPrefix(tenant), tenantPrefix+"indexs/")
	}
	for _, prefix := range prefixes {
		objects, next, err := listObjectPage(ctx, s.Objects, prefix, "", 128)
		if err != nil {
			return false, err
		}
		for _, object := range objects {
			if err := s.Objects.Delete(ctx, object.Key); err != nil {
				return false, err
			}
		}
		if next != "" || len(objects) > 0 {
			return false, nil
		}
	}
	// The catalog is a stale reference to the retired records, rather than data
	// configuration; definitions and completed task metadata remain available.
	for _, key := range []string{s.indexCatalogKey(tenant)} {
		if strings.HasPrefix(key, s.tenantObjectPrefix(tenant)) {
			if err := s.Objects.Delete(ctx, key); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}
